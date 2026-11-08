"""Servicio de metadatos (MS): plano de control replicado con Raft (pysyncobj).

Cada MS ejecuta:
  * un nodo Raft (puerto 7001) que replica el estado de metadatos;
  * un servidor gRPC (puerto 7000) con MetaService (clientes) y ClusterService (DN).
Solo el lider atiende operaciones; un seguidor responde NOT_LEADER con la pista del lider.
"""
import logging
import os
import posixpath
import random
import threading
import time
import uuid
from concurrent import futures

import grpc
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.syncobj import SyncObjException

from dfsha import common
from dfsha import dfsha_pb2 as pb
from dfsha import dfsha_pb2_grpc as rpc

log = logging.getLogger("meta")

NODE_ID = os.environ.get("DFSHA_NODE_ID", "ms-1")
RAFT_SELF = os.environ.get("DFSHA_RAFT_SELF", "127.0.0.1:7001")
RAFT_PEERS = [p for p in os.environ.get("DFSHA_RAFT_PEERS", "").split(",") if p]
GRPC_PORT = int(os.environ.get("DFSHA_GRPC_PORT", "7000"))
DATA_DIR = os.environ.get("DFSHA_DATA", "/data")
SUSPECT_AFTER = float(os.environ.get("DFSHA_SUSPECT_AFTER", "10"))
DEAD_AFTER = float(os.environ.get("DFSHA_DEAD_AFTER", "20"))
GC_GRACE = float(os.environ.get("DFSHA_GC_GRACE", "60"))
USERS = {}
for item in os.environ.get("DFSHA_USERS", "andres:andres123,juan:juan123").split(","):
    name, pwd = item.split(":", 1)
    USERS[name] = common.hash_password(pwd)


class MetaState(SyncObj):
    """Maquina de estados replicada. Todo metodo @replicated debe ser determinista:
    el tiempo se recibe como argumento, nunca se lee dentro."""

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        conf = SyncObjConf(
            password=os.environ.get("DFSHA_RAFT_PASSWORD", "dev-raft-password"),
            # el journal en disco de pysyncobj (mmap) falla con entradas grandes; se persiste con
            # instantaneas periodicas y la durabilidad la da la replicacion en mayoria (2 de 3)
            fullDumpFile=os.path.join(DATA_DIR, "raft.dump"),
            logCompactionMinTime=20,     # instantanea cada ~20 s si hubo cambios
            logCompactionMinEntries=200,
            dynamicMembershipChange=False,
            commandsWaitLeader=True,
            bindAddress=os.environ.get("DFSHA_RAFT_BIND", "0.0.0.0:7001"),
        )
        # el estado se crea antes de arrancar Raft para que la reproduccion del journal lo encuentre;
        # luego se quita de las propiedades internas de SyncObj para que entre en las instantaneas
        self.ns = {"/": {"type": "dir", "owner": "root", "mtime": 0.0}, "/users": {"type": "dir", "owner": "root", "mtime": 0.0}}
        self.replicas = {}      # block_id -> [node_id, ...]
        self.leases = {}        # path -> {holder, mode, token, expires}
        self.fencing = 0
        super().__init__(RAFT_SELF, RAFT_PEERS, conf)
        props = getattr(self, "_SyncObj__properies", None)
        if props is not None:
            props.difference_update({"ns", "replicas", "leases", "fencing"})

    # ---------------- espacio de nombres ----------------
    @replicated
    def mkdir(self, path, owner, now, parents=False):
        if path in self.ns:
            return {"error": "PATH_EXISTS"}
        parent = posixpath.dirname(path)
        if parent not in self.ns:
            if not parents:
                return {"error": "NO_SUCH_PATH", "message": parent}
            chain = []
            p = parent
            while p not in self.ns:
                chain.append(p)
                p = posixpath.dirname(p)
            for d in reversed(chain):
                self.ns[d] = {"type": "dir", "owner": owner, "mtime": now}
        elif self.ns[parent]["type"] != "dir":
            return {"error": "NOT_A_DIR"}
        self.ns[path] = {"type": "dir", "owner": owner, "mtime": now}
        return {"ok": True}

    @replicated
    def remove(self, path, recursive):
        e = self.ns.get(path)
        if e is None:
            return {"error": "NO_SUCH_PATH"}
        children = [p for p in self.ns if p.startswith(path.rstrip("/") + "/")]
        if e["type"] == "dir" and children and not recursive:
            return {"error": "DIR_NOT_EMPTY"}
        for p in children + [path]:
            self.ns.pop(p, None)
            self.leases.pop(p, None)
        return {"ok": True, "removed": len(children) + 1}

    # ---------------- leases con token de cerco ----------------
    @replicated
    def lock(self, path, holder, mode, ttl, now):
        cur = self.leases.get(path)
        if cur and cur["expires"] > now and cur["holder"] != holder:
            return {"error": "LOCKED", "holder": cur["holder"], "expires": cur["expires"]}
        self.fencing += 1
        lease = {"holder": holder, "mode": mode, "token": self.fencing, "expires": now + ttl}
        self.leases[path] = lease
        return {"ok": True, **lease}

    @replicated
    def unlock(self, path, token):
        cur = self.leases.get(path)
        if cur and cur["token"] == token:
            self.leases.pop(path)
            return {"ok": True}
        return {"error": "STALE_FENCING_TOKEN"}

    # ---------------- confirmacion atomica de version ----------------
    @replicated
    def commit(self, path, owner, base_version, fencing_token, size, file_sha, blocks, overwrite, now):
        parent = posixpath.dirname(path)
        if parent not in self.ns or self.ns[parent]["type"] != "dir":
            return {"error": "NO_SUCH_PATH", "message": parent}
        cur = self.ns.get(path)
        if cur is not None and cur["type"] == "dir":
            return {"error": "IS_A_DIR"}
        cur_version = cur["version"] if cur else 0
        if cur is not None and base_version < 0 and not overwrite:
            return {"error": "PATH_EXISTS"}
        if base_version >= 0 and base_version != cur_version:
            return {"error": "VERSION_CONFLICT", "current_version": cur_version}
        lease = self.leases.get(path)
        if lease and lease["expires"] > now:
            if fencing_token != lease["token"]:
                return {"error": "LOCKED" if fencing_token == 0 else "STALE_FENCING_TOKEN", "holder": lease["holder"]}
        # si el lease ya vencio, un token viejo no da prioridad: lo decide base_version
        for b in blocks:
            if b["block_id"] not in self.replicas and len(b["replicas"]) < common.MIN_REPLICAS:
                return {"error": "INSUFFICIENT_REPLICAS", "block": b["block_id"]}
        for b in blocks:
            if b["block_id"] not in self.replicas:
                self.replicas[b["block_id"]] = list(b["replicas"])
        self.ns[path] = {
            "type": "file", "owner": owner, "mtime": now, "version": cur_version + 1,
            "size": size, "sha256": file_sha,
            "blocks": [{"block_id": b["block_id"], "length": b["length"], "sha256": b["sha256"]} for b in blocks],
        }
        if lease and lease["token"] == fencing_token:
            self.leases.pop(path, None)
        return {"ok": True, "version": cur_version + 1}

    # ---------------- ubicacion de replicas ----------------
    @replicated
    def add_replica(self, block_id, node_id):
        reps = self.replicas.get(block_id)
        if reps is not None and node_id not in reps:
            reps.append(node_id)
        return True

    @replicated
    def drop_node(self, node_id):
        for reps in self.replicas.values():
            if node_id in reps:
                reps.remove(node_id)
        return True

    @replicated
    def forget_blocks(self, block_ids):
        for b in block_ids:
            self.replicas.pop(b, None)
        return True

    def referenced_blocks(self):
        refs = set()
        for e in self.ns.values():
            for b in e.get("blocks", []):
                refs.add(b["block_id"])
        return refs


class Cluster:
    """Estado volatil del lider: DN vivos, cargas en curso, ordenes pendientes."""

    def __init__(self):
        self.dns = {}          # node_id -> {address, last, used, blocks:set}
        self.commands = {}     # node_id -> {"replicate": [...], "delete": [...]}
        self.pending_gc = {}   # block_id -> desde cuando no esta referenciado
        self.uploads = {}      # upload_id -> {path, expires}
        self.inflight = {}     # (block_id, target) -> instante de la orden
        self.rotation = 0
        self.lock = threading.Lock()

    def state_of(self, node_id, now):
        d = self.dns.get(node_id)
        if d is None:
            return "DEAD"
        age = now - d["last"]
        if age > DEAD_AFTER:
            return "DEAD"
        if age > SUSPECT_AFTER:
            return "SUSPECT"
        return "ALIVE"

    def alive(self, now):
        return [n for n in self.dns if self.state_of(n, now) == "ALIVE"]

    def pick(self, count, now, exclude=()):
        cands = [n for n in self.alive(now) if n not in exclude]
        cands.sort(key=lambda n: self.dns[n]["used"])
        if not cands:
            return []
        self.rotation += 1
        k = self.rotation % len(cands)
        ordered = cands[k:] + cands[:k]       # rota el primario del pipeline
        return ordered[:count]


class MetaService(rpc.MetaServiceServicer, rpc.ClusterServiceServicer):
    def __init__(self, state, cluster):
        self.s = state
        self.c = cluster

    # ---------------- utilidades ----------------
    def _leader_hint(self):
        leader = self.s._getLeader()
        if leader is None:
            return ""
        host = str(leader.address if hasattr(leader, "address") else leader).rsplit(":", 1)[0]
        return f"{host}:{GRPC_PORT}"

    def _require_leader(self, ctx):
        if not self.s._isLeader():
            ctx.set_trailing_metadata((("leader", self._leader_hint()),))
            ctx.abort(grpc.StatusCode.UNAVAILABLE, f"NOT_LEADER {self._leader_hint()}")

    def _uid(self, ctx):
        md = dict(ctx.invocation_metadata())
        token = md.get("authorization", "").replace("Bearer ", "")
        try:
            return common.verify_jwt(token)
        except Exception:
            ctx.abort(grpc.StatusCode.UNAUTHENTICATED, "AUTH_EXPIRED token invalido o vencido")

    def _path(self, ctx, uid, path):
        path = posixpath.normpath("/" + path.lstrip("/"))
        home = f"/users/{uid}"
        if path != home and not path.startswith(home + "/"):
            log.warning("ACCESO DENEGADO uid=%s path=%s", uid, path)
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, f"NOT_OWNER {path} esta fuera de {home}")
        return path

    def _call(self, ctx, fn, *args):
        try:
            res = fn(*args, sync=True, timeout=5)
        except SyncObjException as e:
            ctx.abort(grpc.StatusCode.UNAVAILABLE, f"NO_QUORUM {e}")
        if isinstance(res, dict) and "error" in res:
            code = {
                "PATH_EXISTS": grpc.StatusCode.ALREADY_EXISTS,
                "NO_SUCH_PATH": grpc.StatusCode.NOT_FOUND,
                "VERSION_CONFLICT": grpc.StatusCode.ABORTED,
                "LOCKED": grpc.StatusCode.FAILED_PRECONDITION,
                "STALE_FENCING_TOKEN": grpc.StatusCode.FAILED_PRECONDITION,
                "DIR_NOT_EMPTY": grpc.StatusCode.FAILED_PRECONDITION,
                "INSUFFICIENT_REPLICAS": grpc.StatusCode.UNAVAILABLE,
            }.get(res["error"], grpc.StatusCode.FAILED_PRECONDITION)
            extra = {k: v for k, v in res.items() if k != "error"}
            ctx.abort(code, f"{res['error']} {extra if extra else ''}".strip())
        return res

    def _block_info(self, idx, b, op):
        now = time.time()
        reps = [self.c.dns[n]["address"] for n in self.s.replicas.get(b["block_id"], [])
                if self.c.state_of(n, now) == "ALIVE"]
        random.shuffle(reps)
        return pb.BlockInfo(index=idx, block_id=b["block_id"], length=b["length"], sha256=b["sha256"],
                            replicas=reps, token=common.block_token(b["block_id"], op))

    # ---------------- MetaService ----------------
    def Login(self, req, ctx):
        self._require_leader(ctx)
        stored = USERS.get(req.user)
        if not stored or not common.check_password(req.password, stored):
            log.warning("LOGIN FALLIDO user=%s", req.user)
            ctx.abort(grpc.StatusCode.UNAUTHENTICATED, "BAD_CREDENTIALS")
        home = f"/users/{req.user}"
        if home not in self.s.ns:
            self._call(ctx, self.s.mkdir, home, req.user, time.time(), True)
        token, exp = common.issue_jwt(req.user)
        return pb.TokenResp(token=token, uid=req.user, expires_at=exp)

    def List(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        e = self.s.ns.get(path)
        if e is None:
            ctx.abort(grpc.StatusCode.NOT_FOUND, "NO_SUCH_PATH")
        if e["type"] == "file":
            items = [(posixpath.basename(path), e)]
        else:
            prefix = path.rstrip("/") + "/"
            items = [(p[len(prefix):], v) for p, v in self.s.ns.items()
                     if p.startswith(prefix) and "/" not in p[len(prefix):]]
        return pb.DirListing(entries=[
            pb.DirEntry(name=n, is_dir=v["type"] == "dir", size=v.get("size", 0),
                        version=v.get("version", 0), mtime=v["mtime"]) for n, v in sorted(items)])

    def Mkdir(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        self._call(ctx, self.s.mkdir, path, uid, time.time(), False)
        return pb.Ack(ok=True, message=f"creado {path}")

    def Remove(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        if path == f"/users/{uid}":
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "no se puede borrar el directorio raiz del usuario")
        res = self._call(ctx, self.s.remove, path, req.recursive)
        return pb.Ack(ok=True, message=f"eliminadas {res['removed']} entradas")

    def Stat(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        e = self.s.ns.get(path)
        if e is None:
            ctx.abort(grpc.StatusCode.NOT_FOUND, "NO_SUCH_PATH")
        if e["type"] == "dir":
            return pb.FileInfo(path=path, is_dir=True, mtime=e["mtime"])
        return pb.FileInfo(path=path, is_dir=False, size=e["size"], version=e["version"],
                           block_size=common.BLOCK_SIZE, file_sha256=e["sha256"], mtime=e["mtime"],
                           blocks=[self._block_info(i, b, "R") for i, b in enumerate(e["blocks"])])

    def AllocateBlocks(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        now = time.time()
        upload_id = "up-" + uuid.uuid4().hex[:8]
        blocks = []
        with self.c.lock:
            for i in range(req.count):
                nodes = self.c.pick(common.REPLICATION, now)
                if len(nodes) < common.MIN_REPLICAS:
                    ctx.abort(grpc.StatusCode.UNAVAILABLE, f"INSUFFICIENT_REPLICAS vivos={len(nodes)}")
                bid = "b-" + uuid.uuid4().hex
                blocks.append(pb.BlockInfo(index=i, block_id=bid, token=common.block_token(bid, "W"),
                                           replicas=[self.c.dns[n]["address"] for n in nodes]))
            self.c.uploads[upload_id] = {"path": path, "expires": now + 3600}
        return pb.UploadPlan(upload_id=upload_id, block_size=common.BLOCK_SIZE,
                             min_replicas=common.MIN_REPLICAS, blocks=blocks)

    def Commit(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        addr_to_node = {d["address"]: n for n, d in self.c.dns.items()}
        blocks = [{"block_id": b.block_id, "length": b.length, "sha256": b.sha256,
                   "replicas": [addr_to_node.get(a, a) for a in b.replicas]} for b in req.blocks]
        res = self._call(ctx, self.s.commit, path, uid, req.base_version, req.fencing_token,
                         req.size, req.file_sha256, blocks, req.overwrite, time.time())
        self.c.uploads.pop(req.upload_id, None)
        log.info("COMMIT %s v%s (%d bloques) por %s", path, res["version"], len(blocks), uid)
        return self.Stat(pb.PathReq(path=path), ctx)

    def Lock(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        holder = dict(ctx.invocation_metadata()).get("x-client-id", uid)
        res = self._call(ctx, self.s.lock, path, holder, req.mode or "EXCLUSIVE", req.ttl_s or 30, time.time())
        return pb.Lease(path=path, mode=res["mode"], fencing_token=res["token"],
                        expires_at=res["expires"], holder=res["holder"])

    def Unlock(self, req, ctx):
        self._require_leader(ctx)
        uid = self._uid(ctx)
        path = self._path(ctx, uid, req.path)
        self._call(ctx, self.s.unlock, path, req.fencing_token)
        return pb.Ack(ok=True)

    def Status(self, req, ctx):
        self._require_leader(ctx)
        now = time.time()
        status = self.s.getStatus()
        under = 0
        if self.s._isLeader():
            for b in self.s.referenced_blocks():
                live = [n for n in self.s.replicas.get(b, []) if self.c.state_of(n, now) == "ALIVE"]
                if len(live) < common.REPLICATION:
                    under += 1
        return pb.ClusterStatus(
            node_id=NODE_ID, is_leader=self.s._isLeader(), leader=self._leader_hint(),
            raft_peers=RAFT_PEERS, under_replicated=under,
            datanodes=[pb.DataNodeState(node_id=n, address=d["address"], state=self.c.state_of(n, now),
                                        last_heartbeat=d["last"], blocks=len(d["blocks"]), used=d["used"])
                       for n, d in sorted(self.c.dns.items())])

    # ---------------- ClusterService ----------------
    def Heartbeat(self, req, ctx):
        self._require_leader(ctx)
        now = time.time()
        with self.c.lock:
            prev = self.c.dns.get(req.node_id)
            was_dead = prev is None or self.c.state_of(req.node_id, now) == "DEAD"
            self.c.dns[req.node_id] = {"address": req.address, "last": now, "used": req.used,
                                       "blocks": set(req.blocks)}
            cmds = self.c.commands.pop(req.node_id, {"replicate": [], "delete": []})
        if was_dead:
            log.info("DN %s (%s) registrado/regreso", req.node_id, req.address)
        # reconciliacion con el informe de bloques
        refs = self.s.referenced_blocks()
        for b in req.blocks:
            reps = self.s.replicas.get(b)
            if b in refs and reps is not None and req.node_id not in reps:
                try:
                    self.s.add_replica(b, req.node_id, sync=True, timeout=5)
                except SyncObjException:
                    pass
            elif b not in refs and b not in self.c.pending_gc and not self._is_staging(b):
                self.c.pending_gc[b] = now
        return pb.Commands(
            replicate=[pb.ReplicateCmd(**c) for c in cmds["replicate"]],
            delete=cmds["delete"])

    def _is_staging(self, block_id):
        return block_id not in self.s.replicas and bool(self.c.uploads)


def replica_manager(state, cluster):
    """Hilo del lider: detecta DN caidos, re-replica bloques y recolecta basura."""
    dead_reported = set()
    while True:
        time.sleep(3)
        if not state._isLeader():
            continue
        now = time.time()
        try:
            for n in list(cluster.dns):
                st = cluster.state_of(n, now)
                if st == "DEAD" and n not in dead_reported:
                    log.warning("DN %s MUERTO: se retira de las replicas y se re-replica", n)
                    state.drop_node(n, sync=True, timeout=5)
                    dead_reported.add(n)
                elif st == "ALIVE":
                    dead_reported.discard(n)
            refs = state.referenced_blocks()
            with cluster.lock:
                for b in refs:
                    holders = [n for n in state.replicas.get(b, []) if cluster.state_of(n, now) == "ALIVE"]
                    missing = common.REPLICATION - len(holders)
                    if missing <= 0 or not holders:
                        continue
                    targets = [t for t in cluster.pick(len(cluster.dns), now, exclude=holders)
                               if now - cluster.inflight.get((b, t), 0) > 30][:missing]
                    for t in targets:
                        src = random.choice(holders)
                        cluster.commands.setdefault(src, {"replicate": [], "delete": []})["replicate"].append(
                            {"block_id": b, "target": cluster.dns[t]["address"],
                             "token": common.block_token(b, "W")})
                        cluster.inflight[(b, t)] = now
                        log.info("RE-REPLICACION %s: %s -> %s", b[:12], src, t)
                # recoleccion de basura diferida
                expired = [b for b, t0 in cluster.pending_gc.items() if now - t0 > GC_GRACE and b not in refs]
                for b in expired:
                    for n, d in cluster.dns.items():
                        if b in d["blocks"]:
                            cluster.commands.setdefault(n, {"replicate": [], "delete": []})["delete"].append(b)
                    cluster.pending_gc.pop(b, None)
                for b in list(cluster.pending_gc):
                    if b in refs:
                        cluster.pending_gc.pop(b)
                for uid_, up in list(cluster.uploads.items()):
                    if up["expires"] < now:
                        cluster.uploads.pop(uid_)
            if expired:
                state.forget_blocks(expired, sync=True, timeout=5)
        except SyncObjException as e:
            log.warning("gestor de replicas sin quorum: %s", e)
        except Exception:
            log.exception("error en gestor de replicas")


def main():
    state = MetaState()
    cluster = Cluster()
    svc = MetaService(state, cluster)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=common.GRPC_OPTS)
    rpc.add_MetaServiceServicer_to_server(svc, server)
    rpc.add_ClusterServiceServicer_to_server(svc, server)
    common.add_port(server, f"0.0.0.0:{GRPC_PORT}", "node")
    server.start()
    threading.Thread(target=replica_manager, args=(state, cluster), daemon=True).start()
    log.info("MS %s escuchando gRPC :%d, Raft %s, pares %s", NODE_ID, GRPC_PORT, RAFT_SELF, RAFT_PEERS)
    last_leader = None
    while True:
        time.sleep(1)
        leader = svc._leader_hint()
        if leader != last_leader:
            log.info("LIDER RAFT actual: %s (yo soy lider: %s)", leader or "ninguno", state._isLeader())
            last_leader = leader


if __name__ == "__main__":
    main()
