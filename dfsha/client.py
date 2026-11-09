"""Biblioteca cliente de DFSha: descubrimiento del lider, put/get en paralelo y primitivas RF3.

El cliente solo conoce un nombre logico de servicio (DFSHA_META, p. ej. meta.dfsha.internal:7000);
las direcciones de los DN las obtiene del MS en cada operacion.
"""
import hashlib
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc

from dfsha import common
from dfsha import dfsha_pb2 as pb
from dfsha import dfsha_pb2_grpc as rpc
from dfsha.data_server import stream_block

log = logging.getLogger("client")


class DFSError(Exception):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class Client:
    def __init__(self, meta=None, token=None, parallel=int(os.environ.get("DFSHA_PARALLEL", "4"))):
        self.meta = meta or os.environ.get("DFSHA_META", "meta.dfsha.internal:7000")
        self.token = token
        self.uid = None
        self.leader = None
        self.parallel = parallel
        self.client_id = "c-" + uuid.uuid4().hex[:8]

    # ---------------- plano de control ----------------
    def _md(self):
        md = [("x-client-id", self.client_id)]
        if self.token:
            md.append(("authorization", f"Bearer {self.token}"))
        return md

    def _meta(self, method, req, retries=6):
        """Llama al MS lider; sigue la pista NOT_LEADER y reintenta durante una eleccion."""
        import time
        last = None
        for attempt in range(retries):
            candidates = ([self.leader] if self.leader else []) + common.resolve_all(self.meta)
            seen = []
            while candidates:
                addr = candidates.pop(0)
                if addr in seen:
                    continue
                seen.append(addr)
                stub = rpc.MetaServiceStub(common.channel(addr, "client"))
                try:
                    res = getattr(stub, method)(req, metadata=self._md(), timeout=10)
                    self.leader = addr
                    return res
                except grpc.RpcError as e:
                    details = e.details() or ""
                    if e.code() == grpc.StatusCode.UNAVAILABLE:
                        last = e
                        if "NOT_LEADER" in details:
                            hint = details.split("NOT_LEADER", 1)[1].strip()
                            if hint:
                                candidates.insert(0, hint)
                        if addr == self.leader:
                            self.leader = None
                        continue
                    code = details.split(" ", 1)[0] if details else e.code().name
                    raise DFSError(code, details) from None
            time.sleep(0.5 * (attempt + 1))
        raise DFSError("UNAVAILABLE", f"ningun MS lider disponible ({last.details() if last else ''})")

    def login(self, user, password):
        res = self._meta("Login", pb.LoginReq(user=user, password=password))
        self.token, self.uid = res.token, res.uid
        return res

    def home(self):
        return f"/users/{self.uid}"

    def ls(self, path):
        return self._meta("List", pb.PathReq(path=path)).entries

    def mkdir(self, path):
        return self._meta("Mkdir", pb.PathReq(path=path))

    def rm(self, path, recursive=False):
        return self._meta("Remove", pb.RemoveReq(path=path, recursive=recursive))

    def stat(self, path):
        return self._meta("Stat", pb.PathReq(path=path))

    def status(self):
        return self._meta("Status", pb.Empty())

    def lock(self, path, mode="EXCLUSIVE", ttl=30):
        return self._meta("Lock", pb.LockReq(path=path, mode=mode, ttl_s=ttl))

    def unlock(self, path, fencing_token):
        return self._meta("Unlock", pb.LeaseRef(path=path, fencing_token=fencing_token))

    # ---------------- plano de datos ----------------
    def _put_block(self, plan_block, data):
        """Envia un bloque al primer DN del pipeline; exige >= min_replicas copias."""
        pipeline = list(plan_block.replicas)
        last_err = None
        for start in range(len(pipeline)):
            first, rest = pipeline[start], pipeline[start + 1:]
            try:
                stub = rpc.DataServiceStub(common.channel(first, "client"))
                ack = stub.PutBlock(stream_block(plan_block.block_id, plan_block.token, data, rest), timeout=120)
                return ack
            except grpc.RpcError as e:
                last_err = e
                log.warning("DN %s no respondio para %s; probando el siguiente", first, plan_block.block_id[:12])
        raise DFSError("INSUFFICIENT_REPLICAS", str(last_err.details() if last_err else ""))

    def _upload_blocks(self, path, chunks):
        """Sube una lista de bytes como bloques nuevos. Devuelve (upload_id, [BlockInfo])."""
        if not chunks:
            plan = self._meta("AllocateBlocks", pb.AllocReq(path=path, count=0))
            return plan.upload_id, []
        plan = self._meta("AllocateBlocks", pb.AllocReq(path=path, count=len(chunks)))
        results = [None] * len(chunks)

        def work(i):
            for attempt in range(3):
                pb_ = plan.blocks[i] if attempt == 0 else self._meta(
                    "AllocateBlocks", pb.AllocReq(path=path, count=1, upload_id=plan.upload_id)).blocks[0]
                try:
                    ack = self._put_block(pb_, chunks[i])
                except DFSError as e:
                    log.warning("bloque %d: %s; se pide otro pipeline", i, e)
                    continue
                if len(ack.stored_on) >= plan.min_replicas:
                    results[i] = pb.BlockInfo(index=i, block_id=ack.block_id, length=ack.length,
                                              sha256=ack.sha256, replicas=list(ack.stored_on),
                                              receipts=list(ack.receipts))
                    return
                log.warning("bloque %d solo en %d copias; se pide otro pipeline", i, len(ack.stored_on))
            raise DFSError("INSUFFICIENT_REPLICAS", f"bloque {i}")

        with ThreadPoolExecutor(self.parallel) as ex:
            list(ex.map(work, range(len(chunks))))
        return plan.upload_id, results

    def _get_block(self, b):
        last = None
        for addr in b.replicas:
            try:
                stub = rpc.DataServiceStub(common.channel(addr, "client"))
                data = b"".join(c.data for c in stub.GetBlock(pb.GetBlockReq(block_id=b.block_id, token=b.token), timeout=120))
                if common.sha256(data) != b.sha256:
                    log.warning("hash incorrecto de %s en %s; se lee otra replica", b.block_id[:12], addr)
                    continue
                return data
            except grpc.RpcError as e:
                last = e
                log.warning("replica %s fallo para %s (%s); se usa la siguiente", addr, b.block_id[:12], e.code().name)
        raise DFSError("BLOCK_UNAVAILABLE", f"{b.block_id} ({last.details() if last else 'sin replicas'})")

    def put(self, local, remote, overwrite=False):
        """RF2 send(): particiona, sube en paralelo y confirma atomicamente."""
        with open(local, "rb") as f:
            data = f.read()
        bs = common.BLOCK_SIZE
        chunks = [data[i:i + bs] for i in range(0, len(data), bs)]
        upload_id, blocks = self._upload_blocks(remote, chunks)
        return self._meta("Commit", pb.CommitReq(
            path=remote, upload_id=upload_id, base_version=-1, size=len(data),
            file_sha256=hashlib.sha256(data).hexdigest(), blocks=blocks, overwrite=overwrite))

    def get(self, remote, local):
        """RF2 recv(): lee los bloques en paralelo de distintos DN y verifica el hash total."""
        info = self.stat(remote)
        with ThreadPoolExecutor(self.parallel) as ex:
            parts = list(ex.map(self._get_block, info.blocks))
        data = b"".join(parts)
        if hashlib.sha256(data).hexdigest() != info.file_sha256:
            raise DFSError("CHECKSUM_MISMATCH", "el archivo reconstruido no coincide")
        with open(local, "wb") as f:
            f.write(data)
        return info

    def open(self, path, mode="r"):
        return DFile(self, path, mode)


class DFile:
    """Primitivas RF3. Semantica declarada:
      * open fija la version vigente como instantanea de lectura (lectura repetible);
      * write admite creacion ('w'), reemplazo ('w' sobre existente), anexado ('a') y
        modificacion parcial por bloque ('rw', copia en escritura de los bloques tocados);
      * las escrituras se vuelven visibles para todos solo al close()/commit() (close-to-open);
      * lock() toma un lease EXCLUSIVE con token de cerco; commit valida base_version.
    """

    def __init__(self, client, path, mode):
        self.c = client
        self.path = path
        self.mode = mode
        self.lease = None
        self.dirty = {}          # indice de bloque -> bytes nuevos
        self.closed = False
        try:
            self.info = client.stat(path)
            self.base_version = self.info.version
            self.blocks = list(self.info.blocks)
            self.size = self.info.size
        except DFSError as e:
            if e.code != "NO_SUCH_PATH" or mode == "r":
                raise
            self.info, self.base_version, self.blocks, self.size = None, 0, [], 0
        if mode == "w":
            self.blocks, self.size = [], 0
        self._cache = {}
        self.lock_mx = threading.Lock()

    def lock(self, ttl=30):
        self.lease = self.c.lock(self.path, "EXCLUSIVE", ttl)
        return self.lease

    def unlock(self):
        if self.lease:
            self.c.unlock(self.path, self.lease.fencing_token)
            self.lease = None

    def _block_bytes(self, idx):
        if idx in self.dirty:
            return self.dirty[idx]
        if idx < len(self.blocks):
            if idx not in self._cache:
                self._cache[idx] = self.c._get_block(self.blocks[idx])
            return self._cache[idx]
        return b""

    def read(self, offset=0, n=None):
        if n is None:
            n = self.size - offset
        out = bytearray()
        bs = common.BLOCK_SIZE
        pos, end = offset, min(offset + n, self.size)
        while pos < end:
            idx, off = divmod(pos, bs)
            blk = self._block_bytes(idx)
            take = blk[off:off + (end - pos)]
            if not take:
                break
            out += take
            pos += len(take)
        return bytes(out)

    def write(self, offset, data):
        if self.mode == "r":
            raise DFSError("READ_ONLY", "archivo abierto en modo lectura")
        if self.mode == "a":
            offset = self.size
        if offset < 0:
            raise DFSError("BAD_OFFSET", "offset negativo")
        bs = common.BLOCK_SIZE
        # escribir mas alla del final: el hueco se rellena con ceros (bloques completos nuevos)
        last = (self.size - 1) // bs if self.size else -1
        for gap in range(last + 1, offset // bs):
            if gap not in self.dirty:
                self.dirty[gap] = b"\0" * bs
        if last >= 0 and offset // bs > last and last not in self.dirty:
            blk = self._block_bytes(last)
            self.dirty[last] = blk + b"\0" * (bs - len(blk))
        pos = 0
        while pos < len(data):
            abs_pos = offset + pos
            idx, off = divmod(abs_pos, bs)
            blk = bytearray(self._block_bytes(idx))
            if len(blk) < off:
                blk += b"\0" * (off - len(blk))
            take = data[pos:pos + (bs - off)]
            blk[off:off + len(take)] = take
            self.dirty[idx] = bytes(blk)
            pos += len(take)
        self.size = max(self.size, offset + len(data))
        return len(data)

    def commit(self):
        nblocks = (self.size + common.BLOCK_SIZE - 1) // common.BLOCK_SIZE
        idxs = sorted(self.dirty)
        upload_id, new = self.c._upload_blocks(self.path, [self.dirty[i] for i in idxs])
        new_by_idx = dict(zip(idxs, new))
        final = []
        for i in range(nblocks):
            b = new_by_idx.get(i) or self.blocks[i]
            final.append(pb.BlockInfo(index=i, block_id=b.block_id, length=b.length,
                                      sha256=b.sha256, replicas=list(b.replicas), receipts=list(b.receipts)))
        full = hashlib.sha256()
        for i in range(nblocks):
            full.update(self._block_bytes(i))
        info = self.c._meta("Commit", pb.CommitReq(
            path=self.path, upload_id=upload_id, base_version=self.base_version,
            fencing_token=self.lease.fencing_token if self.lease else 0,
            size=self.size, file_sha256=full.hexdigest(), blocks=final, overwrite=True))
        self.info, self.base_version, self.blocks = info, info.version, list(info.blocks)
        self.dirty, self._cache = {}, {}
        self.lease = None
        return info

    def close(self):
        if self.closed:
            return None
        self.closed = True
        if self.mode != "r" and (self.dirty or self.info is None or self.mode == "w"):
            return self.commit()
        self.unlock()
        return self.info
