"""Nodo de datos (DN): guarda bloques inmutables cifrados y los replica en pipeline.

Escritura de un bloque: tmp/ -> fsync -> verificar SHA-256 -> rename atomico a blocks/.
Un bloque a medio recibir nunca queda con su nombre final.
"""
import logging
import os
import threading
import time
from concurrent import futures

import grpc

from dfsha import common
from dfsha import dfsha_pb2 as pb
from dfsha import dfsha_pb2_grpc as rpc

log = logging.getLogger("data")

NODE_ID = os.environ.get("DFSHA_NODE_ID", "dn-1")
ADVERTISE = os.environ.get("DFSHA_ADVERTISE", "127.0.0.1:8000")
GRPC_PORT = int(os.environ.get("DFSHA_GRPC_PORT", "8000"))
META = os.environ.get("DFSHA_META", "meta.dfsha.internal:7000")
DATA_DIR = os.environ.get("DFSHA_DATA", "/data")
HEARTBEAT = float(os.environ.get("DFSHA_HEARTBEAT", "3"))
ENCRYPT = os.environ.get("DFSHA_ENCRYPT", "1") == "1"
BLOCKS = os.path.join(DATA_DIR, "blocks")
TMP = os.path.join(DATA_DIR, "tmp")


def block_path(block_id):
    return os.path.join(BLOCKS, block_id)


_block_locks = {}
_block_locks_mx = threading.Lock()


def _lock_for(block_id):
    with _block_locks_mx:
        return _block_locks.setdefault(block_id, threading.Lock())


class BlockExists(Exception):
    pass


def _fsync_dir(path):
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:          # Windows no permite abrir directorios
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def store(block_id, data):
    """Escritura atomica e idempotente: tmp + fsync + rename + fsync del directorio.
    Un bloque es inmutable: repetir los mismos bytes se acepta; bytes distintos se rechazan."""
    with _lock_for(block_id):
        if os.path.exists(block_path(block_id)):
            if common.sha256(load(block_id)) == common.sha256(data):
                return
            raise BlockExists(block_id)
        tmp = os.path.join(TMP, f"{block_id}.{os.getpid()}.{threading.get_ident()}")
        payload = common.encrypt_block(data) if ENCRYPT else data
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, block_path(block_id))
        _fsync_dir(BLOCKS)


def load(block_id):
    with open(block_path(block_id), "rb") as f:
        blob = f.read()
    return common.decrypt_block(blob) if ENCRYPT else blob


def stream_block(block_id, token, data, pipeline):
    """Genera los mensajes de un PutBlock: cabecera + datos en trozos de 1 MiB."""
    first = True
    for i in range(0, max(len(data), 1), common.CHUNK):
        part = data[i:i + common.CHUNK]
        if first:
            yield pb.BlockChunk(block_id=block_id, token=token, sha256=common.sha256(data),
                                pipeline=pipeline, data=part)
            first = False
        else:
            yield pb.BlockChunk(data=part)


class DataService(rpc.DataServiceServicer):
    def PutBlock(self, request_iterator, ctx):
        header = None
        parts = []
        for chunk in request_iterator:
            if header is None:
                header = chunk
            parts.append(chunk.data)
        if header is None:
            ctx.abort(grpc.StatusCode.INVALID_ARGUMENT, "stream vacio")
        if not common.check_block_token(header.token, header.block_id, "W"):
            log.warning("token de bloque invalido para %s", header.block_id)
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "BAD_BLOCK_TOKEN")
        data = b"".join(parts)
        digest = common.sha256(data)
        if digest != header.sha256:
            ctx.abort(grpc.StatusCode.DATA_LOSS, "CHECKSUM_MISMATCH")
        try:
            store(header.block_id, data)
        except BlockExists:
            log.warning("RECHAZO: %s ya existe con otro contenido (bloques inmutables)", header.block_id[:12])
            ctx.abort(grpc.StatusCode.ALREADY_EXISTS, "BLOCK_EXISTS contenido distinto para un block_id existente")
        stored = [ADVERTISE]
        receipts = [common.block_receipt(header.block_id, ADVERTISE, digest, len(data))]
        # reenvio en pipeline al siguiente DN; si falla, se salta ese nodo y se sigue con el resto
        rest = list(header.pipeline)
        while rest:
            nxt, rest = rest[0], rest[1:]
            try:
                stub = rpc.DataServiceStub(common.channel(nxt, "node"))
                ack = stub.PutBlock(stream_block(header.block_id, header.token, data, rest), timeout=60)
                stored.extend(ack.stored_on)
                receipts.extend(ack.receipts)
                break
            except grpc.RpcError as e:
                log.warning("pipeline: no se pudo reenviar %s a %s (%s); se sigue con %d copias",
                            header.block_id[:12], nxt, e.code().name, len(stored))
        log.info("bloque %s guardado (%d B), copias=%s", header.block_id[:12], len(data), stored)
        return pb.BlockAck(block_id=header.block_id, stored_on=stored, sha256=digest, length=len(data),
                           receipts=receipts)

    def GetBlock(self, req, ctx):
        if not (common.check_block_token(req.token, req.block_id, "R")
                or common.check_block_token(req.token, req.block_id, "W")):
            ctx.abort(grpc.StatusCode.PERMISSION_DENIED, "BAD_BLOCK_TOKEN")
        if not os.path.exists(block_path(req.block_id)):
            ctx.abort(grpc.StatusCode.NOT_FOUND, "NO_SUCH_BLOCK")
        data = load(req.block_id)
        digest = common.sha256(data)
        for i in range(0, max(len(data), 1), common.CHUNK):
            yield pb.BlockChunk(block_id=req.block_id, sha256=digest, data=data[i:i + common.CHUNK])


def used_bytes():
    return sum(os.path.getsize(os.path.join(BLOCKS, f)) for f in os.listdir(BLOCKS))


def heartbeat_loop():
    """Envia heartbeats + informe de bloques al lider MS y ejecuta sus ordenes."""
    leader = None
    while True:
        try:
            candidates = ([leader] if leader else []) + common.resolve_all(META)
            cmds = None
            for addr in candidates:
                try:
                    stub = rpc.ClusterServiceStub(common.channel(addr, "node"))
                    cmds = stub.Heartbeat(pb.HeartbeatReq(
                        node_id=NODE_ID, address=ADVERTISE, used=used_bytes(),
                        blocks=os.listdir(BLOCKS)), timeout=5)
                    leader = addr
                    break
                except grpc.RpcError as e:
                    if "NOT_LEADER" in (e.details() or ""):
                        hint = e.details().split("NOT_LEADER", 1)[1].strip()
                        if hint and hint not in candidates:
                            candidates.append(hint)
                    if addr == leader:
                        leader = None
            if cmds:
                for b in cmds.delete:
                    try:
                        os.remove(block_path(b))
                        log.info("GC: bloque %s borrado por orden del MS", b[:12])
                    except FileNotFoundError:
                        pass
                for r in cmds.replicate:
                    threading.Thread(target=replicate, args=(r,), daemon=True).start()
        except Exception:
            log.exception("error en heartbeat")
        time.sleep(HEARTBEAT)


def replicate(cmd):
    try:
        data = load(cmd.block_id)
        stub = rpc.DataServiceStub(common.channel(cmd.target, "node"))
        stub.PutBlock(stream_block(cmd.block_id, cmd.token, data, []), timeout=60)
        log.info("RE-REPLICACION: bloque %s copiado a %s", cmd.block_id[:12], cmd.target)
    except Exception as e:
        log.warning("re-replicacion de %s a %s fallo: %s", cmd.block_id[:12], cmd.target, e)


def main():
    os.makedirs(BLOCKS, exist_ok=True)
    os.makedirs(TMP, exist_ok=True)
    for f in os.listdir(TMP):          # restos de escrituras interrumpidas
        os.remove(os.path.join(TMP, f))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32), options=common.GRPC_OPTS)
    rpc.add_DataServiceServicer_to_server(DataService(), server)
    common.add_port(server, f"0.0.0.0:{GRPC_PORT}", "node")
    server.start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    log.info("DN %s escuchando :%d (anuncia %s), MS=%s, cifrado=%s", NODE_ID, GRPC_PORT, ADVERTISE, META, ENCRYPT)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
