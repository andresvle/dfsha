"""Pruebas de regresion y de ataques con sesion valida para las correcciones de DFSha.
uso: python regresion.py [fase]   fase = todo | antes_caida | despues_caida"""
import hashlib, json, os, sys, tempfile, threading, time, uuid
import grpc
from dfsha import common, dfsha_pb2 as pb
from dfsha.client import Client, DFSError

T = tempfile.gettempdir()
R = []
fase = sys.argv[1] if len(sys.argv) > 1 else "todo"
state = os.path.join(T, "regresion_estado.json")


def rep(i, desc, ok, det=""):
    R.append((i, ok)); print(f"[{'PASA' if ok else 'FALLA'}] {i} {desc} :: {det}", flush=True)


def code(f):
    try:
        f(); return "OK"
    except DFSError as e:
        return e.code
    except Exception as e:
        return type(e).__name__ + " " + str(e)[:60]


A = Client(); A.login("andres", "andres123")
J = Client(); J.login("juan", "juan123")
bs = common.BLOCK_SIZE
tag = uuid.uuid4().hex[:6]

if fase in ("todo", "antes_caida"):
    # R1 carga y recuperacion multibloque
    data = os.urandom(5 * bs + 123)
    open(T + "/r1", "wb").write(data)
    info = A.put(T + "/r1", f"/users/andres/r1_{tag}.bin")
    A.get(f"/users/andres/r1_{tag}.bin", T + "/r1b")
    same = open(T + "/r1b", "rb").read() == data
    nodes = {r for b in info.blocks for r in b.replicas}
    rep("R1", "put/get multibloque identico, bloques en varios DN", same and len(info.blocks) == 6 and len(nodes) >= 3,
        f"bloques={len(info.blocks)} dn_distintos={len(nodes)} replicas_min={min(len(b.replicas) for b in info.blocks)}")
    # R2 RF3 escritura parcial que cruza bloques + append
    f = A.open(f"/users/andres/r1_{tag}.bin", "rw"); f.lock()
    f.write(bs - 3, b"XXXXXX"); f.close()
    f = A.open(f"/users/andres/r1_{tag}.bin", "a"); f.write(0, b"FIN"); f.close()
    exp = data[:bs - 3] + b"XXXXXX" + data[bs + 3:] + b"FIN"
    got = A.open(f"/users/andres/r1_{tag}.bin", "r").read()
    rep("R2", "write parcial entre dos bloques + append con la semantica declarada", got == exp, f"len={len(got)}")
    # R3 escritura mas alla del final: hueco con ceros
    open(T + "/r3", "wb").write(b"hola")
    A.put(T + "/r3", f"/users/andres/r3_{tag}")
    f = A.open(f"/users/andres/r3_{tag}", "rw"); f.write(2 * bs + 10, b"Z"); f.close()
    got = A.open(f"/users/andres/r3_{tag}", "r").read()
    rep("R3", "write mas alla del final rellena con ceros", got == b"hola" + b"\0" * (2 * bs + 6) + b"Z", f"len={len(got)}")
    # R4 concurrencia con lock
    open(T + "/r4", "wb").write(b"0" * 40); A.put(T + "/r4", f"/users/andres/r4_{tag}")
    fa = A.open(f"/users/andres/r4_{tag}", "rw"); fa.lock(); fa.write(0, b"AAAAA")
    B = Client(); B.login("andres", "andres123")
    fb = B.open(f"/users/andres/r4_{tag}", "rw"); fb.write(0, b"BBBBB")
    cb = code(fb.close)
    fa.close()
    rep("R4", "segundo escritor recibe LOCKED mientras otro tiene el lease", cb == "LOCKED", cb)
    # R5 concurrencia optimista sin lock
    f1 = A.open(f"/users/andres/r4_{tag}", "rw"); f2 = B.open(f"/users/andres/r4_{tag}", "rw")
    f1.write(10, b"CCCCC"); f1.close(); f2.write(20, b"DDDDD")
    c2 = code(f2.close)
    final = A.open(f"/users/andres/r4_{tag}", "r").read()
    rep("R5", "escritor con version vieja recibe VERSION_CONFLICT y no pisa", c2 == "VERSION_CONFLICT" and final[:15] == b"AAAAA00000CCCCC", c2)
    # R6 ataque con sesion VALIDA propia: bloque inventado sin recibos
    plan = A._meta("AllocateBlocks", pb.AllocReq(path=f"/users/andres/f01_{tag}", count=1))
    bad = pb.BlockInfo(index=0, block_id=plan.blocks[0].block_id, length=10, sha256="0" * 64, replicas=["x:1", "x:1"])
    c = code(lambda: A._meta("Commit", pb.CommitReq(path=f"/users/andres/f01_{tag}", upload_id=plan.upload_id,
                                                  base_version=-1, size=10, file_sha256="1" * 64, blocks=[bad])))
    rep("R6", "commit con sesion valida pero bloque no guardado (sin recibos) se rechaza", c == "INSUFFICIENT_REPLICAS", c)
    # R7 recibos falsificados/duplicados
    bad.receipts.extend([plan.blocks[0].replicas[0] + "|" + "ab" * 32] * 2)
    c = code(lambda: A._meta("Commit", pb.CommitReq(path=f"/users/andres/f01_{tag}", upload_id=plan.upload_id,
                                                  base_version=-1, size=10, file_sha256="1" * 64, blocks=[bad])))
    rep("R7", "recibos falsificados o repetidos no cuentan como copias", c == "INSUFFICIENT_REPLICAS", c)
    # R8 F02 con sesion VALIDA de juan referenciando bloque de andres
    b0 = info.blocks[0]
    jp = J._meta("AllocateBlocks", pb.AllocReq(path=f"/users/juan/robo_{tag}", count=0))
    ref = pb.BlockInfo(index=0, block_id=b0.block_id, length=b0.length, sha256=b0.sha256, replicas=list(b0.replicas))
    c = code(lambda: J._meta("Commit", pb.CommitReq(path=f"/users/juan/robo_{tag}", upload_id=jp.upload_id,
                                                  base_version=-1, size=b0.length, file_sha256="1" * 64, blocks=[ref])))
    c_stale = code(lambda: J._meta("Commit", pb.CommitReq(path=f"/users/juan/robo_{tag}", upload_id=jp.upload_id,
                                                        base_version=7, size=b0.length, file_sha256="1" * 64, blocks=[ref])))
    rep("R8", "juan con sesion valida no puede referenciar bloque de andres (ni con base_version falsa)",
        c == "FOREIGN_BLOCK" and c_stale == "VERSION_CONFLICT" and code(lambda: J.stat(f"/users/juan/robo_{tag}")) == "NO_SUCH_PATH",
        f"{c} / {c_stale}")
    # R9 sesion de otro usuario
    c = code(lambda: J._meta("Commit", pb.CommitReq(path=f"/users/juan/x_{tag}", upload_id=plan.upload_id,
                                                  base_version=-1, size=0, file_sha256="", blocks=[])))
    rep("R9", "juan no puede usar la sesion de carga de andres", c == "UNKNOWN_UPLOAD", c)
    # para la prueba de durabilidad: commit inmediatamente antes de la caida
    open(T + "/dur", "wb").write(b"durable " + tag.encode())
    A.put(T + "/dur", f"/users/andres/dur_{tag}")
    json.dump({"dur": f"/users/andres/dur_{tag}", "r1": f"/users/andres/r1_{tag}.bin",
               "sha": hashlib.sha256(exp).hexdigest()}, open(state, "w"))
    print("ESTADO_GUARDADO", flush=True)

if fase == "despues_caida":
    st = json.load(open(state))
    c = code(lambda: A.stat(st["dur"]))
    rep("R10", "archivo confirmado justo antes de matar los 3 MS sobrevive", c == "OK", c)
    c = code(lambda: A.get(st["r1"], T + "/r10"))
    ok = c == "OK" and hashlib.sha256(open(T + "/r10", "rb").read()).hexdigest() == st["sha"]
    rep("R11", "archivo multibloque intacto tras reinicio total del plano de control", ok, c)

print("RESUMEN", " ".join(f"{i}={'PASA' if ok else 'FALLA'}" for i, ok in R), flush=True)
