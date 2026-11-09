"""Auditoria de seguridad e integridad (hallazgos F01-F06, R01 de la revision). uso: python auditoria_seguridad.py [--gc]"""
import hashlib, os, sys, time, uuid
import grpc
from dfsha import common, dfsha_pb2 as pb, dfsha_pb2_grpc as rpc
from dfsha.client import Client, DFSError
from dfsha.data_server import stream_block
import tempfile
T = tempfile.gettempdir()
R = []
def rep(i, desc, ok, det=""):
    R.append((i, ok)); print(f"[{'PASA' if ok else 'FALLA'}] {i} {desc} :: {det}", flush=True)
def err(f):
    try:
        return "OK", f()
    except DFSError as e:
        return e.code, None
    except grpc.RpcError as e:
        return e.code().name + " " + (e.details() or ""), None
    except Exception as e:
        return type(e).__name__ + " " + str(e)[:80], None
A = Client(); A.login("andres", "andres123")
J = Client(); J.login("juan", "juan123")
tag = uuid.uuid4().hex[:6]
data = os.urandom(3 * 1024 * 1024)
open(T + "/a.bin", "wb").write(data)
A.put(T + "/a.bin", f"/users/andres/aud_{tag}.bin")
info = A.stat(f"/users/andres/aud_{tag}.bin")
# A1 aislamiento basico
c, _ = err(lambda: J.stat(f"/users/andres/aud_{tag}.bin"))
rep("A1", "juan no puede stat archivo de andres", c.startswith("NOT_OWNER"), c)
c, _ = err(lambda: J.ls("/users/juan/../andres"))
rep("A2", "ruta con .. hacia otro usuario rechazada", c.startswith("NOT_OWNER"), c)
# F01 commit fabricado
fake = pb.BlockInfo(index=0, block_id="b-" + uuid.uuid4().hex, length=10, sha256="0" * 64, replicas=["x:1", "x:1"])
c, _ = err(lambda: A._meta("Commit", pb.CommitReq(path=f"/users/andres/fab_{tag}", upload_id="up-inventado", base_version=-1, size=999, file_sha256="1" * 64, blocks=[fake])))
rep("F01", "commit con upload_id inventado, bloque inexistente y replicas repetidas se rechaza", c != "OK", c)
# F02 referencia a bloque ajeno
b0 = info.blocks[0]
ref = pb.BlockInfo(index=0, block_id=b0.block_id, length=b0.length, sha256=b0.sha256, replicas=list(b0.replicas))
c, _ = err(lambda: J._meta("Commit", pb.CommitReq(path=f"/users/juan/robado_{tag}", upload_id="up-x", base_version=-1, size=b0.length, file_sha256=hashlib.sha256(data[:b0.length]).hexdigest(), blocks=[ref])))
stolen = False
if c == "OK":
    c2, _ = err(lambda: J.get(f"/users/juan/robado_{tag}", T + "/robado.bin"))
    stolen = c2 == "OK" and open(T + "/robado.bin", "rb").read() == data[:b0.length]
rep("F02", "juan no puede publicar ni leer un bloque de andres conociendo su id", c != "OK" and not stolen, f"commit={c} lectura_robada={stolen}")
# F03 sobrescritura de bloque inmutable
plan = A._meta("AllocateBlocks", pb.AllocReq(path=f"/users/andres/imm_{tag}", count=1)).blocks[0]
dn = plan.replicas[0]
st = rpc.DataServiceStub(common.channel(dn, "client"))
st.PutBlock(stream_block(plan.block_id, plan.token, b"A" * 1000, []), timeout=30)
c, _ = err(lambda: st.PutBlock(stream_block(plan.block_id, plan.token, b"B" * 1000, []), timeout=30))
got = b"".join(x.data for x in st.GetBlock(pb.GetBlockReq(block_id=plan.block_id, token=plan.token), timeout=30))
rep("F03", "un block_id existente no acepta contenido distinto", c != "OK" and got == b"A" * 1000, f"segundo_put={c} contenido_final={got[:1]!r}")
# S1 token de bloque falsificado / ausente
c, _ = err(lambda: b"".join(x.data for x in st.GetBlock(pb.GetBlockReq(block_id=b0.block_id, token="R.9999999999.deadbeef"), timeout=30)))
rep("S1", "DN rechaza token de bloque falsificado", "PERMISSION_DENIED" in c, c)
# R01 operaciones internas sin rol de nodo
ms = rpc.MetaServiceStub(common.channel(A.leader, "client"))
c, _ = err(lambda: ms.Status(pb.Empty(), timeout=10))
rep("R01a", "Status exige autenticacion (JWT)", c != "OK", c)
cs = rpc.ClusterServiceStub(common.channel(A.leader, "client"))
c, _ = err(lambda: cs.Heartbeat(pb.HeartbeatReq(node_id="dn-falso", address="10.9.9.9:8000", used=0, blocks=[]), timeout=10))
rep("R01b", "certificado de cliente no puede registrar un DN falso por Heartbeat", c != "OK", c)
if c == "OK":
    time.sleep(1)
    names = [d.node_id for d in A.status().datanodes]
    print("   DN registrados tras el ataque:", names, flush=True)
# F06 escritura mas alla del final
open(T + "/s.txt", "wb").write(b"hola")
A.put(T + "/s.txt", f"/users/andres/eof_{tag}.txt")
def far():
    f = A.open(f"/users/andres/eof_{tag}.txt", "rw"); f.write(3 * common.BLOCK_SIZE, b"X"); return f.close()
c, _ = err(far)
rep("F06", "write muy lejos del final termina con resultado o DFSError controlado", c == "OK" or not c.startswith("IndexError"), c)
# C1 lectura repetible (snapshot) tras reemplazo y GC
if "--gc" in sys.argv:
    p = f"/users/andres/snap_{tag}.bin"
    d1 = os.urandom(2 * 1024 * 1024); open(T + "/v1", "wb").write(d1); A.put(T + "/v1", p)
    f = A.open(p, "r")
    d2 = os.urandom(2 * 1024 * 1024); open(T + "/v2", "wb").write(d2); B = Client(); B.login("andres", "andres123"); B.put(T + "/v2", p, overwrite=True)
    print("   esperando 80 s (GC_GRACE=60)...", flush=True); time.sleep(80)
    c, r = err(lambda: f.read(0, 1024))
    rep("F04", "lector con open() previo sigue leyendo su version tras reemplazo + GC", c == "OK" and r == d1[:1024], c)
print("RESUMEN", " ".join(f"{i}={'PASA' if ok else 'FALLA'}" for i, ok in R), flush=True)
