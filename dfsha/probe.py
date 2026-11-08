"""Prueba de seguridad del canal: intenta hablar con el MS sin certificado de cliente y en texto plano.
Ambos intentos deben fallar porque los servidores exigen mTLS."""
import os

import grpc

from dfsha import dfsha_pb2 as pb
from dfsha import dfsha_pb2_grpc as rpc

TARGET = os.environ.get("DFSHA_META", "meta.dfsha.internal:7000").split(",")[0]
CA = open(os.path.join(os.environ.get("DFSHA_CERTS", "/certs"), "ca.crt"), "rb").read()


def attempt(name, channel):
    try:
        rpc.MetaServiceStub(channel).Login(pb.LoginReq(user="andres", password="andres123"), timeout=5)
        print(f"{name}: CONECTO (fallo de seguridad)")
    except grpc.RpcError as e:
        print(f"{name}: RECHAZADO ({e.code().name})")


attempt("TLS sin certificado de cliente",
        grpc.secure_channel(TARGET, grpc.ssl_channel_credentials(CA),
                            options=[("grpc.ssl_target_name_override", "dfsha.internal")]))
attempt("texto plano (sin TLS)", grpc.insecure_channel(TARGET))
