"""Utilidades compartidas: configuracion, TLS/mTLS, JWT, tokens de bloque y cifrado en reposo."""
import base64
import hashlib
import hmac
import json
import logging
import os
import socket
import time

import grpc
import jwt
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

logging.basicConfig(
    level=os.environ.get("DFSHA_LOG", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

BLOCK_SIZE = int(os.environ.get("DFSHA_BLOCK_SIZE", str(8 * 1024 * 1024)))
REPLICATION = int(os.environ.get("DFSHA_REPLICATION", "3"))
MIN_REPLICAS = int(os.environ.get("DFSHA_MIN_REPLICAS", "2"))
CHUNK = 1024 * 1024
JWT_SECRET = os.environ.get("DFSHA_JWT_SECRET", "dev-jwt-secret-change-me")
BLOCK_TOKEN_SECRET = os.environ.get("DFSHA_BLOCK_SECRET", "dev-block-secret-change-me").encode()
CERT_DIR = os.environ.get("DFSHA_CERTS", "/certs")
TLS_ENABLED = os.environ.get("DFSHA_TLS", "1") == "1"
GRPC_OPTS = [
    ("grpc.max_send_message_length", 64 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
]


# ---------------------------------------------------------------- TLS ------
def _read(name):
    with open(os.path.join(CERT_DIR, name), "rb") as f:
        return f.read()


def server_credentials(cert_name):
    """mTLS: el servidor presenta su certificado y exige uno firmado por la CA interna."""
    return grpc.ssl_server_credentials(
        [(_read(f"{cert_name}.key"), _read(f"{cert_name}.crt"))],
        root_certificates=_read("ca.crt"),
        require_client_auth=True,
    )


def channel_credentials(cert_name):
    return grpc.ssl_channel_credentials(
        root_certificates=_read("ca.crt"),
        private_key=_read(f"{cert_name}.key"),
        certificate_chain=_read(f"{cert_name}.crt"),
    )


_channels = {}


def channel(address, cert_name):
    """Canal gRPC reutilizable. Los certificados usan el nombre 'dfsha' como SAN comun."""
    key = (address, cert_name)
    ch = _channels.get(key)
    if ch is None:
        opts = list(GRPC_OPTS)
        if TLS_ENABLED:
            opts.append(("grpc.ssl_target_name_override", "dfsha.internal"))
            ch = grpc.secure_channel(address, channel_credentials(cert_name), options=opts)
        else:
            ch = grpc.insecure_channel(address, options=opts)
        _channels[key] = ch
    return ch


def add_port(server, address, cert_name):
    if TLS_ENABLED:
        server.add_secure_port(address, server_credentials(cert_name))
    else:
        server.add_insecure_port(address)


def resolve_all(service):
    """Resuelve un nombre logico host:puerto a todas sus direcciones (transparencia de ubicacion)."""
    out = []
    for item in service.split(","):
        host, port = item.strip().rsplit(":", 1)
        try:
            infos = socket.getaddrinfo(host, int(port), socket.AF_INET, socket.SOCK_STREAM)
            for info in infos:
                addr = f"{info[4][0]}:{port}"
                if addr not in out:
                    out.append(addr)
        except socket.gaierror:
            continue
    return out


# ----------------------------------------------------------- usuarios ------
def hash_password(password, salt=None):
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 200_000)
    return base64.b64encode(salt).decode() + "$" + base64.b64encode(digest).decode()


def check_password(password, stored):
    salt_b64, _ = stored.split("$", 1)
    return hmac.compare_digest(hash_password(password, base64.b64decode(salt_b64)), stored)


def issue_jwt(uid, ttl=3600):
    exp = int(time.time()) + ttl
    return jwt.encode({"uid": uid, "exp": exp}, JWT_SECRET, algorithm="HS256"), exp


def verify_jwt(token):
    return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])["uid"]


# --------------------------------------------------- tokens de bloque ------
def block_token(block_id, op, ttl=600):
    """Token firmado por el MS: autoriza una operacion (R/W) sobre un unico bloque."""
    exp = int(time.time()) + ttl
    msg = f"{block_id}|{op}|{exp}".encode()
    sig = hmac.new(BLOCK_TOKEN_SECRET, msg, hashlib.sha256).hexdigest()
    return f"{op}.{exp}.{sig}"


def check_block_token(token, block_id, op):
    try:
        t_op, exp, sig = token.split(".", 2)
    except ValueError:
        return False
    if t_op != op or int(exp) < time.time():
        return False
    msg = f"{block_id}|{t_op}|{exp}".encode()
    return hmac.compare_digest(hmac.new(BLOCK_TOKEN_SECRET, msg, hashlib.sha256).hexdigest(), sig)


# ------------------------------------------------- cifrado en reposo ------
def _kek():
    raw = os.environ.get("DFSHA_KEK")
    if raw:
        return base64.b64decode(raw)
    return hashlib.sha256(b"dev-kek-change-me").digest()


def encrypt_block(plain):
    """Cifrado de sobre: DEK aleatoria por bloque (AES-256-GCM), envuelta con la KEK."""
    dek = AESGCM.generate_key(bit_length=256)
    n1, n2 = os.urandom(12), os.urandom(12)
    ct = AESGCM(dek).encrypt(n1, plain, None)
    wrapped = AESGCM(_kek()).encrypt(n2, dek, None)
    header = json.dumps({
        "n1": base64.b64encode(n1).decode(),
        "n2": base64.b64encode(n2).decode(),
        "dek": base64.b64encode(wrapped).decode(),
    }).encode()
    return len(header).to_bytes(4, "big") + header + ct


def decrypt_block(blob):
    hlen = int.from_bytes(blob[:4], "big")
    header = json.loads(blob[4:4 + hlen])
    dek = AESGCM(_kek()).decrypt(base64.b64decode(header["n2"]), base64.b64decode(header["dek"]), None)
    return AESGCM(dek).decrypt(base64.b64decode(header["n1"]), blob[4 + hlen:], None)


def sha256(data):
    return hashlib.sha256(data).hexdigest()
