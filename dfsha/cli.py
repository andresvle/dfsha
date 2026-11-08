"""CLI de DFSha.

  dfsha login <usuario>            dfsha put <local> [remota] [--overwrite]
  dfsha ls [ruta]                  dfsha get <remota> [local]
  dfsha cd <ruta> / dfsha pwd      dfsha stat <ruta>
  dfsha mkdir <ruta>               dfsha rm [-r] <ruta> / dfsha rmdir <ruta>
  dfsha cat <ruta> [--offset N --len N]           (RF3 open + read)
  dfsha write <ruta> --offset N --data TEXTO      (RF3 open rw + lock + write + close)
  dfsha append <ruta> --data TEXTO                (RF3 open a + write + close)
  dfsha lock <ruta> [--hold SEG]                  (toma el lease y lo retiene)
  dfsha status                                    (estado del cluster)
"""
import argparse
import getpass
import json
import os
import posixpath
import sys
import time

from dfsha.client import Client, DFSError

SESSION = os.path.expanduser(os.environ.get("DFSHA_SESSION", "~/.dfsha_session.json"))


def load_session():
    try:
        with open(SESSION) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_session(s):
    with open(SESSION, "w") as f:
        json.dump(s, f)
    os.chmod(SESSION, 0o600)


def resolve(sess, path):
    cwd = sess.get("cwd") or f"/users/{sess.get('uid', '')}"
    if not path:
        return cwd
    return posixpath.normpath(path if path.startswith("/") else posixpath.join(cwd, path))


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def main(argv=None):
    p = argparse.ArgumentParser(prog="dfsha")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("login"); s.add_argument("user"); s.add_argument("--password")
    for name in ("ls", "cd", "mkdir", "rmdir", "stat"):
        s = sub.add_parser(name); s.add_argument("path", nargs="?" if name in ("ls", "cd") else None, default="")
    sub.add_parser("pwd"); sub.add_parser("status"); sub.add_parser("whoami")
    s = sub.add_parser("rm"); s.add_argument("path"); s.add_argument("-r", action="store_true")
    s = sub.add_parser("put"); s.add_argument("local"); s.add_argument("remote", nargs="?"); s.add_argument("--overwrite", action="store_true")
    s = sub.add_parser("get"); s.add_argument("remote"); s.add_argument("local", nargs="?")
    s = sub.add_parser("cat"); s.add_argument("path"); s.add_argument("--offset", type=int, default=0); s.add_argument("--len", type=int)
    s = sub.add_parser("write"); s.add_argument("path"); s.add_argument("--offset", type=int, default=0); s.add_argument("--data", required=True)
    s.add_argument("--no-lock", action="store_true"); s.add_argument("--hold", type=float, default=0)
    s = sub.add_parser("append"); s.add_argument("path"); s.add_argument("--data", required=True)
    s = sub.add_parser("lock"); s.add_argument("path"); s.add_argument("--hold", type=float, default=15)
    a = p.parse_args(argv)

    sess = load_session()
    c = Client(token=sess.get("token"))
    c.uid = sess.get("uid")
    try:
        t0 = time.time()
        if a.cmd == "login":
            pwd = a.password or os.environ.get("DFSHA_PASSWORD")
            if not pwd:
                try:
                    pwd = getpass.getpass("Contrasena: ")
                except EOFError:
                    raise DFSError("NO_PASSWORD", "sin terminal: use --password o DFSHA_PASSWORD") from None
            res = c.login(a.user, pwd)
            save_session({"token": res.token, "uid": res.uid, "cwd": f"/users/{res.uid}"})
            print(f"Sesion iniciada como {res.uid}; directorio /users/{res.uid}")
        elif a.cmd == "whoami":
            print(sess.get("uid", "(sin sesion)"))
        elif a.cmd == "pwd":
            print(resolve(sess, ""))
        elif a.cmd == "cd":
            path = resolve(sess, a.path)
            if not c.stat(path).is_dir:
                raise DFSError("NOT_A_DIR", path)
            sess["cwd"] = path
            save_session(sess)
            print(path)
        elif a.cmd == "ls":
            for e in c.ls(resolve(sess, a.path)):
                kind = "d" if e.is_dir else "-"
                ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(e.mtime))
                print(f"{kind} {human(e.size):>9}  v{e.version:<3} {ts}  {e.name}{'/' if e.is_dir else ''}")
        elif a.cmd == "mkdir":
            print(c.mkdir(resolve(sess, a.path)).message)
        elif a.cmd in ("rm", "rmdir"):
            path = resolve(sess, a.path)
            if a.cmd == "rmdir" and not c.stat(path).is_dir:
                raise DFSError("NOT_A_DIR", path)
            print(c.rm(path, recursive=getattr(a, "r", False)).message)
        elif a.cmd == "stat":
            info = c.stat(resolve(sess, a.path))
            print(f"ruta: {info.path}\ntipo: {'directorio' if info.is_dir else 'archivo'}")
            if not info.is_dir:
                print(f"tamano: {info.size} B  version: {info.version}  sha256: {info.file_sha256}")
                for b in info.blocks:
                    print(f"  bloque {b.index}: {b.block_id[:14]}  {human(b.length):>9}  replicas={list(b.replicas)}")
        elif a.cmd == "put":
            remote = resolve(sess, a.remote or os.path.basename(a.local))
            info = c.put(a.local, remote, overwrite=a.overwrite)
            dt = time.time() - t0
            print(f"put {a.local} -> {info.path}: {human(info.size)} en {len(info.blocks)} bloques, "
                  f"v{info.version}, {dt:.2f} s ({info.size / 1048576 / max(dt, 1e-6):.1f} MB/s)")
            print(f"sha256 {info.file_sha256}")
        elif a.cmd == "get":
            remote = resolve(sess, a.remote)
            local = a.local or posixpath.basename(remote)
            info = c.get(remote, local)
            dt = time.time() - t0
            print(f"get {info.path} -> {local}: {human(info.size)}, v{info.version}, {dt:.2f} s "
                  f"({info.size / 1048576 / max(dt, 1e-6):.1f} MB/s), sha256 verificado")
        elif a.cmd == "cat":
            f = c.open(resolve(sess, a.path), "r")
            sys.stdout.write(f.read(a.offset, a.len).decode(errors="replace"))
            sys.stdout.write("\n")
            f.close()
        elif a.cmd == "write":
            f = c.open(resolve(sess, a.path), "rw")
            if not a.no_lock:
                lease = f.lock()
                print(f"lock EXCLUSIVE concedido, fencing_token={lease.fencing_token}")
            f.write(a.offset, a.data.encode())
            if a.hold:
                print(f"esperando {a.hold} s antes de confirmar" + ("" if a.no_lock else " (con el lease tomado)") + "...")
                time.sleep(a.hold)
            info = f.close()
            print(f"write confirmado: {info.path} v{info.version}, {info.size} B")
        elif a.cmd == "append":
            f = c.open(resolve(sess, a.path), "a")
            f.write(0, a.data.encode())
            info = f.close()
            print(f"append confirmado: {info.path} v{info.version}, {info.size} B")
        elif a.cmd == "lock":
            path = resolve(sess, a.path)
            lease = c.lock(path, "EXCLUSIVE", int(a.hold) + 5)
            print(f"lock EXCLUSIVE sobre {path}: fencing_token={lease.fencing_token}, holder={lease.holder}")
            time.sleep(a.hold)
            c.unlock(path, lease.fencing_token)
            print("lock liberado")
        elif a.cmd == "status":
            st = c.status()
            print(f"MS consultado: {st.node_id}  lider: {st.leader}  bloques sub-replicados: {st.under_replicated}")
            for d in st.datanodes:
                print(f"  {d.node_id:<6} {d.address:<22} {d.state:<8} bloques={d.blocks:<4} usado={human(d.used)}")
    except DFSError as e:
        print(f"ERROR {e.code}: {e.message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
