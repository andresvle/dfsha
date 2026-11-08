#!/bin/sh
# Genera la CA interna del cluster y los certificados mTLS de nodos y clientes.
# Todos usan el SAN dfsha.internal (los canales hacen ssl_target_name_override).
set -e
OUT=${1:-certs}
mkdir -p "$OUT"
cd "$OUT"
[ -f ca.key ] || openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 365 -subj "/CN=DFSha Internal CA"
for name in node client; do
  [ -f $name.crt ] && continue
  openssl req -newkey rsa:2048 -nodes -keyout $name.key -out $name.csr -subj "/CN=dfsha-$name"
  printf "subjectAltName=DNS:dfsha.internal\nextendedKeyUsage=serverAuth,clientAuth\n" > $name.ext
  openssl x509 -req -in $name.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out $name.crt -days 365 -extfile $name.ext
  rm -f $name.csr $name.ext
done
chmod 644 *.crt *.key
echo "certificados en $(pwd)"
