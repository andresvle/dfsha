#!/bin/bash
# Pruebas minimas exigidas (enunciado 5.2). Se ejecuta en la instancia "client".
#   ./run_tests.sh all | carga | rf3 | concurrencia | fallos | consistencia | seguridad | rendimiento
# La inyeccion de fallos detiene/inicia el contenedor de un DN o MS por SSM (sin tocar al cliente).
set -uo pipefail
REGION=${AWS_REGION:-us-east-1}
OUT=${OUT:-${HOME:-/root}/resultados}
mkdir -p "$OUT"
cd "$OUT"
export DFSHA_PASSWORD=${DFSHA_PASSWORD:-andres123}

title() { echo; echo "=================== $* ==================="; }
iid() { aws ec2 describe-instances --region $REGION --filters "Name=tag:Name,Values=$1" "Name=instance-state-name,Values=running" --query 'Reservations[0].Instances[0].InstanceId' --output text; }
remote() { # nodo comando : ejecuta en otra instancia via SSM y espera
  local id cmd
  id=$(iid "$1")
  cmd=$(aws ssm send-command --region $REGION --instance-ids "$id" --document-name AWS-RunShellScript \
        --parameters "commands=[\"$2\"]" --query Command.CommandId --output text)
  aws ssm wait command-executed --region $REGION --command-id "$cmd" --instance-id "$id" 2>/dev/null || true
  aws ssm get-command-invocation --region $REGION --command-id "$cmd" --instance-id "$id" --query StandardOutputContent --output text
}
remote_async() { # nodo comando : lanza sin esperar e imprime el id del comando
  aws ssm send-command --region $REGION --instance-ids "$(iid "$1")" --document-name AWS-RunShellScript \
    --parameters "commands=[\"$2\"]" --query Command.CommandId --output text
}
remote_result() { # nodo id_comando
  local id; id=$(iid "$1")
  aws ssm wait command-executed --region $REGION --command-id "$2" --instance-id "$id" 2>/dev/null || true
  aws ssm get-command-invocation --region $REGION --command-id "$2" --instance-id "$id" --query StandardOutputContent --output text
}
leader() { dfsha status | head -1 | sed 's/.*lider: \([^ ]*\).*/\1/'; }

prueba_carga() {
  title "1. CARGA Y RECUPERACION (archivo grande particionado, hash identico)"
  dfsha login andres
  dd if=/dev/urandom of=grande.bin bs=1M count=${SIZE_MB:-200} status=none
  sha256sum grande.bin | tee sha_original.txt
  dfsha mkdir datos 2>/dev/null || true
  dfsha put grande.bin datos/grande.bin --overwrite
  dfsha stat datos/grande.bin | head -12
  rm -f recuperado.bin
  dfsha get datos/grande.bin recuperado.bin
  sha256sum recuperado.bin | tee sha_recuperado.txt
  if [ "$(cut -d' ' -f1 sha_original.txt)" = "$(cut -d' ' -f1 sha_recuperado.txt)" ]; then echo "RESULTADO: hash IDENTICO"; else echo "RESULTADO: hash DISTINTO"; fi
  dfsha status
}

prueba_rf3() {
  title "2. ACCESO RF3 (open/read/write/lock/close con la semantica declarada)"
  dfsha login andres
  printf 'Hola DFSha. Linea original del archivo de pruebas RF3.\n' > rf3.txt
  dfsha put rf3.txt rf3.txt --overwrite
  echo "--- read(offset=0, n=20)"; dfsha cat rf3.txt --offset 0 --len 20
  echo "--- write(offset=5) modificacion parcial con lock"; dfsha write rf3.txt --offset 5 --data "MUNDO"
  dfsha cat rf3.txt
  echo "--- append (anexado)"; dfsha append rf3.txt --data " +anexo"
  dfsha cat rf3.txt
  dfsha stat rf3.txt | head -4
}

prueba_concurrencia() {
  title "3. CONCURRENCIA (dos clientes sobre el mismo archivo/bloque)"
  dfsha login andres
  printf '0000000000000000000000000000000000000000\n' > conc.txt
  dfsha put conc.txt conc.txt --overwrite
  echo "--- cliente A toma el lock y retiene 8 s; cliente B intenta escribir el mismo bloque"
  (dfsha write conc.txt --offset 0 --data "AAAAA" --hold 8 2>&1 | sed 's/^/[A] /') &
  sleep 2
  dfsha write conc.txt --offset 0 --data "BBBBB" 2>&1 | sed 's/^/[B] /'
  wait
  echo "--- contenido final (gana A; B fue rechazado con LOCKED):"; dfsha cat conc.txt
  echo "--- sin locks: B abre la version v, A confirma v+1 antes que B -> control optimista (VERSION_CONFLICT)"
  (dfsha write conc.txt --offset 20 --data "DDDDD" --no-lock --hold 6 2>&1 | sed 's/^/[B2] /') &
  sleep 2
  dfsha write conc.txt --offset 10 --data "CCCCC" --no-lock 2>&1 | sed 's/^/[A2] /'
  wait
  dfsha cat conc.txt
}

prueba_fallos() {
  title "4. REPLICACION Y FALLOS (caida de un DN y del lider MS)"
  dfsha login andres
  dfsha stat datos/grande.bin | sed -n '4,6p'
  echo "--- se detiene dn-2"; remote dn-2 "docker stop dfsha"
  sleep 3
  rm -f tras_caida.bin; dfsha get datos/grande.bin tras_caida.bin && sha256sum tras_caida.bin
  echo "--- esperando deteccion (DEAD) y re-replicacion..."; sleep 25; dfsha status
  sleep 20; dfsha status
  echo "--- se detiene el MS lider: $(leader)"
  LNODE=$(leader | cut -d. -f1)
  remote $LNODE "docker stop dfsha"
  T0=$(date +%s.%N); until dfsha ls >/dev/null 2>&1; do sleep 0.2; done; T1=$(date +%s.%N)
  echo "metadatos disponibles otra vez en $(echo "$T1 - $T0" | bc) s"; dfsha status | head -1
  dfsha put rf3.txt despues_de_eleccion.txt --overwrite
  echo "--- se restauran los nodos"; remote dn-2 "docker start dfsha"; remote $LNODE "docker start dfsha"
}

prueba_consistencia() {
  title "5. CONSISTENCIA (replica desconectada durante una escritura)"
  dfsha login andres
  dd if=/dev/urandom of=consist.bin bs=1M count=24 status=none
  echo "--- se desconecta dn-3 y se escribe"; remote dn-3 "docker stop dfsha"
  dfsha put consist.bin consist.bin --overwrite
  dfsha stat consist.bin | sed -n '4,8p'
  echo "--- lectores: la version confirmada se lee completa desde las replicas vivas"
  dfsha get consist.bin consist_leido.bin && cmp consist.bin consist_leido.bin && echo "contenido identico"
  echo "--- dn-3 regresa: se reincorpora y el gestor completa R=3"; remote dn-3 "docker start dfsha"
  sleep 30; dfsha status; dfsha stat consist.bin | sed -n '4,8p'
}

prueba_seguridad() {
  title "6. SEGURIDAD (acceso autorizado/denegado, canal y almacenamiento cifrados)"
  dfsha login andres
  echo "--- captura del trafico hacia dn-1 mientras se sube un archivo con texto conocido"
  CAP=$(remote_async dn-1 "dnf -y -q install tcpdump >/dev/null 2>&1; timeout 20 tcpdump -i any -A -s0 tcp port 8000 2>/dev/null > /tmp/cap.txt; echo bytes_capturados=\$(wc -c < /tmp/cap.txt) lineas_con_texto_plano=\$(grep -c secreto /tmp/cap.txt)")
  sleep 12
  echo "secreto de andres" > secreto.txt; dfsha put secreto.txt secreto.txt --overwrite
  for i in 1 2 3; do dfsha put secreto.txt secreto_$i.txt --overwrite >/dev/null; done
  echo "captura en dn-1 (puerto 8000): $(remote_result dn-1 $CAP)"
  echo "--- juan intenta leer el archivo de andres"
  DFSHA_PASSWORD=juan123 dfsha login juan
  dfsha cat /users/andres/secreto.txt
  dfsha ls /users/andres
  echo "--- contrasena incorrecta"; DFSHA_PASSWORD=mala dfsha login andres
  echo "--- conexiones sin certificado de cliente o sin TLS (mTLS exige certificado)"
  docker run --rm --network host --env-file /opt/dfsha/cluster.env -v /opt/dfsha/certs:/certs:ro dfsha:latest python -m dfsha.probe
  echo "--- bloque en disco del DN (cifrado AES-256-GCM, no aparece el texto plano):"
  remote dn-1 "grep -rl 'secreto de andres' /var/lib/dfsha/blocks || echo 'texto plano NO encontrado en disco'"
  dfsha login andres
}

prueba_rendimiento() {
  title "7. RENDIMIENTO (latencia y throughput bajo dos condiciones)"
  dfsha login andres
  echo "condicion,tamano_mb,operacion,segundos,mb_s" > rendimiento.csv
  for mb in 16 64 256; do
    dd if=/dev/urandom of=perf_$mb.bin bs=1M count=$mb status=none
    for rep in 1 2 3; do
      for par in 1 4; do
        s=$(date +%s.%N); DFSHA_PARALLEL=$par dfsha put perf_$mb.bin perf_$mb.bin --overwrite >/dev/null; e=$(date +%s.%N)
        t=$(echo "$e - $s" | bc); echo "paralelo=$par,$mb,put,$t,$(echo "scale=2; $mb / $t" | bc)" >> rendimiento.csv
        s=$(date +%s.%N); DFSHA_PARALLEL=$par dfsha get perf_$mb.bin perf_get.bin >/dev/null; e=$(date +%s.%N)
        t=$(echo "$e - $s" | bc); echo "paralelo=$par,$mb,get,$t,$(echo "scale=2; $mb / $t" | bc)" >> rendimiento.csv
      done
    done
  done
  echo "--- latencia de metadatos (20 x stat)"
  s=$(date +%s.%N); for i in $(seq 20); do dfsha stat perf_16.bin >/dev/null; done; e=$(date +%s.%N)
  echo "stat promedio (incluye arranque del contenedor CLI): $(echo "scale=3; ($e - $s) / 20" | bc) s"
  column -s, -t rendimiento.csv
}

case "${1:-all}" in
  carga) prueba_carga ;; rf3) prueba_rf3 ;; concurrencia) prueba_concurrencia ;;
  fallos) prueba_fallos ;; consistencia) prueba_consistencia ;; seguridad) prueba_seguridad ;;
  rendimiento) prueba_rendimiento ;;
  all) prueba_carga; prueba_rf3; prueba_concurrencia; prueba_seguridad; prueba_consistencia; prueba_fallos; prueba_rendimiento ;;
esac 2>&1 | tee -a "$OUT/evidencia_$(date +%Y%m%d_%H%M%S).log"
