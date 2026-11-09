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

SUMMARY="$OUT/resumen_$(date +%Y%m%d_%H%M%S).txt"
title() { echo; echo "=================== $* ==================="; }
check() { # descripcion condicion... : registra PASA/FALLA en el resumen
  local desc=$1; shift
  if "$@" >/dev/null 2>&1; then echo "[PASA] $desc" | tee -a "$SUMMARY"; else echo "[FALLA] $desc" | tee -a "$SUMMARY"; fi
}
has() { grep -q -- "$2" <<<"$1"; }
PYRUN() { docker run --rm --network host --env-file /opt/dfsha/client.env -e TMPDIR=/work \
  -v /opt/dfsha/certs:/certs:ro -v /opt/dfsha/tests:/tests:ro -v "$PWD":/work -w /work dfsha:latest python "$@"; }
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
  check "carga: hash del archivo recuperado identico al original" test "$(cut -d' ' -f1 sha_original.txt)" = "$(cut -d' ' -f1 sha_recuperado.txt)"
  local nb; nb=$(dfsha stat datos/grande.bin | grep -c 'bloque ')
  check "carga: archivo particionado en varios bloques ($nb)" test "$nb" -gt 1
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
  local final; final=$(dfsha cat rf3.txt); echo "$final"
  dfsha stat rf3.txt | head -4
  check "rf3: read(0,20) devuelve el prefijo" test "$(dfsha cat rf3.txt --offset 0 --len 20)" = "Hola MUNDO. Linea or"
  check "rf3: write parcial + append reflejados en el contenido final" has "$final" "Hola MUNDO. Linea original del archivo de pruebas RF3."
  check "rf3: anexo presente al final" has "$final" "+anexo"
}

prueba_concurrencia() {
  title "3. CONCURRENCIA (dos clientes sobre el mismo archivo/bloque)"
  dfsha login andres
  printf '0000000000000000000000000000000000000000\n' > conc.txt
  dfsha put conc.txt conc.txt --overwrite
  echo "--- cliente A toma el lock y retiene 8 s; cliente B intenta escribir el mismo bloque"
  (dfsha write conc.txt --offset 0 --data "AAAAA" --hold 8 2>&1 | sed 's/^/[A] /' > conc_a.txt) &
  sleep 3
  dfsha write conc.txt --offset 0 --data "BBBBB" 2>&1 | sed 's/^/[B] /' | tee conc_b.txt
  wait; cat conc_a.txt
  local c1; c1=$(dfsha cat conc.txt)
  echo "--- contenido final (gana A; B fue rechazado con LOCKED):"; echo "$c1"
  check "concurrencia: B recibe LOCKED mientras A tiene el lease" grep -q LOCKED conc_b.txt
  check "concurrencia: gana A (AAAAA) y no se mezclan bytes de B" test "${c1:0:5}" = "AAAAA"
  echo "--- sin locks: B abre la version v, A confirma v+1 antes que B -> control optimista (VERSION_CONFLICT)"
  (dfsha write conc.txt --offset 20 --data "DDDDD" --no-lock --hold 6 2>&1 | sed 's/^/[B2] /' > conc_b2.txt) &
  sleep 3
  dfsha write conc.txt --offset 10 --data "CCCCC" --no-lock 2>&1 | sed 's/^/[A2] /'
  wait; cat conc_b2.txt
  local c2; c2=$(dfsha cat conc.txt); echo "$c2"
  check "concurrencia: escritor con version vieja recibe VERSION_CONFLICT" grep -q VERSION_CONFLICT conc_b2.txt
  check "concurrencia: la escritura rechazada no aparece (sin DDDDD)" bash -c "! grep -q DDDDD <<<'$c2'"
}

prueba_fallos() {
  title "4. REPLICACION Y FALLOS (caida de un DN y del lider MS)"
  dfsha login andres
  dfsha stat datos/grande.bin | sed -n '4,6p'
  echo "--- se detiene dn-2"; remote dn-2 "docker stop dfsha"
  sleep 3
  rm -f tras_caida.bin; dfsha get datos/grande.bin tras_caida.bin && sha256sum tras_caida.bin
  check "fallos: con dn-2 caido el archivo se lee completo e identico" test "$(sha256sum tras_caida.bin | cut -d' ' -f1)" = "$(cut -d' ' -f1 sha_original.txt)"
  echo "--- esperando deteccion (DEAD) y re-replicacion..."; sleep 25; dfsha status
  sleep 20; local st; st=$(dfsha status); echo "$st"
  check "fallos: dn-2 detectado como DEAD" has "$st" "dn-2 .*DEAD"
  check "fallos: re-replicacion completa (0 bloques sub-replicados)" has "$st" "sub-replicados: 0"
  echo "--- se detiene el MS lider: $(leader)"
  LNODE=$(leader | cut -d. -f1)
  remote $LNODE "docker stop dfsha"
  T0=$(date +%s.%N); for i in $(seq 150); do dfsha ls >/dev/null 2>&1 && break; sleep 0.2; done; T1=$(date +%s.%N)
  echo "metadatos disponibles otra vez en $(echo "$T1 - $T0" | bc) s"; local nl; nl=$(dfsha status | head -1); echo "$nl"
  check "fallos: nuevo lider elegido distinto de $LNODE" bash -c "! grep -q 'lider: $LNODE' <<<'$nl'"
  check "fallos: escritura confirmada despues de la eleccion" dfsha put rf3.txt despues_de_eleccion.txt --overwrite
  echo "--- se restauran los nodos"; remote dn-2 "docker start dfsha"; remote $LNODE "docker start dfsha"
  sleep 20; st=$(dfsha status); echo "$st"
  check "fallos: tras restaurar, los 4 DN ALIVE" test "$(grep -c ALIVE <<<"$st")" -ge 4
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
  check "consistencia: con dn-3 desconectado la version confirmada se lee identica" cmp consist.bin consist_leido.bin
  check "consistencia: ningun bloque anuncia a dn-3 mientras esta caido" bash -c "! dfsha stat consist.bin | grep -q \"$(getent hosts dn-3.dfsha.internal | cut -d' ' -f1):8000\""
  echo "--- dn-3 regresa: se reincorpora y el gestor completa R=3"; remote dn-3 "docker start dfsha"
  sleep 30; local st; st=$(dfsha status); echo "$st"; dfsha stat consist.bin | sed -n '4,8p'
  check "consistencia: dn-3 reincorporado (ALIVE) y 0 bloques sub-replicados" bash -c "grep -q 'dn-3 .*ALIVE' <<<'$st' && grep -q 'sub-replicados: 0' <<<'$st'"
}

prueba_seguridad() {
  title "6. SEGURIDAD (acceso autorizado/denegado, canal y almacenamiento cifrados)"
  dfsha login andres
  echo "--- captura del trafico hacia dn-1 mientras se sube un archivo con texto conocido"
  CAP=$(remote_async dn-1 "dnf -y -q install tcpdump >/dev/null 2>&1; timeout 20 tcpdump -i any -A -s0 tcp port 8000 2>/dev/null > /tmp/cap.txt; echo bytes_capturados=\$(wc -c < /tmp/cap.txt) lineas_con_texto_plano=\$(grep -c secreto /tmp/cap.txt)")
  sleep 12
  echo "secreto de andres" > secreto.txt; dfsha put secreto.txt secreto.txt --overwrite
  for i in 1 2 3; do dfsha put secreto.txt secreto_$i.txt --overwrite >/dev/null; done
  local cap; cap=$(remote_result dn-1 $CAP); echo "captura en dn-1 (puerto 8000): $cap"
  check "seguridad: trafico capturado en el puerto 8000 sin texto plano" bash -c "grep -q 'lineas_con_texto_plano=0' <<<'$cap' && ! grep -q 'bytes_capturados=0 ' <<<'$cap'"
  echo "--- juan intenta leer el archivo de andres"
  DFSHA_PASSWORD=juan123 dfsha login juan
  local d1 d2; d1=$(dfsha cat /users/andres/secreto.txt 2>&1); d2=$(dfsha ls /users/andres 2>&1); echo "$d1"; echo "$d2"
  check "seguridad: juan no puede leer ni listar el espacio de andres (NOT_OWNER)" bash -c "grep -q NOT_OWNER <<<'$d1' && grep -q NOT_OWNER <<<'$d2'"
  echo "--- contrasena incorrecta"; local d3; d3=$(DFSHA_PASSWORD=mala dfsha login andres 2>&1); echo "$d3"
  check "seguridad: contrasena incorrecta rechazada (BAD_CREDENTIALS)" has "$d3" BAD_CREDENTIALS
  echo "--- conexiones sin certificado de cliente o sin TLS (mTLS exige certificado)"
  local pr; pr=$(docker run --rm --network host --env-file /opt/dfsha/client.env -v /opt/dfsha/certs:/certs:ro dfsha:latest python -m dfsha.probe); echo "$pr"
  check "seguridad: conexiones sin certificado y en texto plano rechazadas" bash -c "test \$(grep -c RECHAZADO <<<'$pr') -eq 2"
  echo "--- bloques en disco de los 4 DN (cifrado AES-256-GCM, no aparece el texto plano):"
  local n hits=0
  for n in dn-1 dn-2 dn-3 dn-4; do
    local r; r=$(remote $n "grep -rl 'secreto de andres' /var/lib/dfsha/blocks | wc -l"); echo "$n: archivos con texto plano = $r"
    hits=$((hits + ${r:-1}))
  done
  check "seguridad: el texto plano no aparece en ningun bloque en disco" test "$hits" -eq 0
  check "seguridad: el cliente no tiene secretos del cluster ni clave de nodo/CA" bash -c "! test -e /opt/dfsha/cluster.env && ! test -e /opt/dfsha/certs/node.key && ! test -e /opt/dfsha/certs/ca.key"
  dfsha login andres
}

prueba_auditoria() {
  title "8. AUDITORIA DE SEGURIDAD E INTEGRIDAD (hallazgos F01-F06 y R01 de la revision)"
  dfsha login andres
  local out; out=$(PYRUN /tests/auditoria_seguridad.py --gc 2>&1 | grep -v WARNING); echo "$out"
  check "auditoria: todas las comprobaciones de seguridad e integridad pasan" bash -c "grep -q RESUMEN <<<'$out' && ! grep -q FALLA <<<'$out'"
}

prueba_durabilidad() {
  title "9. DURABILIDAD DEL PLANO DE CONTROL (commit y caida inmediata de los 3 MS)"
  local out; out=$(PYRUN /tests/regresion.py antes_caida 2>&1 | grep -v WARNING); echo "$out"
  check "regresion: RF2/RF3, concurrencia y ataques con sesion valida" bash -c "grep -q ESTADO_GUARDADO <<<'$out' && ! grep -q FALLA <<<'$out'"
  echo "--- docker kill simultaneo de ms-1, ms-2 y ms-3 inmediatamente despues del commit"
  local c1 c2 c3
  c1=$(remote_async ms-1 "docker kill dfsha; sleep 2; docker start dfsha"); c2=$(remote_async ms-2 "docker kill dfsha; sleep 2; docker start dfsha"); c3=$(remote_async ms-3 "docker kill dfsha; sleep 2; docker start dfsha")
  remote_result ms-1 $c1; remote_result ms-2 $c2; remote_result ms-3 $c3
  sleep 15
  out=$(PYRUN /tests/regresion.py despues_caida 2>&1 | grep -v WARNING); echo "$out"
  check "durabilidad: el ultimo commit sobrevive al reinicio abrupto de los 3 MS" bash -c "grep -q 'R10' <<<'$out' && ! grep -q FALLA <<<'$out'"
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
  rendimiento) prueba_rendimiento ;; auditoria) prueba_auditoria ;; durabilidad) prueba_durabilidad ;;
  all) prueba_carga; prueba_rf3; prueba_concurrencia; prueba_seguridad; prueba_consistencia; prueba_fallos
       prueba_auditoria; prueba_durabilidad; prueba_rendimiento ;;
esac 2>&1 | tee -a "$OUT/evidencia_$(date +%Y%m%d_%H%M%S).log"
echo; echo "=================== RESUMEN ==================="
cat "$SUMMARY" 2>/dev/null
echo "pasan: $(grep -c PASA "$SUMMARY" 2>/dev/null)  fallan: $(grep -c FALLA "$SUMMARY" 2>/dev/null)"
! grep -q FALLA "$SUMMARY" 2>/dev/null
