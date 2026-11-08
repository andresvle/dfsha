#!/bin/bash
# Prueba de humo: levanta 3 MS + 4 DN como procesos en una sola maquina (p. ej. CloudShell).
#   scripts/local_cluster.sh start | stop
set -e
cd "$(dirname "$0")/.."
RUN=${RUN:-/tmp/dfsha-local}
if [ "${1:-start}" = "stop" ]; then pkill -f "dfsha.meta_server|dfsha.data_server" || true; exit 0; fi
pip install -q -r requirements.txt
python3 -m grpc_tools.protoc -I proto --python_out=dfsha --grpc_python_out=dfsha proto/dfsha.proto
sed -i 's/^import dfsha_pb2/from dfsha import dfsha_pb2/' dfsha/dfsha_pb2_grpc.py
grep -q meta.dfsha.local /etc/hosts 2>/dev/null || true
mkdir -p $RUN
export PYTHONPATH=$PWD DFSHA_CERTS=$PWD/certs DFSHA_BLOCK_SIZE=1048576 DFSHA_META=127.0.0.1:7100,127.0.0.1:7200,127.0.0.1:7300
export DFSHA_SUSPECT_AFTER=5 DFSHA_DEAD_AFTER=10 DFSHA_GC_GRACE=20
for i in 1 2 3; do
  peers=$(for j in 1 2 3; do [ $j != $i ] && printf "127.0.0.1:7%d01," $j; done; true)
  DFSHA_NODE_ID=ms-$i DFSHA_GRPC_PORT=7${i}00 DFSHA_RAFT_SELF=127.0.0.1:7${i}01 DFSHA_RAFT_BIND=127.0.0.1:7${i}01 \
  DFSHA_RAFT_PEERS=${peers%,} DFSHA_DATA=$RUN/ms-$i nohup python3 -m dfsha.meta_server > $RUN/ms-$i.log 2>&1 &
done
for i in 1 2 3 4; do
  DFSHA_NODE_ID=dn-$i DFSHA_GRPC_PORT=8${i}00 DFSHA_ADVERTISE=127.0.0.1:8${i}00 DFSHA_DATA=$RUN/dn-$i \
  nohup python3 -m dfsha.data_server > $RUN/dn-$i.log 2>&1 &
done
echo "cluster local arrancando; logs en $RUN"
