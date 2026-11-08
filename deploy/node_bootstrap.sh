#!/bin/bash
# Arranque de una instancia EC2 de DFSha (Amazon Linux 2023). Lo ejecuta el user-data.
# Variables esperadas (las fija launch.sh): ROLE (meta|data|client), NODE_ID, BUCKET, DFSHA_* extra.
set -euxo pipefail
dnf install -y docker bc bind-utils openssl
systemctl enable --now docker
usermod -aG docker ec2-user

mkdir -p /opt/dfsha /var/lib/dfsha
aws s3 cp "s3://${BUCKET}/dfsha.tar.gz" /opt/dfsha/dfsha.tar.gz
tar -xzf /opt/dfsha/dfsha.tar.gz -C /opt/dfsha
aws s3 cp "s3://${BUCKET}/cluster.env" /opt/dfsha/cluster.env
cd /opt/dfsha
docker build -t dfsha:latest .

TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
MY_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

docker rm -f dfsha 2>/dev/null || true      # idempotente: sirve tambien para redesplegar
COMMON="--restart unless-stopped --network host --env-file /opt/dfsha/cluster.env -v /opt/dfsha/certs:/certs:ro -v /var/lib/dfsha:/data"
case "$ROLE" in
  meta)
    docker run -d --name dfsha $COMMON \
      -e DFSHA_NODE_ID="$NODE_ID" \
      -e DFSHA_RAFT_SELF="${NODE_ID}.dfsha.internal:7001" \
      -e DFSHA_RAFT_PEERS="$RAFT_PEERS" \
      dfsha:latest python -m dfsha.meta_server ;;
  data)
    docker run -d --name dfsha $COMMON \
      -e DFSHA_NODE_ID="$NODE_ID" \
      -e DFSHA_ADVERTISE="${MY_IP}:8000" \
      dfsha:latest python -m dfsha.data_server ;;
  client)
    cat > /usr/local/bin/dfsha <<'EOF'
#!/bin/sh
exec docker run --rm -i --network host --env-file /opt/dfsha/cluster.env \
  -e DFSHA_SESSION=/work/.dfsha_session.json -e DFSHA_PASSWORD -e DFSHA_PARALLEL \
  -v /opt/dfsha/certs:/certs:ro -v "$PWD":/work -w /work dfsha:latest dfsha "$@"
EOF
    chmod +x /usr/local/bin/dfsha
    cp -r /opt/dfsha/tests /home/ec2-user/tests && chown -R ec2-user /home/ec2-user/tests ;;
esac
echo "DFSha $ROLE $NODE_ID listo" > /var/lib/dfsha/READY
