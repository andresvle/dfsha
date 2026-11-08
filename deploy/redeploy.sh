#!/bin/bash
# Publica una nueva version del codigo y la reinstala en todas las instancias (via SSM),
# volviendo a ejecutar el user-data de cada una. Los datos en /var/lib/dfsha se conservan.
#   ./deploy/redeploy.sh [nombre1 nombre2 ...]   (por defecto: todas)
set -euo pipefail
REGION=${AWS_REGION:-us-east-1}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-dfsha-${ACCOUNT}-artifacts}
aws s3 cp dfsha.tar.gz "s3://$BUCKET/dfsha.tar.gz"
aws s3 cp deploy/node_bootstrap.sh "s3://$BUCKET/node_bootstrap.sh"
names=${*:-"ms-1 ms-2 ms-3 dn-1 dn-2 dn-3 dn-4 client"}
ids=""
for n in $names; do
  ids+=" $(aws ec2 describe-instances --region $REGION --filters Name=tag:Name,Values=$n Name=instance-state-name,Values=running --query 'Reservations[0].Instances[0].InstanceId' --output text)"
done
cmd='TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300"); curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/user-data > /root/ud.sh; rm -f /var/lib/dfsha/READY; bash /root/ud.sh; cat /var/lib/dfsha/READY'
cid=$(aws ssm send-command --region $REGION --instance-ids $ids --document-name AWS-RunShellScript \
  --parameters "$(jq -nc --arg c "$cmd" '{commands:[$c], executionTimeout:["1800"]}')" --query Command.CommandId --output text)
echo "comando $cid enviado a:$ids"
for id in $ids; do
  aws ssm wait command-executed --region $REGION --command-id $cid --instance-id $id 2>/dev/null || true
  aws ssm get-command-invocation --region $REGION --command-id $cid --instance-id $id --query StandardOutputContent --output text | tail -n 1
done
