#!/bin/bash
# Despliega DFSha en AWS Academy desde CloudShell.
#   ./launch.sh            crea bucket, grupo de seguridad, zona DNS privada e instancias
#   ./launch.sh status     muestra instancias y registros DNS
#   ./launch.sh destroy    termina instancias (deja bucket, zona DNS, SG y rol)
#   ./launch.sh destroy-all  termina instancias y borra bucket, zona DNS, SG y rol IAM
# Requiere en el directorio actual: dfsha.tar.gz (codigo + certs) y deploy/node_bootstrap.sh
set -euo pipefail
REGION=${AWS_REGION:-us-east-1}
PROJECT=dfsha
TYPE_META=${TYPE_META:-t3.micro}
TYPE_DATA=${TYPE_DATA:-t3.micro}
TYPE_CLIENT=${TYPE_CLIENT:-t3.micro}
PROFILE=${PROFILE:-dfsha-node-profile}
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
BUCKET=${BUCKET:-dfsha-${ACCOUNT}-artifacts}
ZONE_NAME=dfsha.internal

aws_() { aws --region "$REGION" "$@"; }

if [ "${1:-}" = "status" ]; then
  aws_ ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT" "Name=instance-state-name,Values=pending,running,stopped" \
    --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,InstanceId,State.Name,PrivateIpAddress,PublicIpAddress]' --output table
  exit 0
fi
if [ "${1:-}" = "destroy" ] || [ "${1:-}" = "destroy-all" ]; then
  ids=$(aws_ ec2 describe-instances --filters "Name=tag:Project,Values=$PROJECT" "Name=instance-state-name,Values=pending,running,stopped" --query 'Reservations[].Instances[].InstanceId' --output text)
  if [ -n "$ids" ]; then
    aws_ ec2 terminate-instances --instance-ids $ids --output text
    aws_ ec2 wait instance-terminated --instance-ids $ids
  fi
  if [ "$1" = "destroy-all" ]; then   # borra tambien todo lo que genera costo o estado
    zone=$(aws_ route53 list-hosted-zones-by-name --dns-name $ZONE_NAME --query "HostedZones[?Name=='${ZONE_NAME}.'].Id | [0]" --output text)
    if [ "$zone" != "None" ]; then
      zone=${zone#/hostedzone/}
      recs=$(aws_ route53 list-resource-record-sets --hosted-zone-id $zone --query "ResourceRecordSets[?Type=='A']" --output json)
      if [ "$recs" != "[]" ]; then
        aws_ route53 change-resource-record-sets --hosted-zone-id $zone --change-batch \
          "$(echo "$recs" | jq '{Changes: [.[] | {Action: "DELETE", ResourceRecordSet: .}]}')" >/dev/null
      fi
      aws_ route53 delete-hosted-zone --id $zone >/dev/null
    fi
    sg=$(aws_ ec2 describe-security-groups --filters Name=group-name,Values=dfsha-sg --query 'SecurityGroups[0].GroupId' --output text)
    [ "$sg" != "None" ] && aws_ ec2 delete-security-group --group-id $sg
    aws_ s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true
    if aws iam get-instance-profile --instance-profile-name dfsha-node-profile >/dev/null 2>&1; then
      aws iam remove-role-from-instance-profile --instance-profile-name dfsha-node-profile --role-name dfsha-node-role
      aws iam delete-instance-profile --instance-profile-name dfsha-node-profile
      aws iam delete-role-policy --role-name dfsha-node-role --policy-name dfsha-access
      aws iam detach-role-policy --role-name dfsha-node-role --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
      aws iam delete-role --role-name dfsha-node-role
    fi
    echo "recursos de DFSha eliminados"
  fi
  exit 0
fi

VPC=$(aws_ ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET=$(aws_ ec2 describe-subnets --filters Name=vpc-id,Values=$VPC Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text)
AMI=$(aws_ ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query Parameter.Value --output text)
echo "VPC=$VPC SUBNET=$SUBNET AMI=$AMI BUCKET=$BUCKET"

# 1. artefactos en S3 (bucket privado)
aws_ s3api head-bucket --bucket "$BUCKET" 2>/dev/null || aws_ s3 mb "s3://$BUCKET"
aws_ s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws_ s3 cp dfsha.tar.gz "s3://$BUCKET/dfsha.tar.gz"
if ! aws_ s3 ls "s3://$BUCKET/cluster.env" >/dev/null 2>&1; then
  cat > cluster.env <<EOF
DFSHA_JWT_SECRET=$(openssl rand -hex 32)
DFSHA_BLOCK_SECRET=$(openssl rand -hex 32)
DFSHA_RAFT_PASSWORD=$(openssl rand -hex 24)
DFSHA_KEK=$(openssl rand -base64 32)
DFSHA_META=meta.dfsha.internal:7000
DFSHA_BLOCK_SIZE=8388608
DFSHA_USERS=andres:andres123,juan:juan123
DFSHA_SUSPECT_AFTER=8
DFSHA_DEAD_AFTER=15
DFSHA_GC_GRACE=60
EOF
  aws_ s3 cp cluster.env "s3://$BUCKET/cluster.env"
  rm -f cluster.env
fi

# 1b. rol IAM de las instancias (en AWS Academy se usa LabInstanceProfile: PROFILE=LabInstanceProfile)
if ! aws iam get-instance-profile --instance-profile-name "$PROFILE" >/dev/null 2>&1; then
  aws iam create-role --role-name dfsha-node-role --assume-role-policy-document \
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name dfsha-node-role --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  aws iam put-role-policy --role-name dfsha-node-role --policy-name dfsha-access --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
    {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\"],\"Resource\":\"arn:aws:s3:::$BUCKET/*\"},
    {\"Effect\":\"Allow\",\"Action\":[\"ec2:DescribeInstances\",\"ssm:SendCommand\",\"ssm:GetCommandInvocation\",\"ssm:ListCommandInvocations\"],\"Resource\":\"*\"}]}"
  aws iam create-instance-profile --instance-profile-name "$PROFILE" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "$PROFILE" --role-name dfsha-node-role
  sleep 15   # propagacion de IAM
fi

# 2. grupo de seguridad: todo el trafico solo entre miembros; SSH para Instance Connect
SG=$(aws_ ec2 describe-security-groups --filters Name=group-name,Values=dfsha-sg Name=vpc-id,Values=$VPC --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SG" = "None" ]; then
  SG=$(aws_ ec2 create-security-group --group-name dfsha-sg --description "DFSha cluster" --vpc-id $VPC --query GroupId --output text)
  aws_ ec2 authorize-security-group-ingress --group-id $SG --protocol all --source-group $SG >/dev/null
fi   # sin puertos abiertos a Internet: la administracion se hace por SSM
echo "SG=$SG"

# 3. zona DNS privada para el descubrimiento (RNF7)
ZONE=$(aws_ route53 list-hosted-zones-by-name --dns-name $ZONE_NAME --query "HostedZones[?Name=='${ZONE_NAME}.'].Id | [0]" --output text)
if [ "$ZONE" = "None" ]; then
  ZONE=$(aws_ route53 create-hosted-zone --name $ZONE_NAME --caller-reference "dfsha-$(date +%s)" \
    --vpc VPCRegion=$REGION,VPCId=$VPC --hosted-zone-config PrivateZone=true --query HostedZone.Id --output text)
fi
ZONE=${ZONE#/hostedzone/}
echo "ZONE=$ZONE"

launch() { # nombre rol tipo extra_env
  local name=$1 role=$2 type=$3 extra=${4:-}
  local ud
  ud=$(printf '#!/bin/bash\nexport ROLE=%s NODE_ID=%s BUCKET=%s %s\naws s3 cp s3://%s/node_bootstrap.sh /root/node_bootstrap.sh\nbash /root/node_bootstrap.sh > /var/log/dfsha-bootstrap.log 2>&1\n' \
    "$role" "$name" "$BUCKET" "$extra" "$BUCKET")
  aws_ ec2 run-instances --image-id $AMI --instance-type $type --subnet-id $SUBNET --security-group-ids $SG \
    --iam-instance-profile Name=$PROFILE \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=10,VolumeType=gp3,Encrypted=true}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$name},{Key=Project,Value=$PROJECT},{Key=Role,Value=$role}]" \
    --user-data "$ud" --query 'Instances[0].InstanceId' --output text
}

aws_ s3 cp deploy/node_bootstrap.sh "s3://$BUCKET/node_bootstrap.sh"
PEERS_ALL="ms-1.dfsha.internal:7001,ms-2.dfsha.internal:7001,ms-3.dfsha.internal:7001"
declare -A IDS
for n in 1 2 3; do
  peers=$(echo $PEERS_ALL | tr ',' '\n' | grep -v "^ms-$n\." | paste -sd,)
  IDS[ms-$n]=$(launch ms-$n meta $TYPE_META "RAFT_PEERS=$peers")
done
for n in 1 2 3 4; do IDS[dn-$n]=$(launch dn-$n data $TYPE_DATA); done
IDS[client]=$(launch client client $TYPE_CLIENT)
echo "instancias: ${IDS[*]}"
aws_ ec2 wait instance-running --instance-ids ${IDS[*]}

# 4. registros DNS: un nombre por nodo y el nombre logico del servicio de metadatos
changes=""
meta_ips=""
for name in "${!IDS[@]}"; do
  ip=$(aws_ ec2 describe-instances --instance-ids ${IDS[$name]} --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
  changes+="{\"Action\":\"UPSERT\",\"ResourceRecordSet\":{\"Name\":\"$name.$ZONE_NAME\",\"Type\":\"A\",\"TTL\":30,\"ResourceRecords\":[{\"Value\":\"$ip\"}]}},"
  [[ $name == ms-* ]] && meta_ips+="{\"Value\":\"$ip\"},"
done
changes+="{\"Action\":\"UPSERT\",\"ResourceRecordSet\":{\"Name\":\"meta.$ZONE_NAME\",\"Type\":\"A\",\"TTL\":30,\"ResourceRecords\":[${meta_ips%,}]}}"
aws_ route53 change-resource-record-sets --hosted-zone-id $ZONE --change-batch "{\"Changes\":[${changes}]}" --query ChangeInfo.Status --output text
echo "Despliegue lanzado. Las instancias tardan ~3-4 min en construir la imagen."
"$0" status
