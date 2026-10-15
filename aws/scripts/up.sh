#!/usr/bin/env bash
# Despliega (o actualiza) el stack flights-orquestacion: state machine, EventBridge, logs y alarma.
# Funciona desde cualquier carpeta. Repetirlo es seguro: CloudFormation aplica solo la diferencia.
# Uso:  bash aws/scripts/up.sh
set -euo pipefail
export AWS_PROFILE=flights AWS_DEFAULT_REGION=us-east-2 AWS_PAGER=""
cd "$(dirname "$0")/../.."

ACC=$(aws sts get-caller-identity --query Account --output text)
WORK="flights-work-${ACC}-us-east-2-an"

echo "1/2 empaquetando: sube statemachine/pipeline.asl.json a s3://$WORK/cfn/"
aws cloudformation package \
  --template-file infra/orquestacion.yaml \
  --s3-bucket "$WORK" --s3-prefix cfn \
  --output-template-file infra/orquestacion.packaged.yaml

echo "2/2 desplegando el stack flights-orquestacion"
aws cloudformation deploy \
  --template-file infra/orquestacion.packaged.yaml \
  --stack-name flights-orquestacion \
  --capabilities CAPABILITY_IAM \
  --tags Project=flights \
  --no-fail-on-empty-changeset

aws cloudformation describe-stacks --stack-name flights-orquestacion \
  --query 'Stacks[0].{Estado:StackStatus,Salidas:Outputs[].[OutputKey,OutputValue]}' --output json
