#!/usr/bin/env bash
# Lista lo que está encendido en la cuenta del proyecto. Solo lectura: no crea ni borra nada.
# Uso: aws/scripts/vivos.sh
export AWS_PROFILE=flights
export AWS_DEFAULT_REGION=us-east-2
export AWS_PAGER=""

seccion() { printf '\n== %s ==\n' "$1"; }

seccion "Identidad"
ARN=$(aws sts get-caller-identity --query Arn --output text) || {
  echo "Sesión vencida. Renová con: aws login --profile flights"
  exit 1
}
echo "$ARN"
ACC=$(aws sts get-caller-identity --query Account --output text)

seccion "KMS (customer managed keys, ~USD 1/mes cada una)"
aws kms list-aliases --query "Aliases[?starts_with(AliasName, 'alias/flights')].[AliasName,TargetKeyId]" --output text

seccion "S3 (buckets flights-*: objetos y tamaño)"
for b in $(aws s3api list-buckets --query "Buckets[?starts_with(Name, 'flights-')].Name" --output text); do
  echo "$b"
  aws s3 ls "s3://$b" --recursive --summarize --human-readable | tail -2 | sed 's/^/   /'
done

seccion "Glue: jobs y crawlers (cobran por tiempo de ejecución)"
JOBS=$(aws glue list-jobs --query 'JobNames' --output text)
CRAWLERS=$(aws glue list-crawlers --query 'CrawlerNames' --output text)
echo "jobs:     ${JOBS:-ninguno}"
echo "crawlers: ${CRAWLERS:-ninguno}"
echo "corridas activas (RUNNING, STARTING o STOPPING):"
ACTIVAS=0
for j in $JOBS; do
  OUT=$(aws glue get-job-runs --job-name "$j" --max-results 20 \
    --query "JobRuns[?JobRunState=='RUNNING'||JobRunState=='STARTING'||JobRunState=='STOPPING'].[JobName,Id,JobRunState]" --output text)
  [ -n "$OUT" ] && { echo "   $OUT"; ACTIVAS=1; }
done
for c in $CRAWLERS; do
  OUT=$(aws glue get-crawler --name "$c" --query "Crawler.[Name,State]" --output text)
  case "$OUT" in *READY*) ;; *) echo "   crawler $OUT"; ACTIVAS=1 ;; esac
done
[ "$ACTIVAS" = 0 ] && echo "   ninguna"

seccion "Athena (workgroups)"
aws athena list-work-groups --query 'WorkGroups[].[Name,State]' --output text

seccion "SNS (topics)"
aws sns list-topics --query 'Topics[].TopicArn' --output text

seccion "Budgets"
aws budgets describe-budgets --account-id "$ACC" --query 'Budgets[].[BudgetName,BudgetLimit.Amount,BudgetLimit.Unit]' --output text

seccion "CloudWatch (alarmas)"
aws cloudwatch describe-alarms --query 'MetricAlarms[].[AlarmName,StateValue]' --output text

seccion "Step Functions (state machines)"
aws stepfunctions list-state-machines --query 'stateMachines[].name' --output text

seccion "EventBridge (reglas)"
aws events list-rules --query 'Rules[].[Name,State]' --output text

seccion "CloudTrail (trails: la primera copia de eventos de gestión es gratis)"
for t in $(aws cloudtrail describe-trails --query 'trailList[].TrailARN' --output text); do
  aws cloudtrail get-trail --name "$t" --query 'Trail.[Name,IsMultiRegionTrail,S3BucketName]' --output text
  echo "   registrando: $(aws cloudtrail get-trail-status --name "$t" --query IsLogging --output text)"
done

echo
echo "Fin. Lo que cobra por hora o por uso: keys de KMS, corridas de Glue activas y datos en S3."
