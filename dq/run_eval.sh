#!/usr/bin/env bash
# Evalúa el ruleset flights-silver-dq sobre UN mes de silver y muestra el resultado de cada regla.
# Uso (desde la raíz del repo):  dq/run_eval.sh 2024 1
# Código de salida: 0 si pasan todas las reglas, 2 si falla alguna, 1 si la evaluación misma falló.
set -u
export AWS_PROFILE=flights AWS_DEFAULT_REGION=us-east-2 AWS_PAGER=""

[ $# -eq 2 ] || { echo "uso: $0 AAAA M   (por ejemplo: $0 2024 1)"; exit 1; }
Y=$1
M=$(printf '%02d' "$2")

# pushDownPredicate: evalúa solo la partición del mes, sin leer todo silver.
SRC=$(printf '{"GlueTable":{"DatabaseName":"flights_silver","TableName":"flights","AdditionalOptions":{"pushDownPredicate":"year = \x27%s\x27 and month = \x27%s\x27"}}}' "$Y" "$M")

# 2 workers y 10 minutos de tope: por defecto serían 5 workers y 48 horas.
RUN=$(aws glue start-data-quality-ruleset-evaluation-run \
  --data-source "$SRC" --role AWSGlueServiceRole-flights \
  --number-of-workers 2 --timeout 10 \
  --ruleset-names flights-silver-dq --query RunId --output text) || exit 1
echo "evaluación de $Y-$M: $RUN"

while true; do
  S=$(aws glue get-data-quality-ruleset-evaluation-run --run-id "$RUN" --query Status --output text)
  echo "$(date +%H:%M:%S)  $S"
  case "$S" in SUCCEEDED) break ;; FAILED|STOPPED|TIMEOUT) echo "la evaluación no terminó bien"; exit 1 ;; esac
  sleep 10
done

# Ojo: SUCCEEDED quiere decir que la evaluación CORRIÓ, no que las reglas pasaron.
RID=$(aws glue get-data-quality-ruleset-evaluation-run --run-id "$RUN" --query 'ResultIds[0]' --output text)
aws glue get-data-quality-result --result-id "$RID" --query 'RuleResults[].[Result,Description]' --output text
SCORE=$(aws glue get-data-quality-result --result-id "$RID" --query Score --output text)
FAILS=$(aws glue get-data-quality-result --result-id "$RID" --query 'length(RuleResults[?Result!=`PASS`])' --output text)
echo "puntaje: $SCORE | reglas que no pasaron: $FAILS"
[ "$FAILS" = "0" ] && exit 0 || exit 2
