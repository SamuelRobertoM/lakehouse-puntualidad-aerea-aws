#!/usr/bin/env bash
# Demo para el video: la MISMA consulta con tres identidades (el admin y los dos analistas de Lake Formation).
# Solo hace SELECT en Athena (centavos). Cada identidad usa su propio workgroup, con resultados aislados.
# Uso:  bash aws/scripts/demo-analistas.sh            todo seguido
#       bash aws/scripts/demo-analistas.sh --pausa    espera Enter entre escenas (para filmar)
set -uo pipefail
export AWS_PAGER="" AWS_DEFAULT_REGION=us-east-2 MSYS_NO_PATHCONV=1
PAUSA=0; [ "${1:-}" = "--pausa" ] && PAUSA=1

# perfil | workgroup | nombre que se muestra
IDENTIDADES=(
  "flights|flights-wg|ADMIN  (sin Lake Formation: ve todo)"
  "flights-analista-sin-matricula|flights-wg-sin-matricula|ANALISTA 1  (sin la columna matricula)"
  "flights-analista-aerolinea|flights-wg-aerolinea|ANALISTA 2  (solo la aerolinea DL)"
)
B=$'\033[1m'; C=$'\033[1;36m'; V=$'\033[32m'; R=$'\033[1;31m'; G=$'\033[90m'; N=$'\033[0m'

# consulta PERFIL WORKGROUP SQL MODO(filas|columnas) -> imprime el resultado o el error de Athena
consulta() {
  local q s
  q=$(aws athena start-query-execution --profile "$1" --work-group "$2" --query-string "$3" \
        --query QueryExecutionId --output text 2>&1) || { echo "   ${R}NO ARRANCO${N} $(echo "$q" | grep -E 'Unable|Code' | head -2 | cut -c1-200)"; return; }
  for _ in $(seq 1 40); do
    s=$(aws athena get-query-execution --profile "$1" --query-execution-id "$q" --query QueryExecution.Status.State --output text)
    [ "$s" = SUCCEEDED ] || [ "$s" = FAILED ] || [ "$s" = CANCELLED ] && break; sleep 2
  done
  if [ "$s" != SUCCEEDED ]; then
    echo "   ${R}ERROR${N} $(aws athena get-query-execution --profile "$1" --query-execution-id "$q" \
         --query QueryExecution.Status.StateChangeReason --output text | cut -c1-170)"; return
  fi
  aws athena get-query-results --profile "$1" --query-execution-id "$q" --output json | MODO="$4" VERDE="$V" GRIS="$G" NORMAL="$N" python -I -c "
import sys, json, os
filas = [[c.get('VarCharValue', '') for c in r['Data']] for r in json.load(sys.stdin)['ResultSet']['Rows']]
cab, cuerpo = filas[0], filas[1:]
v, g, n = os.environ['VERDE'], os.environ['GRIS'], os.environ['NORMAL']
if os.environ['MODO'] == 'columnas':
    print(f'   {v}OK{n}  {len(cab)} columnas | matricula (tail_number): ' + ('SI' if 'tail_number' in cab else 'NO'))
else:
    print(f'   {v}OK{n}  {len(cuerpo)} fila(s)')
    for r in cuerpo[:5]: print('       ' + '   '.join(r))
    if len(cuerpo) > 5: print(f'       {g}... y {len(cuerpo) - 5} mas{n}')
"
}

escena() { # TITULO SQL MODO
  echo; echo "${C}============================================================${N}"
  echo "${C}$1${N}"; echo "${G}$2${N}"; echo "${C}============================================================${N}"
  for i in "${IDENTIDADES[@]}"; do
    IFS='|' read -r perfil wg nombre <<< "$i"
    echo "${B}$nombre${N}"
    consulta "$perfil" "$wg" "$2" "$3"
  done
  [ "$PAUSA" = 1 ] && read -r -p "   (Enter para la siguiente escena) " _
}

echo "${B}Lake Formation: la misma consulta, tres identidades${N}"
for i in "${IDENTIDADES[@]}"; do
  IFS='|' read -r perfil _ nombre <<< "$i"
  printf '  %-45s %s\n' "$nombre" "$(aws sts get-caller-identity --profile "$perfil" --query Arn --output text | sed -E 's#arn:aws:sts::[0-9]+:assumed-role/##; s#/[^/]*$##')"
done

escena "Escena 1 | Vuelos por aerolinea en enero (el filtro por FILA)" \
  "SELECT carrier_code, COUNT(*) AS vuelos FROM flights_gold.fact_flights WHERE year='2024' AND month='01' GROUP BY carrier_code ORDER BY vuelos DESC" filas
escena "Escena 2 | Pido las matriculas (el filtro por COLUMNA)" \
  "SELECT tail_number FROM flights_gold.fact_flights LIMIT 3" filas
escena "Escena 3 | SELECT * : cuantas columnas ve cada uno" \
  "SELECT * FROM flights_gold.fact_flights LIMIT 1" columnas
escena "Escena 4 | Una tabla que no le dieron al analista 2 (agregado de TODAS las aerolineas)" \
  "SELECT date_key, airport_code, flights FROM flights_gold.agg_daily_airport ORDER BY date_key, airport_code LIMIT 2" filas

# Escena 5: se saltan Athena y Lake Formation e intentan leer el Parquet directo de S3 (lo frena IAM, no Lake Formation)
ACC=$(aws sts get-caller-identity --profile flights --query Account --output text)
GOLD="flights-gold-$ACC-us-east-2-an"
CLAVE=$(aws s3api list-objects-v2 --profile flights --bucket "$GOLD" --prefix fact_flights/year=2024/month=01/ \
          --query 'Contents[0].Key' --output text)
echo; echo "${C}============================================================${N}"
echo "${C}Escena 5 | Saltarse Athena: leer el Parquet de gold directo de S3${N}"
echo "${G}aws s3api get-object --bucket $GOLD --key fact_flights/.../part-00000-....parquet${N}"
echo "${C}============================================================${N}"
for i in "${IDENTIDADES[@]}"; do
  IFS='|' read -r perfil _ nombre <<< "$i"
  echo "${B}$nombre${N}"
  tmp=$(cygpath -m "$(mktemp)")  # ruta de Windows: aws.exe no entiende /tmp
  if aws s3api get-object --profile "$perfil" --bucket "$GOLD" --key "$CLAVE" --range bytes=0-99 "$tmp" >/dev/null 2>"$tmp.err"; then
    echo "   ${V}OK${N}  leyo 100 bytes del archivo ($(head -c 4 "$tmp"))"
  elif grep -q AccessDenied "$tmp.err"; then
    echo "   ${R}ACCESO DENEGADO${N} $(grep -o 'not authorized to perform: [A-Za-z0-9:]*' "$tmp.err" | head -1)"
  else
    echo "   ${R}ERROR${N} $(head -c 200 "$tmp.err")"
  fi
  rm -f "$tmp" "$tmp.err"
done
echo
