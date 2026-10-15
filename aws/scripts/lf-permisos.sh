#!/usr/bin/env bash
# Permisos de Lake Formation de la fase 3: dos analistas (roles de IAM) sobre la base flights_gold.
# Modo hibrido: solo estos dos roles pasan por Lake Formation; el resto (Glue, state machine, admin)
# sigue con IAM. Repetirlo es seguro: grant y opt-in se pueden volver a aplicar.
#
#   flights-analista-sin-matricula  fact_flights SIN tail_number (filtro por columna), agg y las 4 dimensiones
#   flights-analista-aerolinea      fact_flights solo de DL (filtro por fila) y las 4 dimensiones; NO agg
#                                   (agg_daily_airport junta todas las aerolineas y no se puede filtrar)
#
# Uso:  bash aws/scripts/lf-permisos.sh           aplica los permisos
#       bash aws/scripts/lf-permisos.sh revocar   los quita (los analistas pierden todo acceso)
set -euo pipefail
export AWS_PROFILE=flights AWS_DEFAULT_REGION=us-east-2 AWS_PAGER="" MSYS_NO_PATHCONV=1

ACC=$(aws sts get-caller-identity --query Account --output text)
DB=flights_gold
CARRIER=DL
DIMS=(dim_airport dim_carrier dim_cancellation dim_date)
TABLAS=(fact_flights agg_daily_airport "${DIMS[@]}")
FILTRO=solo_aerolinea_$CARRIER
SIN_MATRICULA="DataLakePrincipalIdentifier=arn:aws:iam::$ACC:role/flights-analista-sin-matricula"
AEROLINEA="DataLakePrincipalIdentifier=arn:aws:iam::$ACC:role/flights-analista-aerolinea"

tabla()   { echo "{\"Table\":{\"CatalogId\":\"$ACC\",\"DatabaseName\":\"$DB\",\"Name\":\"$1\"}}"; }
base()    { echo "{\"Database\":{\"CatalogId\":\"$ACC\",\"Name\":\"$DB\"}}"; }
filtro()  { echo "{\"DataCellsFilter\":{\"TableCatalogId\":\"$ACC\",\"DatabaseName\":\"$DB\",\"TableName\":\"fact_flights\",\"Name\":\"$FILTRO\"}}"; }
sin_tpl() { echo "{\"TableWithColumns\":{\"CatalogId\":\"$ACC\",\"DatabaseName\":\"$DB\",\"Name\":\"fact_flights\",\"ColumnWildcard\":{\"ExcludedColumnNames\":[\"tail_number\"]}}}"; }
todas()   { echo "{\"Table\":{\"CatalogId\":\"$ACC\",\"DatabaseName\":\"$DB\",\"TableWildcard\":{}}}"; }

if [ "${1:-}" = "revocar" ]; then
  echo "Revocando: opt-in, grants y filtro de los dos analistas"
  for p in "$SIN_MATRICULA" "$AEROLINEA"; do
    aws lakeformation delete-lake-formation-opt-in --principal "$p" --resource "$(todas)" || true
    aws lakeformation revoke-permissions --principal "$p" --permissions DESCRIBE --resource "$(base)" || true
  done
  aws lakeformation revoke-permissions --principal "$SIN_MATRICULA" --permissions SELECT --resource "$(sin_tpl)" || true
  for t in agg_daily_airport "${DIMS[@]}"; do
    aws lakeformation revoke-permissions --principal "$SIN_MATRICULA" --permissions SELECT --resource "$(tabla "$t")" || true
  done
  aws lakeformation revoke-permissions --principal "$AEROLINEA" --permissions SELECT --resource "$(filtro)" || true
  for t in "${DIMS[@]}"; do
    aws lakeformation revoke-permissions --principal "$AEROLINEA" --permissions SELECT --resource "$(tabla "$t")" || true
  done
  aws lakeformation delete-data-cells-filter --table-catalog-id "$ACC" --database-name "$DB" \
    --table-name fact_flights --name "$FILTRO" || true
  echo "Listo."
  exit 0
fi

echo "1/5 requisito del modo hibrido: IAM_ALLOWED_PRINCIPALS con ALL en cada tabla (el resto sigue con IAM)"
for t in "${TABLAS[@]}"; do
  aws lakeformation grant-permissions --principal DataLakePrincipalIdentifier=IAM_ALLOWED_PRINCIPALS \
    --permissions ALL --resource "$(tabla "$t")"
done

echo "2/5 ver la base: DESCRIBE para los dos"
for p in "$SIN_MATRICULA" "$AEROLINEA"; do
  aws lakeformation grant-permissions --principal "$p" --permissions DESCRIBE --resource "$(base)"
done

echo "3/5 analista sin matricula: SELECT con la columna tail_number excluida, y el resto de las tablas completas"
aws lakeformation grant-permissions --principal "$SIN_MATRICULA" --permissions SELECT --resource "$(sin_tpl)"
for t in agg_daily_airport "${DIMS[@]}"; do
  aws lakeformation grant-permissions --principal "$SIN_MATRICULA" --permissions SELECT --resource "$(tabla "$t")"
done

echo "4/5 analista de una aerolinea: filtro por fila carrier_code = '$CARRIER' y las dimensiones"
aws lakeformation create-data-cells-filter --table-data "{\"TableCatalogId\":\"$ACC\",\"DatabaseName\":\"$DB\",\"TableName\":\"fact_flights\",\"Name\":\"$FILTRO\",\"RowFilter\":{\"FilterExpression\":\"carrier_code = '$CARRIER'\"},\"ColumnWildcard\":{}}" \
  || echo "   (el filtro ya existia)"
aws lakeformation grant-permissions --principal "$AEROLINEA" --permissions SELECT --resource "$(filtro)"
for t in "${DIMS[@]}"; do
  aws lakeformation grant-permissions --principal "$AEROLINEA" --permissions SELECT --resource "$(tabla "$t")"
done

echo "5/5 opt-in: Lake Formation manda para los dos analistas sobre TODAS las tablas de $DB"
echo "    (lo que no se les dio explicitamente queda denegado, por ejemplo agg para el de aerolinea)"
for p in "$SIN_MATRICULA" "$AEROLINEA"; do
  aws lakeformation create-lake-formation-opt-in --principal "$p" --resource "$(todas)" \
    || echo "   (el opt-in ya existia)"
done
echo "Listo. Se revisa en la consola: Lake Formation > Permissions > Data permissions"
