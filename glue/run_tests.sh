#!/usr/bin/env bash
# Corre las pruebas de los dos jobs de Glue (pytest) dentro de la imagen oficial de Glue 5.0,
# con el mismo Spark y Python que usa el job en AWS. Requiere Docker en marcha.
# La primera vez baja la imagen (~12 GB). No usa AWS ni cuesta nada.
# Uso:  bash glue/run_tests.sh              todas las pruebas
#       bash glue/run_tests.sh -k gold      solo las que contienen "gold" (los argumentos pasan a pytest)
set -euo pipefail
export MSYS_NO_PATHCONV=1  # Git Bash de Windows: que no convierta las rutas /usr/... del contenedor
cd "$(dirname "$0")/.."

IMAGEN=public.ecr.aws/glue/aws-glue-libs:5
RAIZ=$(pwd -W 2>/dev/null || pwd)  # Git Bash: ruta de Windows; Linux y macOS: pwd

# El PYTHONPATH apunta al Spark 3.5.4 de Glue; sin él, python3 importa el pyspark 3.5.2 de pip.
docker run --rm \
  -e PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-0.10.9.7-src.zip:/usr/share/aws/glue-pds/PyGlue.zip \
  -v "$RAIZ:/work" -w /work \
  --entrypoint python3 "$IMAGEN" -m pytest glue/tests -q -p no:cacheprovider "$@"
