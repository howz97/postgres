#!/bin/bash

set -euxo pipefail

. /home/zhanghao/code/howz97/postgres/cmudb/env/env_var.sh

PGPASSWORD=${POSTGRES_PASSWORD} ${BIN_DIR}/psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -p ${POSTGRES_PORT}
