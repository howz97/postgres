#!/bin/bash

set -euxo pipefail

. /home/zhanghao/code/howz97/postgres/cmudb/env/env_var.sh

# ${BIN_DIR}/postgres -D ${BIN_DIR}/pgdata -p ${POSTGRES_PORT} 
${BIN_DIR}/pg_ctl --core-files -D ${BIN_DIR}/pgdata -o "-p ${POSTGRES_PORT}" start

