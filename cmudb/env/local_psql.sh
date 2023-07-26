#!/bin/bash

set -euxo pipefail

. /home/zhanghao/code/postgres/cmudb/env/env_var.sh

if ! PGPASSWORD=${POSTGRES_PASSWORD} "${BIN_DIR}"/psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -p ${POSTGRES_PORT} -c "SELECT 1" >/dev/null; then
  "${BIN_DIR}"/psql -c "create user ${POSTGRES_USER} with login password '${POSTGRES_PASSWORD}'" postgres -p ${POSTGRES_PORT}
  "${BIN_DIR}"/psql -c "create database ${POSTGRES_DB} with owner = '${POSTGRES_USER}'" postgres -p ${POSTGRES_PORT}
  "${BIN_DIR}"/psql -c "grant pg_monitor to ${POSTGRES_USER}" postgres -p ${POSTGRES_PORT}
  "${BIN_DIR}"/psql -c "alter user ${POSTGRES_USER} with superuser" postgres -p ${POSTGRES_PORT}

  PGPASSWORD=${POSTGRES_PASSWORD} "${BIN_DIR}"/psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -p ${POSTGRES_PORT} --echo-all -f ./cmudb/extensions/db721_fdw/chicken_farm_schema.sql
fi
PGPASSWORD=${POSTGRES_PASSWORD} "${BIN_DIR}"/psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -p ${POSTGRES_PORT}
