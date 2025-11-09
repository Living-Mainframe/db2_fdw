#!/bin/bash
set -ex

if [ ! -e "$PGBIN" ]; then
	PGBINS=(/usr/pgsql-*/bin)
	PGBIN=${PGBINS[0]}
fi
export PATH=$PGBIN:$PATH

export PGDATA=/pgdata/pgdata
mkdir -p "$PGDATA"

if [ ! -e "$PGDATA/PG_VERSION" ]; then
	initdb
fi

postgres
