#!/bin/bash
set -ex
export PATH="${PATH}:/usr/pgsql-${PGVERSION}/bin"
echo %pgversion "${PGVERSION}" >>~/.rpmmacros

git config --global --add safe.directory /host
cd /host
FDW_MAJOR=$(git describe --tags --abbrev=0 | grep -o '[0-9.]\+')
echo %fdw_version "${FDW_MAJOR}" >>~/.rpmmacros

rpmbuild -ba /host/db2_fdw.spec

cp ~/rpmbuild/RPMS/x86_64/postgresql*-db2_fdw-*.rpm /host/rpms/
