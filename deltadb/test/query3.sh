#!/bin/sh

# List the names of the file servers that
# were in operation on February 3rd, 2013.



DIR=${CCTOOLS}/bin
DATA=/var/tmp/catalog.history


${DIR}/ddb_collect ${DATA} 2013-03-14@00:00:00 d1 | \
${DIR}/ddb_select_static  type=chirp | \
${DIR}/ddb_project name | \
${DIR}/ddb_reduce_temporal d1 name,LAST | \
${DIR}/ddb_pivot name.LAST
