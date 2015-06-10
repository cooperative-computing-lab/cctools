#!/bin/sh

# List the names of the file servers that
# were in operation on February 3rd, 2013.



DIR=${CCTOOLS}/bin
DATA=/var/tmp/catalog.history


${DIR}/deltadb_collect ${DATA} 2013-03-14@00:00:00 d1 | \
${DIR}/deltadb_select_static  type=chirp | \
${DIR}/deltadb_project name | \
${DIR}/deltadb_reduce_temporal d1 name,LAST | \
${DIR}/deltadb_pivot name.LAST

# vim: set noexpandtab tabstop=4:
