#! /bin/sh

# Wrap tasks with an Apptainer container

# get the directory that contains the execution context from the location of this script
ctx_dir=$(dirname $( cd -- "$( dirname -- "$0" )" > /dev/null 2>&1 && pwd ))

# execute the command line with the container image "image.img"
exec apptainer exec --home "${VINE_SANDBOX:-${PWD}}" "${ctx_dir}/image.sif" "$@"
