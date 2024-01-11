#!/bin/bash

if [ X$CCTOOLS_SOURCE_PROFILE = Xyes ]
then
    source ~/.bash_profile
fi

# Leave out some items that are research prototypes.
DISABLED_SYS=$(echo --without-system-{parrot,prune,umbrella,weaver})
DISABLED_LIB=$(echo --with-{readline,fuse,perl}-path\ no)

# Now configure in the normal way.
conda run --name cctools-build ./configure --strict ${DISABLED_SYS} ${DISABLED_LIB} "$@"
[[ -f config.mk ]] && make clean
echo === Contents of config.mk ===
cat config.mk
