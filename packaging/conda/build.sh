
DISABLED_SYS=$(echo --without-system-{allpairs,parrot,prune,sand,umbrella,wavefront,weaver})
DISABLED_LIB=$(echo --with-{readline,fuse}-path\ no)


if [[ "$PY3K" == 1 ]]; then
    PYTHON_OPT="--with-python3-path"
else
    PYTHON_OPT="--with-python-path"
fi

if [[ "$(uname)" == "Darwin" ]]; then
    PERL_PATH="no"
else
    PERL_PATH="${PREFIX}"
fi

./configure --prefix "${PREFIX}" --with-base-dir "${PREFIX}" ${PYTHON_OPT} "${PREFIX}" --with-perl-path "${PERL_PATH}" ${DISABLED_LIB} ${DISABLED_SYS}

make -j${CPU_COUNT}
make install

if ! make test
then
    cat cctools.test.fail
    exit 1
else
    exit 0
fi

