#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid

prepare()
{
    mkdir foo
    ln -s ..//.//./..///foo/ foo
    ../src/chirp_server -r $PWD/foo -p 9095 &
    echo "$!" > $PID_FILE
    exit 0
}

run()
{
    exec ../src/chirp localhost:9095 <<EOF
help
df -g
mkdir bar
mv foo bar/foo
ls bar/foo
audit -r
whoami
whoareyou localhost:9095
ls
mkdir _test
ls
cd _test
ls
put /etc/hosts hosts.txt
cat hosts.txt
getacl
localpath hosts.txt
exit
EOF
}

clean()
{
    kill -9 `cat $PID_FILE`
    rm -rf foo _test .__acl $PID_FILE $TEST_FILE
    exit 0
}

dispatch $@
