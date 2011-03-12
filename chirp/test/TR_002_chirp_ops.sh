#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid
PORT_FILE=chirp_server.port

prepare()
{
    mkdir foo
    ln -s ..//.//./..///foo/ foo/foo
    port=`find_free_port`
    ../src/chirp_server -r $PWD/foo -p $port &
    echo $! > $PID_FILE
    echo $port > $PORT_FILE
    exit 0
}

run()
{
    port=`cat $PORT_FILE`
    exec ../src/chirp localhost:$port <<EOF
help
df -g
mkdir bar
mv foo bar/foo
ls bar/foo
audit -r
whoami
whoareyou localhost:$port
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
    rm -rf foo _test .__acl $PID_FILE $PORT_FILE $TEST_FILE
    exit 0
}

dispatch $@
