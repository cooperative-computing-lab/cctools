#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid

prepare()
{
	ln -s ..//.//.///src/
    ../src/chirp_server -p 9095 &
    pid=$!
    
    echo $pid > $PID_FILE
    exit 0
}

run()
{
    exec ../src/chirp localhost:9095 <<EOF
help
df -g
ls src
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
    kill -9 $(cat $PID_FILE)
    rm -f $TEST_FILE
    rm -f $PID_FILE
    rm -f .__acl
    rm -fr _test
    rm -f src
    exit 0
}

dispatch $@
