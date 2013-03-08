#!/bin/sh

. ../../dttools/src/test_runner.common.sh

TEST_FILE=chirp_benchmark.tmp
PID_FILE=chirp_server.pid
PORT_FILE=chirp_server.port

prepare()
{
	rm -rf foo bar

	rm -f $PORT_FILE	
	../src/chirp_server -Z $PORT_FILE &
	pid=$!
	echo $pid > $PID_FILE

	for i in 1 2 3 4 5
	do
		if [ -f $PORT_FILE ]
		then
			exit 0
		else
			sleep 1
		fi
	done

	exit 1
}

run()
{
    port=`cat $PORT_FILE`
    exec ../src/chirp localhost:$port <<EOF
help
df -g
mkdir foo
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
md5 hosts.txt
getacl
localpath hosts.txt
exit
EOF
}

clean()
{
	if [ -f $PID_FILE ]
	then
		kill -9 `cat $PID_FILE`
	fi

    rm -rf foo _test .__acl $PID_FILE $PORT_FILE $TEST_FILE
    exit 0
}

dispatch $@
