#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
	echo "creating update.json"
	echo '{"type":"cctools-test","size":1048576,"enabled":true}' > update.json
}

run()
{
	echo "starting the catalog server"
	../src/catalog_server -d all -o catalog.log &
	pid=$!

	echo "sending udp updates to the server"
	for i in 1 2 3 4 5
	do
		../../dttools/src/catalog_update --catalog localhost:9097 --file update.json -d all
		sleep 1
	done

	# note the url contains the base 64 encoding of a jx expression:
	# echo 'type=="cctools-test"' | base64

	echo "fetching the results via http"
	curl http://localhost:9097/query/dHlwZT09ImNjdG9vbHMtdGVzdCIK > query.out

	if ../../dttools/src/jx2json < query.out
	then
		echo "output is valid json"
		result=0
	else
		echo "output is not valid json:"
		cat query.out
		echo "========================="
		result=1
	fi

	echo "killing the catalog server"
	kill -9 $pid
	wait $pid
	
	if [ $result != 0 ]
	then
		echo "contents of catalog.log:"
		cat catalog.log
	fi

	return $result
}

clean()
{
	rm -f cert.pem key.pem catalog.log update.json query.out
	rm -rf catalog.history
	return 0
}

dispatch "$@"
