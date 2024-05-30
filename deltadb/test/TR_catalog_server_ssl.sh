#!/bin/sh

. ../../dttools/test/test_runner_common.sh

check_needed()
{
	avail=`grep CCTOOLS_OPENSSL_AVAILABLE ../../config.mk | cut -f2 -d=`
	if [ $avail = no ] 
	then
		return 1
	fi
}

prepare()
{
	echo "creating a temporary certificate"
	openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes -subj "/C=US/ST=Indiana/L=Notre Dame/O=Testing/OU=Testing/CN=localhost/emailAddress=nobody@nowhere.com"

	echo "creating update.json"
	echo '{"type":"cctools-test","size":1048576,"enabled":true}' > update.json
}

run()
{
	echo "starting the catalog server"
	../src/catalog_server -d all -o catalog.log --port-file catalog.port --port 9197 --ssl-port-file catalog.ssl.port --ssl-port 9199 --ssl-cert cert.pem --ssl-key key.pem &
	pid=$!

	echo "waiting for catalog server to start"
	wait_for_file_creation catalog.port 5
	wait_for_file_creation catalog.ssl.port 5

	port=`cat catalog.port`
	ssl_port=`cat catalog.ssl.port`
	
	echo "sending updates to the server"
	for i in 1 2 3 4 5
	do
		../../dttools/src/catalog_update --catalog localhost:$port --file update.json
		sleep 1
	done

	# note the url contains the base 64 encoding of a jx expression:
	# echo 'type=="cctools-test"' | base64

	echo "fetching the results via https"
	curl --insecure https://localhost:${ssl_port}/query/dHlwZT09ImNjdG9vbHMtdGVzdCIK > query.out

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
	kill $pid
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
	rm -f cert.pem key.pem catalog.log catalog.port catalog.ssl.port update.json query.out
	rm -rf catalog.history
	return 0
}

dispatch "$@"
