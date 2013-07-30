#!/bin/sh

. ../../dttools/src/test_runner.common.sh


chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
parrot_debug=parrot.debug

expected=expected.txt
output=output.txt

psearch="../src/parrot_run -d all -o $parrot_debug ../src/parrot_search"

prepare()
{
	../../chirp/src/chirp_server -r ./fixtures -I 127.0.0.1 -Z "$chirp_port" -b -B "$chirp_pid" -d all -o "$chirp_debug"

	cat > "$expected" <<EOF
++
/a/b/bar
/a/b/c/bar
++
no results
++
/a/b/c/bar
++
no results
++
/a/b/bar
++
no results
++
/a/b/foo
++
/a/b/foo
++
no results
++
/a/b/foo
++
/a/b/bar
/a/b/c/bar
++
EOF

	wait_for_file_creation "$chirp_port" 5
	wait_for_file_creation "$chirp_pid" 5
}

run()
{
	{
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ 'bar' | sort
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '/bar'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ 'c/bar'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '/c/bar'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ 'b/bar'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '/b/bar'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ 'b/foo'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '/a/b/foo'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '/*/foo'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '/*/*/foo'
		echo ++
		$psearch /chirp/localhost:`cat $chirp_port`/ '*/*r' | sort
		echo ++
	} > "$output"

	diff --ignore-all-space "$expected" "$output"
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill -9 `cat $chirp_pid`
	fi
	rm -f "$chirp_debug" "$chirp_pid" "$chirp_port" "$parrot_debug" "$expected" "$output"
}

dispatch $@
