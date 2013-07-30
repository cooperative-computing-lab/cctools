#!/bin/sh

. ../../dttools/src/test_runner.common.sh

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
chirp_root=chirp.root

expected=expected.txt
output=output.txt

prepare()
{
	cat > "$expected" <<EOF
++
/a/bar
/b/bar
++
1
++
/a/bar
++
/foo/a/bar
++
++
/foo/a/bar
++
/foo/a/bar
/foo/b/bar
++
/a/bar
/b/bar
++
/a/bar
/b/bar
++
/foo/a/bar
/foo/b/bar
++
/foo/a
/foo/a/bar
/foo/b
/foo/b/bar
++
/foo/b/bar
++
EOF

	../src/chirp_server -r "$chirp_root" -I 127.0.0.1 -Z "$chirp_port" -b -B "$chirp_pid" -d all -o "$chirp_debug"

	wait_for_file_creation "$chirp_port" 5
	wait_for_file_creation "$chirp_pid" 5
}

run()
{
	set -e
	../src/chirp localhost:`cat "$chirp_port"` mkdir foo
	../src/chirp localhost:`cat "$chirp_port"` mkdir foo/a
	../src/chirp localhost:`cat "$chirp_port"` mkdir foo/a/bar
	../src/chirp localhost:`cat "$chirp_port"` mkdir foo/b
	../src/chirp localhost:`cat "$chirp_port"` mkdir foo/b/bar

	{
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     'foo' 'bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search -s  'foo' 'bar' | wc -l
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     'foo' 'a/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search -i  'foo' 'a/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     'foo' '/foo/a/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     '/' '/foo/a/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     '/' '/foo/*/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     '/foo' '/*/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search     '/foo' '*/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search -i  '/foo' '*/bar' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search -i  '/foo' '*' | sort
		echo ++
		../src/chirp localhost:`cat "$chirp_port"` search -i  '/' '/foo/b/bar' | sort
		echo ++
	} | tr -d ' ' > "$output"
	diff "$expected" "$output"
	set +e
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill -9 `cat "$chirp_pid"`
	fi

	rm -rf "$chirp_debug" "$chirp_pid" "$chirp_port" "$chirp_root" "$expected" "$output"
}

dispatch $@
