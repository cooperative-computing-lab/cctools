#!/bin/sh

set -ex

. ../../dttools/src/test_runner.common.sh

chirp_debug=chirp.debug
chirp_pid=chirp.pid
chirp_port=chirp.port
chirp_root=chirp.root
chirp_transient=chirp.transient

ticket=my.ticket

prepare()
{
    mkdir "$chirp_transient"

	../src/chirp_server -r "$chirp_root" -I 127.0.0.1 -Z "$chirp_port" -b -B "$chirp_pid" -d all -o "$chirp_debug" -y "$chirp_transient" --job-concurrency 2

	wait_for_file_creation "$chirp_port" 5
	wait_for_file_creation "$chirp_pid" 5
}

run()
{
	local host="localhost:$(cat "$chirp_port")"
	local json

	../src/chirp -a unix "$host" mkdir -p "/users/$(whoami)/data"
	../src/chirp -a unix "$host" mkdir -p "/users/$(whoami)/bin"
	../src/chirp -a unix "$host" put /dev/stdin "/users/$(whoami)/data/db.txt" <<EOF
a,b,c
d,e,f
EOF
	../src/chirp -a unix "$host" put /dev/stdin "/users/$(whoami)/data/conf.txt" <<EOF
A = 1
EOF
	../src/chirp -a unix "$host" put /dev/stdin "/users/$(whoami)/bin/script" <<EOF
#!/bin/sh

cat < db.txt > output
EOF

	json=$(cat <<EOF
{
	"executable": "/bin/bash",
	"arguments": [
		"bash",
		"-c",
		"(env && echo foo bar && pwd) > bar; cat < foo >> bar; sleep 1;",
	],
	"environment": {
		"HOME": ".",
	},
	"files": [
		{
			"serv_path": "/users/$(whoami)/bin/script",
			"task_path": "script",
			"type": "INPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/conf.txt",
			"task_path": "data/conf.txt",
			"type": "INPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo",
			"type": "INPUT",
			"binding": "LINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo_copy",
			"type": "INPUT",
			"binding": "COPY"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo_sym",
			"type": "INPUT",
			"binding": "SYMLINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/output.txt",
			"task_path": "bar",
			"type": "OUTPUT"
		},
	],
}
EOF
)
	J1=$(../src/chirp -a unix -d all "$host" job_create "$json")
	echo Job $J1 created.
	../src/chirp -a unix -d all "$host" job_commit "[$J1]"
	../src/chirp -a unix -d all "$host" job_commit "[$J1]" && return 1 # EPERM
	J1b=$(../src/chirp -a unix -d all "$host" job_create "$json")
	echo Job $J1b created.
	../src/chirp -a unix -d all "$host" job_kill "[$J1b]"
	J1c=$(../src/chirp -a unix -d all "$host" job_create "$json")
	echo Job $J1c created.
	../src/chirp -a unix -d all "$host" job_commit "[$J1c]"

	json=$(cat <<EOF
{
	"executable": "/bin/bash",
	"arguments": [
		"bash",
		"-c",
		"echo foo > barr; sleep 1; kill -TERM 0",
	],
	"environment": {
		"HOME": ".",
	},
	"files": [
		{
			"serv_path": "/users/$(whoami)/bin/script",
			"task_path": "script",
			"type": "INPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/conf.txt",
			"task_path": "data/conf.txt",
			"type": "INPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo",
			"type": "INPUT",
			"binding": "LINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo_copy",
			"type": "INPUT",
			"binding": "COPY"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo_sym",
			"type": "INPUT",
			"binding": "SYMLINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/output.txt",
			"task_path": "bar",
			"type": "OUTPUT"
		},
	],
}
EOF
)
	J2=$(../src/chirp -a unix -d all "$host" job_create "$json")
	echo Job $J2 created.
	../src/chirp -a unix -d all "$host" job_commit "[$J2]"

	echo Job status for $J1.
    ../src/chirp -a unix -d all "$host" job_status "[$J1]"
	echo Job status for $J1b.
    ../src/chirp -a unix -d all "$host" job_status "[$J1b]"
	echo Job status for $J1c.
    ../src/chirp -a unix -d all "$host" job_status "[$J1c]"
    echo Killing $J1c
    ../src/chirp -a unix -d all "$host" job_kill "[$J1c]"
	echo Job status for $J1c.
    ../src/chirp -a unix -d all "$host" job_status "[$J1c]"
	echo Job status for $J2.
    ../src/chirp -a unix -d all "$host" job_status "[$J2]"

	echo Waiting for jobs.
    ../src/chirp -a unix -d all "$host" job_wait $J1 2
    ../src/chirp -a unix -d all "$host" job_reap "[$J1]"
    ../src/chirp -a unix -d all "$host" job_wait 0 1
    ../src/chirp -a unix -d all "$host" job_reap "[$J1b,$J1c]"
    ../src/chirp -a unix -d all "$host" job_status "[$J1b]"
    ../src/chirp -a unix -d all "$host" job_status "[$J1c]"

	# An error due to ACL
	json=$(cat <<EOF
{
	"executable": "/bin/bash",
	"arguments": [
		"bash",
		"-c",
		"(env && echo foo bar && pwd) > bar; cat < foo >> bar; sleep 1;",
	],
	"environment": {
		"HOME": ".",
	},
	"files": [
		{
			"serv_path": "/users/$(whoami)/bin/script",
			"task_path": "script",
			"type": "INPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/conf.txt",
			"task_path": "data/conf.txt",
			"type": "INPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo",
			"type": "INPUT",
			"binding": "LINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo_copy",
			"type": "INPUT",
			"binding": "COPY"
		},
		{
			"serv_path": "/users/$(whoami)/data/db.txt",
			"task_path": "foo_sym",
			"type": "INPUT",
			"binding": "SYMLINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/output.txt",
			"task_path": "bar",
			"type": "OUTPUT"
		},
	],
}
EOF
)
	J3=$(../src/chirp -a hostname -d all "$host" job_create "$json")
	echo Job $J3 created.
	../src/chirp -a hostname -d all "$host" job_commit "[$J3]"
	../src/chirp -a hostname -d all "$host" job_wait $J3

	set +e
	return $?
}

clean()
{
	if [ -r "$chirp_pid" ]; then
		/bin/kill -9 `cat "$chirp_pid"`
	fi

	rm -rf "$chirp_debug" "$chirp_pid" "$chirp_port" "$chirp_root" "$ticket" "$chirp_transient"
}

dispatch $@

# vim: set noexpandtab tabstop=4:
