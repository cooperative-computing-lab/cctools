#!/bin/sh

set -ex

set -e

. ../../dttools/test/test_runner_common.sh
. ./chirp-common.sh

c="./hostport.$PPID"
cr="./root.$PPID"

chirp() {
	../src/chirp -a unix "$@"
}

prepare()
{
	chirp_start local --auth=hostname --jobs --job-concurrency=2
	echo "$hostport" > "$c"
	echo "$root" > "$cr"
	return 0
}

run()
{
	hostport=$(cat "$c")
	root=$(cat "$cr")
	local json

	chirp -a unix "$hostport" mkdir -p "/users/$(whoami)/data"
	chirp -a unix "$hostport" setacl "/users/$(whoami)/data" 'hostname:*' 'lr'
	chirp -a unix "$hostport" mkdir -p "/users/$(whoami)/bin"
	chirp -a unix "$hostport" put /dev/stdin "/users/$(whoami)/data/db.txt" <<EOF
a,b,c
d,e,f
EOF
	chirp -a unix "$hostport" put /dev/stdin "/users/$(whoami)/data/conf.txt" <<EOF
A = 1
EOF
	chirp -a unix "$hostport" put /dev/stdin "/users/$(whoami)/bin/script" <<EOF
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
			"binding": "LINK"
		},
		{
			"serv_path": "/users/$(whoami)/data/output.txt",
			"task_path": "bar",
			"type": "OUTPUT"
		},
		{
			"serv_path": "/users/$(whoami)/data/chirp.debug",
			"task_path": ".chirp.debug",
			"type": "OUTPUT"
		}
		,{
			"serv_path": "chirp:\\/\\/${hostport}\\/users\\/$(whoami)\\/data\\/db.txt",
			"task_path": "foo_url",
			"type": "INPUT",
			"binding": "URL"
		}
	],
}
EOF
)
	J1=$(chirp -a unix -d all "$hostport" job_create "$json")
	echo Job $J1 created.
	chirp -a unix -d all "$hostport" job_commit "[$J1]"
	chirp -a unix -d all "$hostport" job_commit "[$J1]" || return 1 # harmless NOP
	J1b=$(chirp -a unix -d all "$hostport" job_create "$json")
	echo Job $J1b created.
	chirp -a unix -d all "$hostport" job_kill "[$J1b]"
	J1c=$(chirp -a unix -d all "$hostport" job_create "$json")
	echo Job $J1c created.
	chirp -a unix -d all "$hostport" job_commit "[$J1c]"
	echo Killing $J1c
	chirp -a unix -d all "$hostport" job_kill "[$J1c]"

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
			"binding": "LINK"
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
	J2=$(chirp -a unix -d all "$hostport" job_create "$json")
	echo Job $J2 created.
	chirp -a unix -d all "$hostport" job_commit "[$J2]"

	echo Job status for $J1.
	chirp -a unix -d all "$hostport" job_status "[$J1]"
	echo Job status for $J1b.
	chirp -a unix -d all "$hostport" job_status "[$J1b]"
	echo Job status for $J1c.
	chirp -a unix -d all "$hostport" job_status "[$J1c]"
	echo Job status for $J2.
	chirp -a unix -d all "$hostport" job_status "[$J2]"

	echo Waiting for jobs.
	chirp -a unix -d all "$hostport" job_wait $J1 2
	chirp -a unix -d all "$hostport" job_reap "[$J1]"
	chirp -a unix -d all "$hostport" job_wait 0 1
	chirp -a unix -d all "$hostport" job_reap "[$J1b,$J1c]"
	chirp -a unix -d all "$hostport" job_status "[$J1b]"
	chirp -a unix -d all "$hostport" job_status "[$J1c]"

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
			"binding": "LINK"
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
	J3=$(chirp -a hostname -d all "$hostport" job_create "$json")
	echo Job $J3 created.
	chirp -a hostname -d all "$hostport" job_commit "[$J3]"
	chirp -a hostname -d all "$hostport" job_wait $J3

	return 0
}

clean()
{
	chirp_clean
	rm -rf "$c" "$cr"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
