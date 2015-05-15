#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ../../chirp/test/chirp-common.sh

c="./hostport.$PPID"
parrot_debug=parrot.debug

parrot() {
	../src/parrot_run --no-chirp-catalog --debug=all --debug-file="$parrot_debug" --work-dir="/chirp/$hostport/" "$@"
}

prepare()
{
	chirp_start local
	echo "$hostport" > "$c"

	return 0
}

run()
{
	hostport=$(cat "$c")

	parrot /bin/sh <<EOF1
cat > a.py <<EOF2
#!/chirp/$hostport/python

import sys

print(' '.join(sys.argv))
EOF2
cp "$(which python)" "$(which sh)" .
chmod 700 a.py python sh
EOF1
	for loader in `find -L /lib /lib64 -name 'ld-linux*.so*' 2>/dev/null`; do
		[ "$(parrot --ld-path="$loader" -- ./a.py 1 2 | tee /dev/tty)" = './a.py 1 2' ]
		[ "$(parrot --ld-path="$loader" -- ./sh -c 'echo "$0"' | tee /dev/tty)" = './sh' ]
		return 0
	done
	echo No loader found!
	return 1
}

clean()
{
	chirp_clean
	rm -f "$c" "$parrot_debug"
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
