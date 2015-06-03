#!/bin/sh

set -e

. ../../dttools/test/test_runner_common.sh
. ../../chirp/test/chirp-common.sh

c="./hostport.$PPID"
parrot_debug=parrot.debug
exe="hello-static"

parrot() {
	../src/parrot_run --no-chirp-catalog --debug=all --debug-file="$parrot_debug" --timeout=5 --work-dir="/chirp/$hostport/" "$@"
}

prepare()
{
	chirp_start local
	echo "$hostport" > "$c"

	set +e
	# -static requires "libc-devel" which is missing on some platforms
	gcc -static -I../src/ -g -o "$exe" -x c - -x none <<EOF
#include <stdio.h>
int main (int argc, char *argv[])
{
	printf("Hello, world!\\n");
	return 0;
}
EOF
	set -e

	return 0
}

run()
{
	hostport=$(cat "$c")

	parrot /bin/sh <<EOF1
mkdir bin
cat > bin/a.py <<EOF2
#!/chirp/$hostport/bin/python

import sys

print(' '.join(sys.argv))
EOF2
cp /usr/bin/python /bin/sh bin/
chmod 700 bin/a.py bin/python bin/sh
EOF1
	if [ -x "$exe" ]; then
		[ "$(parrot -- "$(pwd)/$exe" | tee -a /dev/stderr)" = 'Hello, world!' ]
	fi
	for loader in `find -L /lib64 /lib -name 'ld-linux*.so*' 2>/dev/null`; do
		[ "$(parrot --ld-path="$loader" -- ./bin/a.py 1 2 | tee -a /dev/stderr)" = './bin/a.py 1 2' ]
		[ "$(parrot --ld-path="$loader" -- ./bin/sh -c 'echo "$0"' | tee -a /dev/stderr)" = './bin/sh' ]
		if [ -x "$exe" ]; then
			[ "$(parrot --ld-path="$loader" -- "$(pwd)/$exe" | tee -a /dev/stderr)" = 'Hello, world!' ]
		fi
		return 0
	done
	echo No loader found!
	return 1
}

clean()
{
	chirp_clean
	rm -f "$c" "$parrot_debug" "$exe"
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
