#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
	mkdir $test_dir
	cd $test_dir
	ln -sf ../../src/makeflow .
	ln -sf ../syntax/quotes.01.makeflow
cat > A.expected <<'EOF'
A
EOF

cat > B.expected <<'EOF'
B
EOF

cat > A\ B.expected <<'EOF'
A B 'A B' $(FILES)
EOF
	exit 0
}

run()
{
	cd $test_dir
	./makeflow -j 1 quotes.01.makeflow

	if [ $? -eq 0 ]; then
		diff -w A.expected A  || exit 1
		diff -w B.expected B  || exit 1
		diff -w A\ B.expected A\ B || exit 1
	else
		exit 1
	fi
}

clean()
{
	rm -fr $test_dir
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
