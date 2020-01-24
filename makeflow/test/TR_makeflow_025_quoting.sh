#!/bin/sh

. ../../dttools/test/test_runner_common.sh

test_dir=`basename $0 .sh`.dir

prepare()
{
	mkdir $test_dir
	cd $test_dir
	ln -sf ../../src/makeflow .
	ln -sf ../syntax/quotes.01.makeflow

for letter in A B C D E F G 
do
	/bin/echo ${letter} > ${letter}.expected
done

cat > C\ D.expected <<'EOF'
C D 'C D' "C D"
EOF

cat > H\ I.expected <<'EOF'
H I
EOF

cat > J\ K.expected <<'EOF'
J K
EOF

	exit 0
}

run()
{
	cd $test_dir
	./makeflow -j 1 quotes.01.makeflow

	if [ $? -eq 0 ]; then
		for file in A B C D E F G "C D" "H I" "J K"
		do
			diff -w "${file}".expected "${file}" || exit 1
		done
	fi

	exit 0
}

clean()
{
	rm -fr $test_dir
	exit 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
