#!/bin/sh

. ../../dttools/test/test_runner_common.sh

prepare()
{
cat << EOF > jx_env.input
{
    "hello":"goodbye",
    "plate": { "fork" : { "knife" : { "spoon" : "tea spoon" } } },
	"tag"  : "it"
}
EOF

	return 0
}

run()
{
	eval $(../src/jx2env jx_env.input varname=hello othervarname=plate.fork.knife.spoon)

	[ "${varname}" = "goodbye" ] || exit 1
	[ "${othervarname}" = "tea spoon" ] || exit 1
	[ -z ${tag} ] || exit 1
}

clean()
{
	rm -f jx_env.input
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
