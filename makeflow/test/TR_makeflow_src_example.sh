#!/bin/sh

. ../../dttools/src/test_runner.common.sh

CONVERT=`which convert`

if [ -z "$CONVERT" ]; then
	if   [ -f /usr/bin/convert ]; then
		CONVERT=/usr/bin/convert
	elif [ -f /usr/local/bin/convert ]; then
		CONVERT=/usr/local/bin/convert
	elif [ -f /opt/local/bin/convert ]; then
		CONVERT=/opt/local/bin/convert
	fi
fi

# example of makeflow provided with the source
MAKE_FILE_ORG=../example/example.makeflow
MAKE_FILE=example.makeflow

sed -e "s:^CONVERT.*:CONVERT=$CONVERT:" > $MAKE_FILE < $MAKE_FILE_ORG

PRODUCTS="capitol.montage.gif"

after_clean()
{
  rm $MAKE_FILE
}

. ./makeflow_dirs_test_common.sh

dispatch $@

