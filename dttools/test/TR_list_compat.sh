#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="list_compat.test"

prepare()
{
	${CC} -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#include "list.h"

int main (int argc, char *argv[])
{
	struct list *list = list_create();
	assert(list_size(list) == 0);

	list_push_head(list, (void *) 1);
	assert(list_size(list) == 1);
	assert(list_peek_head(list) == (void *) 1);
	assert(list_peek_tail(list) == (void *) 1);

	list_push_tail(list, (void *) 2);
	assert(list_size(list) == 2);
	assert(list_peek_head(list) == (void *) 1);
	assert(list_peek_tail(list) == (void *) 2);

	list_push_tail(list, (void *) 3);
	assert(list_size(list) == 3);
	assert(list_peek_head(list) == (void *) 1);
	assert(list_peek_tail(list) == (void *) 3);

	assert(list_next_item(list) == NULL);
	list_first_item(list);
	void *i;
	while ((i = list_next_item(list))) {
		if (i == (void *) 2) break;
	}
	assert(list_peek_current(list) == (void *) 3);
	
	assert(list_pop_tail(list) == (void *) 3);
	assert(list_size(list) == 2);
	assert(list_peek_head(list) == (void *) 1);
	assert(list_peek_tail(list) == (void *) 2);

	assert(list_pop_head(list) == (void *) 1);
	assert(list_size(list) == 1);
	assert(list_peek_head(list) == (void *) 2);
	assert(list_peek_tail(list) == (void *) 2);

	assert(list_pop_tail(list) == (void *) 2);
	assert(list_size(list) == 0);
	assert(list_peek_head(list) == NULL);
	assert(list_peek_tail(list) == NULL);

	return 0;
}
EOF
	return $?
}

run()
{
	./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"
