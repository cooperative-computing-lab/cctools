#!/bin/sh

. ../../dttools/test/test_runner_common.sh

exe="list.test"

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none ../src/libdttools.a -lm <<EOF
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>

#include "linkedlist.h"

int main (int argc, char *argv[])
{
	bool ok;
	intptr_t item = 0;

	// first create an empty list
	struct linkedlist *list = linkedlist_create();
	assert(list);
	assert(linkedlist_size(list) == 0);

	// make a cursor on it
	struct linkedlist_cursor *cur = linkedlist_cursor_create(list);
	assert(cur);

	// ensure that things don't work when position is undefined
	assert(linkedlist_tell(cur) == -1);
	ok = linkedlist_seek(cur, 0);
	assert(!ok);
	ok = linkedlist_next(cur);
	assert(!ok);
	ok = linkedlist_prev(cur);
	assert(!ok);
	ok = linkedlist_get(cur, (void **) &item);
	assert(!ok);
	ok = linkedlist_set(cur, (void *) item);
	assert(!ok);
	ok = linkedlist_drop(cur);
	assert(!ok);

	// put in a couple of items
	linkedlist_insert(cur, (void *) 3);
	assert(linkedlist_size(list) == 1);
	assert(linkedlist_tell(cur) == -1);
	linkedlist_insert(cur, (void *) 2);
	assert(linkedlist_size(list) == 2);
	assert(linkedlist_tell(cur) == -1);

	// move on to an item
	ok = linkedlist_seek(cur, 0);
	assert(ok);

	// try moving past the end
	ok = linkedlist_seek(cur, 2);
	assert(!ok);

	// try basic functionality
	assert(linkedlist_tell(cur) == 0);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 2);
	ok = linkedlist_set(cur, (void *) 5);
	assert(ok);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 5);

	// try a negative index
	ok = linkedlist_seek(cur, -1);
	assert(ok);

	// try to seek past the beginning
	ok = linkedlist_seek(cur, -3);
	assert(!ok);

	// check we're on the right item
	assert(linkedlist_tell(cur) == 1);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 3);

	// make another cursor, and insert between the two elements
	struct linkedlist_cursor *alt = linkedlist_cursor_create(list);
	assert(linkedlist_tell(alt) == -1);
	ok = linkedlist_seek(alt, -2);
	assert(ok);
	assert(linkedlist_tell(alt) == 0);
	linkedlist_insert(alt, (void *) 7);
	assert(linkedlist_size(list) == 3);

	// make sure the original cursor is OK
	assert(linkedlist_tell(cur) == 2);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 3);

	// and the new cursor
	assert(linkedlist_tell(alt) == 0);
	ok = linkedlist_get(alt, (void **) &item);
	assert(ok);
	assert(item == 5);

	// move to the new item
	ok = linkedlist_next(alt);
	assert(ok);

	// and check both again
	assert(linkedlist_tell(cur) == 2);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 3);

	assert(linkedlist_tell(alt) == 1);
	ok = linkedlist_get(alt, (void **) &item);
	assert(ok);
	assert(item == 7);

	// now move the old cursor to the same place
	ok = linkedlist_prev(cur);
	assert(ok);

	// and check both again
	assert(linkedlist_tell(cur) == 1);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 7);

	assert(linkedlist_tell(alt) == 1);
	ok = linkedlist_get(alt, (void **) &item);
	assert(ok);
	assert(item == 7);

	// drop the middle element
	ok = linkedlist_drop(cur);
	assert(ok);
	assert(linkedlist_size(list) == 2);

	// and check both again
	assert(linkedlist_tell(cur) == 1);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 3);

	assert(linkedlist_tell(alt) == 1);
	ok = linkedlist_get(alt, (void **) &item);
	assert(ok);
	assert(item == 3);

	// walk off the right
	ok = linkedlist_next(alt);
	assert(!ok);
	assert(linkedlist_tell(alt) == -1);

	// and the left
	ok = linkedlist_prev(cur);
	assert(ok);
	assert(linkedlist_tell(cur) == 0);
	ok = linkedlist_get(cur, (void **) &item);
	assert(ok);
	assert(item == 5);
	ok = linkedlist_prev(cur);
	assert(!ok);
	assert(linkedlist_tell(cur) == -1);

	// delete the cursors and try to delete while there are items
	linkedlist_cursor_destroy(cur);
	linkedlist_cursor_destroy(alt);
	ok = linkedlist_destroy(list);
	assert(!ok);

	// we still need a cursor...
	cur = linkedlist_cursor_create(list);
	assert(cur);

	// clear the list
	linkedlist_seek(cur, 0);
	while (linkedlist_tell(cur) != -1) {
		ok = linkedlist_drop(cur);
		assert(ok);
	}

	// try to delete while there's an outstanding cursor
	ok = linkedlist_destroy(list);
	assert(!ok);

	// delete said cursor
	linkedlist_cursor_destroy(cur);

	// finally delete the list
	ok = linkedlist_destroy(list);
	assert(ok);

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
