#!/bin/sh

. ../../dttools/src/test_runner.common.sh

exe="mmap.test"

prepare()
{
	gcc -I../src/ -g -o "$exe" -x c - -x none -lm <<EOF
#include <unistd.h>
#include <fcntl.h>

#include <sys/mman.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main (int argc, char *argv[])
{
	int i;
	/* Test that we can allocate mmap lots of files without hitting _SC_OPEN_MAX fd limit. */
	for (i = 0; i < (int)sysconf(_SC_OPEN_MAX)+1; i++) {
		int fd = open("/dev/zero", O_RDWR, S_IRUSR|S_IWUSR);
		if (fd == -1) {
			fprintf(stderr, "= -1 [%s] open(...)\n", strerror(errno));
			abort();
		}
		char *file = mmap(NULL, 4096, PROT_READ,MAP_FILE|MAP_PRIVATE,fd,0);
		if (file == NULL) {
			fprintf(stderr, "NULL [%s] = mmap(...)\n", strerror(errno));
			abort();
		}
		if (close(fd) == -1) {
			fprintf(stderr, "-1 [%s] = close(fd)\n", strerror(errno), fd);
			abort();
		}
	}
	return 0;
}
EOF
	return $?
}

run()
{
	../src/parrot_run -- ./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch $@

# vim: set noexpandtab tabstop=4:
