#!/bin/sh

. ../../dttools/test/test_runner_common.sh
. ./parrot-test.sh

exe="mmap.test"

check_needed()
{
	# Do not run this test in github actions b/c ptrace operations are *very* slow there.
	if [ -n "$GITHUB_ACTION" ]
    then
        return 1
    else
        return 0
    fi
}

prepare()
{
	gcc -I../src/ -g $CCTOOLS_TEST_CCFLAGS -o "$exe" -x c - -x none -lm <<EOF
#include <unistd.h>
#include <fcntl.h>

#include <sys/mman.h>
#include <sys/stat.h>

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void dumpmaps (void)
{
	char buf[65536] = "";
	int fd = open("/proc/self/maps", O_RDONLY);
	read(fd, buf, sizeof(buf)-1);
	write(STDOUT_FILENO, buf, strlen(buf));
	close(fd);
}

int main (int argc, char *argv[])
{
	int i;
	int open_max = sysconf(_SC_OPEN_MAX);
	if (open_max == -1) abort();
	void **mappings = calloc(open_max + 1, sizeof(*mappings));

	/* Test that we can allocate mmap lots of files without hitting _SC_OPEN_MAX fd limit. */
	for (i = 0; i < open_max + 1; i++) {
		int fd = open("/dev/zero", O_RDWR, S_IRUSR|S_IWUSR);
		if (fd == -1) {
			fprintf(stderr, "= -1 [%s] open(...)\n", strerror(errno));
			abort();
		}
		mappings[i] = mmap(NULL, 4096, PROT_READ,MAP_FILE|MAP_PRIVATE, fd, 0);
		if (mappings[i] == NULL) {
			fprintf(stderr, "NULL [%s] = mmap(...)\n", strerror(errno));
			abort();
		}
		if (close(fd) == -1) {
			fprintf(stderr, "-1 [%s] = close(fd)\n", strerror(errno), fd);
			abort();
		}
	}
	for (i = 0; mappings[i]; i++)
		mappings[i] = (munmap(mappings[i], 4096), NULL);

	/* before split */
	fprintf(stderr, "---\\n");
	{
		char f[PATH_MAX] = "/tmp/foo.XXXXXX";
		int fd = mkstemp(f);
		if (fd >= 0) {
			unlink(f);
			ftruncate(fd, 0x2000);
			uint8_t *addr = mmap(NULL, 0x2000, PROT_READ, MAP_SHARED, fd, 0);
			close(fd);
			dumpmaps();
			if (addr != MAP_FAILED) {
				munmap(addr+0x1000, 0x118);
			}
		}
		dumpmaps();
	}

	/* after split */
	fprintf(stderr, "---\\n");
	{
		char f[PATH_MAX] = "/tmp/foo.XXXXXX";
		int fd = mkstemp(f);
		if (fd >= 0) {
			unlink(f);
			ftruncate(fd, 0x2000);
			uint8_t *addr = mmap(NULL, 0x2000, PROT_READ, MAP_SHARED, fd, 0);
			close(fd);
			dumpmaps();
			if (addr != MAP_FAILED) {
				munmap(addr, 0x118);
			}
		}
		dumpmaps();
	}

	/* whole */
	{
		char f[PATH_MAX] = "/tmp/foo.XXXXXX";
		int fd = mkstemp(f);
		if (fd >= 0) {
			unlink(f);
			ftruncate(fd, 0x2000);
			uint8_t *addr = mmap(NULL, 0x2000, PROT_READ, MAP_SHARED, fd, 0);
			close(fd);
			dumpmaps();
			if (addr != MAP_FAILED) {
				munmap(addr, 0x1118);
			}
		}
		dumpmaps();
	}
	return 0;
}
EOF
	return $?
}

run()
{
	parrot -- ./"$exe"
	return $?
}

clean()
{
	rm -f "$exe"
	return 0
}

dispatch "$@"

# vim: set noexpandtab tabstop=4:
