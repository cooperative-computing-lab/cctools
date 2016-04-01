#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE // Aaaaaah!!
#endif

#if !defined(RTLD_NEXT)
#define RTLD_NEXT 0
#endif

#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <signal.h>
#include <inttypes.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

int open(const char *path, int flags, ...)
{

	__typeof__(open) *original_open = dlsym(RTLD_NEXT, "open");

	va_list ap;
	int fd;
	int mode;
	int prev_errno = errno;

	va_start(ap, flags);
	mode = va_arg(ap, int);
	va_end(ap);

	fd = original_open(path, flags, mode);

	if(fd == -1 && errno == ENOSPC) {
		fprintf(stderr, "OPEN ERROR: inode capacity reached.\n");
		return fd;
	}

	if(!errno) {
		errno = prev_errno;
	}

	return fd;
}

ssize_t write(int fd, const void *buf, size_t count) {

	__typeof__(write) *original_write = dlsym(RTLD_NEXT, "write");

	int real_count;
	int prev_errno = errno;
	errno = 0;
	real_count = original_write(fd, buf, count);

	if(real_count < 0 && errno == ENOSPC) {
		int fd;
		fd = open("../loop_dev_report.txt", O_RDWR | O_CREAT);
		char *desc = sprintf("fd: %d\n", fd);
		if(fd < 0) { original_write(STDERR_FILENO, "WRITE ERROR: could not alert Work Queue of full loop device.\n", 61); }
		original_write(STDERR_FILENO, "WRITE ERROR: device capacity reached.\n", 39);
		return real_count;
	}

	if(!errno) {
	   errno = prev_errno;
	}

	return real_count;
}
