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
#include <string.h>
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
		int err_fd;
		char *filename = getenv("CCTOOLS_DISK_ALLOC");
		if(!filename) {
			fprintf(stderr, "OPEN ERROR: could not set flag to alert resource management system that loop device is full.\n");
			fprintf(stderr, "OPEN ERROR: device capacity reached.\n");
			return fd;
		}
		err_fd = original_open(filename, O_RDWR | O_CREAT);
		if(err_fd < 0) { fprintf(stderr, "OPEN ERROR: could not alert resource management system that loop device is full.\n"); }
		fprintf(stderr, "OPEN ERROR: device capacity reached.\n");
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
		char *filename = getenv("CCTOOLS_DISK_ALLOC");
		if(!filename) {
			original_write(STDERR_FILENO, "WRITE ERROR: could not set flag to alert resource management system that loop device is full.\n", 94);
			original_write(STDERR_FILENO, "WRITE ERROR: device capacity reached.\n", 39);
			return real_count;
		}
		fd = open(filename, O_RDWR | O_CREAT);
		if(fd < 0) { original_write(STDERR_FILENO, "WRITE ERROR: could not alert resource management system that loop device is full.\n", 77); }	
		original_write(STDERR_FILENO, "WRITE ERROR: device capacity reached.\n", 39);
		return real_count;
	}

	if(!errno) {
	   errno = prev_errno;
	}

	return real_count;
}
