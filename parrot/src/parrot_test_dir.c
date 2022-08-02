#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CATCHUNIX(expr) \
	do {\
		if ((expr) == -1) {\
			perror(#expr);\
			exit(EXIT_FAILURE);\
		}\
	} while (0)

#define check(cmp,expr) \
do {\
	int rc = (expr);\
	if (!(cmp rc)) {\
		fprintf(stderr, "[%s:%d]: unexpected failure: %s %d '%s'\\n", __FILE__, __LINE__, #cmp, rc, strerror(errno));\
		exit(EXIT_FAILURE);\
	}\
} while (0)

int main (int argc, char *argv[])
{
	int fd;

	check(0 <=, fd = open(".", O_RDONLY));
	CATCHUNIX(close(fd));
	check(0 <=, fd = open("/.", O_RDONLY));
	CATCHUNIX(close(fd));
	check(0 <=, fd = open("/..", O_RDONLY));
	CATCHUNIX(close(fd));
	check(0 <=, fd = open("/proc/self/fd", O_RDONLY));
	CATCHUNIX(close(fd));
	check(0 <=, fd = open("/proc/self", O_RDONLY));
	CATCHUNIX(close(fd));

	check(0 <=, fd = open("/", O_RDONLY|O_DIRECTORY));
	CATCHUNIX(close(fd));
	check(0 <=, fd = open("/", O_DIRECTORY)); /* O_RDONLY is 0 on Linux */
	CATCHUNIX(close(fd));

	CATCHUNIX(mkdir("foo", S_IRWXU));
	check(-1 ==, fd = open("foo", O_WRONLY));
	check(EISDIR ==, errno);
	check(-1 ==, fd = open("foo", O_RDWR));
	check(EISDIR ==, errno);

	/*
	This test is exercising behavior that is unspecified in POSIX,
	and seems to vary between versions of Linux.
	*/

	/*
	struct stat info;
	check(0 <=, fd = open("foo/bar", O_CREAT|O_DIRECTORY, S_IRUSR|S_IWUSR));
	CATCHUNIX(fstat(fd, &info));
	check(!!, S_ISREG(info.st_mode));
	CATCHUNIX(close(fd));
	CATCHUNIX(unlink("foo/bar"));
	*/

	CATCHUNIX(fd = open("foo/bar", O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR));
	CATCHUNIX(close(fd));

	check(-1 ==, fd = open("foo/bar", O_RDONLY|O_DIRECTORY));
	check(ENOTDIR ==, errno);

	/* This open should fail, but due to Parrot's handling of paths,
	we have a minor inconsistency with Linux behavior. */
	//check(-1 ==, fd = open("foo/bar/..", O_RDONLY|O_DIRECTORY));
	//check(ENOTDIR ==, errno);

	unlink("foo/bar");
	rmdir("foo");

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
