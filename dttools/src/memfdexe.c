#include "debug.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#if defined(__linux__)
#	include <syscall.h>
#endif
#include <unistd.h>

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int memfdexe (const char *name, const char *extradir)
{
	int fd;

#if defined(__linux__) && defined(SYS_memfd_create)
	fd = syscall(SYS_memfd_create, name, 0);
#else
	errno = ENOSYS;
	fd = -1;
#endif /* defined(__linux__) && defined(SYS_memfd_create) */

	if (fd == -1 && errno == ENOSYS) {
		int i;
		const char *dirs[] = {
			"/dev/shm",
			"/tmp",
			"/var/tmp",
			extradir,
			NULL
		};
		size_t pagesize = getpagesize();
		for (i = 0; dirs[i]; i++) {
			char path[PATH_MAX];
			snprintf(path, sizeof(path), "%s/%s.XXXXXX", dirs[i], name);

			debug(D_DEBUG, "trying to create memfdexe '%s'", path);
			fd = mkstemp(path);
			if (fd >= 0) {
				if (unlink(path) == -1) {
					debug(D_DEBUG, "could not unlink memfdexe '%s': %s", path, strerror(errno));
					/* no way to fix that, might as well continue... */
				}
				if (fchmod(fd, S_IRWXU) == -1) {
					debug(D_DEBUG, "could not set permissions on memfdexe: %s", strerror(errno));
					close(fd);
					continue;
				}

				/* test if we can use it for executable data (i.e. is dir on a file system mounted with the 'noexec' option) */
				if (ftruncate(fd, pagesize) == -1) {
					debug(D_DEBUG, "could not grow memfdexe: %s", strerror(errno));
					close(fd);
					continue;
				}
				void *addr = mmap(NULL, pagesize, PROT_READ|PROT_EXEC, MAP_SHARED, fd, 0);
				if (addr == MAP_FAILED) {
					debug(D_DEBUG, "failed executable mapping: %s", strerror(errno));
					close(fd);
					continue;
				}
				munmap(addr, pagesize);
				ftruncate(fd, 0);
				break;
			} else {
				debug(D_DEBUG, "could not create memfdexe: %s", strerror(errno));
			}
		}
	}
	return fd;
}

/* vim: set noexpandtab tabstop=8: */
