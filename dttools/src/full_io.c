/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#define _XOPEN_SOURCE 500
#define _LARGEFILE64_SOURCE 1

#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <stdio.h>

#include "full_io.h"

ssize_t full_read(int fd, void *buf, size_t count)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {

		chunk = read(fd, buf, count);
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

ssize_t full_write(int fd, const void *buf, size_t count)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
		chunk = write(fd, buf, count);
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

INT64_T full_pread64(int fd, void *buf, INT64_T count, INT64_T offset)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
		chunk = pread(fd, buf, count, offset);
#else
		chunk = pread64(fd, buf, count, offset);
#endif
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
			offset += (INT64_T) chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

INT64_T full_pwrite64(int fd, const void *buf, INT64_T count, INT64_T offset)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
		chunk = pwrite(fd, buf, count, offset);
#else
		chunk = pwrite64(fd, buf, count, offset);
#endif
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
			offset += (INT64_T) chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

ssize_t full_pread(int fd, void *buf, size_t count, off_t offset)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
		chunk = pread(fd, buf, count, offset);
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
			offset += chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

ssize_t full_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
		chunk = pwrite(fd, buf, count, offset);
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
			offset += chunk;
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

ssize_t full_fread(FILE * file, void *buf, size_t count)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
		chunk = fread(buf, 1, count, file);
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

ssize_t full_fwrite(FILE * file, const void *buf, size_t count)
{
	ssize_t total = 0;
	ssize_t chunk = 0;

	while(count > 0) {
		chunk = fwrite(buf, 1, count, file);
		if(chunk < 0) {
			if(errno == EINTR) {
				continue;
			} else {
				break;
			}
		} else if(chunk == 0) {
			break;
		} else {
			total += chunk;
			count -= chunk;
			buf = (char *) buf + chunk; //Previously without the cast! (but sizeof char == 1)
		}
	}

	if(total > 0) {
		return total;
	} else {
		if(chunk == 0) {
			return 0;
		} else {
			return -1;
		}
	}
}
