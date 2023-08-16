/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_reli.h"
#include "chirp_protocol.h"
#include "chirp_thirdput.h"
#include "chirp_acl.h"

#include "debug.h"

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/stat.h>

static INT64_T chirp_thirdput_recursive(const char *subject, const char *lpath, const char *hostname, const char *rpath, const char *hostsubject, time_t stoptime)
{
	struct chirp_stat info;
	INT64_T size = 0, result;
	char newlpath[CHIRP_PATH_MAX];
	char newrpath[CHIRP_PATH_MAX];
	int save_errno;
	int my_target_acl = 0;

	result = cfs->lstat(lpath, &info);
	if(result < 0)
		return result;

	if(S_ISDIR(info.cst_mode)) {
		CHIRP_FILE *aclfile;
		struct chirp_dir *dir;
		struct chirp_dirent *d;
		char aclsubject[CHIRP_PATH_MAX];
		int aclflags;

		if(!chirp_acl_check_dir(lpath, subject, CHIRP_ACL_LIST))
			return -1;

		// create the directory, but do not fail if it already exists
		result = chirp_reli_mkdir(hostname, rpath, S_IRWXU, stoptime);
		if(result < 0 && errno != EEXIST)
			return result;

		// set the access control to include the initiator
		result = chirp_reli_setacl(hostname, rpath, subject, "rwldax", stoptime);
		if(result < 0 && errno != EACCES)
			return result;

		// transfer each of the directory contents recurisvely
		dir = cfs->opendir(lpath);
		while((d = cfs->readdir(dir))) {
			if(!strcmp(d->name, "."))
				continue;
			if(!strcmp(d->name, ".."))
				continue;
			if(!strncmp(d->name, ".__", 3))
				continue;
			sprintf(newlpath, "%s/%s", lpath, d->name);
			sprintf(newrpath, "%s/%s", rpath, d->name);
			result = chirp_thirdput_recursive(subject, newlpath, hostname, newrpath, hostsubject, stoptime);
			if(result >= 0) {
				size += result;
			} else {
				result = -1;
				break;
			}
		}
		save_errno = errno;

		// finally, set the acl to duplicate the source directory,
		// but do not take away permissions from me or the initiator
		cfs->closedir(dir);

		aclfile = chirp_acl_open(lpath);
		if(!aclfile)
			return -1;

		while(chirp_acl_read(aclfile, aclsubject, &aclflags)) {

			// wait until the last minute to take away my permissions
			if(!strcmp(aclsubject, hostsubject)) {
				my_target_acl = aclflags;
			}
			// do not take permissions away from the initiator
			if(!strcmp(aclsubject, subject)) {
				continue;
			}

			chirp_reli_setacl(hostname, rpath, aclsubject, chirp_acl_flags_to_text(aclflags), stoptime);
		}

		chirp_acl_close(aclfile);

		// after setting everything else, then set my permissions from the ACL
		chirp_reli_setacl(hostname, rpath, hostsubject, chirp_acl_flags_to_text(my_target_acl), stoptime);

		errno = save_errno;

		if(result >= 0) {
			return size;
		} else {
			return -1;
		}

	} else if(S_ISLNK(info.cst_mode)) {
		if(!chirp_acl_check(lpath, subject, CHIRP_ACL_READ))
			return -1;
		result = cfs->readlink(lpath, newlpath, sizeof(newlpath));
		if(result < 0)
			return -1;
		return chirp_reli_symlink(hostname, newlpath, rpath, stoptime);
	} else if(S_ISREG(info.cst_mode)) {
		if(!chirp_acl_check(lpath, subject, CHIRP_ACL_READ))
			return -1;
		int fd = cfs->open(lpath, O_RDONLY, 0);
		if(fd >= 0) {
			struct chirp_file *F = chirp_reli_open(hostname, rpath, O_WRONLY|O_CREAT|O_TRUNC, info.cst_mode, stoptime);
			if(F) {
				char buffer[65536];
				INT64_T offset = 0;
				INT64_T nread;
				while ((nread = cfs->pread(fd, buffer, sizeof(buffer), offset)) > 0) {
					INT64_T nwritten = 0;
					while (nwritten < nread) {
						INT64_T nwrite = chirp_reli_pwrite(F, buffer+nwritten, nread-nwritten, offset, stoptime);
						if (nwrite == -1) {
							save_errno = errno;
							cfs->close(fd);
							chirp_reli_close(F, stoptime);
							errno = save_errno;
							return -1;
						}
						nwritten += nwrite;
						offset += nwrite;
					}
				}
				if(nread == -1) offset = -1;
				save_errno = errno;
				cfs->close(fd);
				chirp_reli_close(F, stoptime);
				errno = save_errno;
				return offset;
			} else {
				save_errno = errno;
				cfs->close(fd);
				errno = save_errno;
				return -1;
			}
		} else {
			return -1;
		}
	} else {
		return 0;
	}

	return -1;
}

INT64_T chirp_thirdput(const char *subject, const char *lpath, const char *hostname, const char *rpath, time_t stoptime)
{
	INT64_T result;
	time_t start, stop;
	char hostsubject[CHIRP_PATH_MAX];

	result = chirp_reli_whoami(hostname, hostsubject, sizeof(hostsubject), stoptime);
	if(result < 0)
		return result;

	debug(D_DEBUG, "thirdput: sending %s to chirp://%s/%s", lpath, hostname, rpath);

	start = time(0);
	result = chirp_thirdput_recursive(subject, lpath, hostname, rpath, hostsubject, stoptime);
	stop = time(0);

	if(stop == start)
		stop++;

	if(result >= 0) {
		debug(D_DEBUG, "thirdput: sent %"PRId64" bytes in %d seconds (%.1lfMB/s)", result, (int) (stop - start), result / (double) (stop - start));
	} else {
		debug(D_DEBUG, "thirdput: error: %s\n", strerror(errno));
	}

	return result;
}

/* vim: set noexpandtab tabstop=8: */
