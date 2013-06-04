/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.

This module is based heavily on code provided
Christophe Blanchet, IBCP
*/

#ifdef HAS_EGEE

#include "pfs_table.h"
#include "pfs_service.h"
#include "pfs_process.h"

extern "C" {
#include "debug.h"
#include "stringtools.h"

#include "lcg_util.h"
#include "lfc_api.h"
#include "serrno.h"

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
}
#define LFN_URL_HEAD "lfn:"
static int lfc_error_to_errno(int e)
{
	int r;

	switch (e) {
	case SENOSHOST:
	case SENOSSERV:
	case SENOTRFILE:
	case SEENTRYNFND:
		r = ENOENT;
		break;

	case SETIMEDOUT:
		r = ETIMEDOUT;
		break;

	case SEBADFFORM:
	case SEBADFOPT:
	case SEINCFOPT:
	case SENAMETOOLONG:
	case SEUBUF2SMALL:
	case SEBADVERSION:
	case SEMSGINVRNO:
	case SEUMSG2LONG:
	case SENOCONFIG:
		r = EINVAL;
		break;

	case SEINTERNAL:
	case SECONNDROP:
	case SEBADIFNAM:
	case SECOMERR:
	case SERTYEXHAUST:
	case SECTHREADINIT:
	case SECTHREADERR:
	case SESYSERR:
	case SEADNSINIT:
	case SEADNSSUBMIT:
	case SEADNS:
	case SEADNSTOOMANY:
		r = EIO;
		break;

	case SENOMAPDB:
	case SENOMAPFND:
	case SEUSERUNKN:
		r = EACCES;
		break;

	case SEOPNOTSUP:
		r = ENOSYS;
		break;

	case SEWOULDBLOCK:
		r = EWOULDBLOCK;
		break;

	case SEINPROGRESS:
		r = EINPROGRESS;
		break;

	case SENOTADMIN:
		r = EPERM;
		break;

	case SEENTRYEXISTS:
		r = EEXIST;
		break;

	case SEGROUPUNKN:
	case SECHECKSUM:
		r = EINVAL;
		break;

	case SELOOP:
		r = ELOOP;
		break;

	case ESEC_SYSTEM:
	case ESEC_BAD_CREDENTIALS:
	case ESEC_NO_CONTEXT:
	case ESEC_BAD_MAGIC:
	case ESEC_NO_USER:
	case ESEC_NO_PRINC:
	case ESEC_NO_SECMECH:
	case ESEC_CTX_NOT_INITIALIZED:
	case ESEC_PROTNOTSUPP:
	case ESEC_NO_SVC_NAME:
	case ESEC_NO_SVC_TYPE:
	case ESEC_NO_SECPROT:
	case ESEC_BAD_PEER_RESP:
		r = EACCES;
		break;

		/* Most of the error numbers returned are valid Unix errnos. */
	default:
		r = e;
		break;

	}
	debug(D_LFC, "serror %d (%s) translates to unix errno %d (%s)", e, sstrerror(e), r, strerror(r));
	return r;
}

class pfs_service_lfc:public pfs_service {
      public:

	void free_replica_list(char **replicas) {
		int i;

		for(i = 0; replicas[i]; i++) {
			free(replicas[i]);
		} free(replicas);
	}

	/*
	   Need a more clever replica choice.
	   This one simply picks one from random.
	 */

	int choose_replica_from_list(char **replicas) {
		int n = 0;
		while(replicas[n]) {
			debug(D_LFC, "replica: %s", replicas[n]);
			n++;
		}
		return rand() % n;
	}

	virtual pfs_file *open(pfs_name * name, int flags, mode_t mode) {

		char **replicas;

		debug(D_LFC, "open: querying catalog for replicas");

		int result = lcg_lr(name->path + 4, 0, 0, 0, &replicas);
		if(result < 0) {
			/* lcg_lr sets errno, not serrno */
			return 0;
		}

		int choice = choose_replica_from_list(replicas);

		debug(D_LFC, "open: chose replica %s", replicas[choice]);

		free_replica_list(replicas);

		extern int pfs_force_cache;
		return pfs_current->table->open_object(replicas[choice], flags, mode, pfs_force_cache);
	}

	virtual int stat(pfs_name * name, struct pfs_stat *buf) {
		struct lfc_filestatg statbuf;

		debug(D_LFC, "stat %s", name->path);

		pfs_service_emulate_stat(name, buf);

		if(lfc_statg(name->path + 4, NULL, &statbuf) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}

		buf->st_mode = statbuf.filemode;
		buf->st_nlink = statbuf.nlink;
		buf->st_uid = statbuf.uid;
		buf->st_gid = statbuf.gid;
		buf->st_size = statbuf.filesize;
		buf->st_atime = statbuf.atime;
		buf->st_mtime = statbuf.mtime;
		buf->st_ctime = statbuf.ctime;

		return 0;
	}

	virtual int lstat(pfs_name * name, struct pfs_stat *buf) {
		return this->stat(name, buf);
	}

	virtual int access(pfs_name * name, int mode) {
		debug(D_LFC, "access %d %d", name->path, mode);

		if(lfc_access(name->path, mode) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}

		return 0;
	}

	virtual int unlink(pfs_name * name) {
		debug(D_LFC, "unlink %s", name->path);

		if(lfc_unlink(name->path) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}
		return 0;
	}

	virtual int chmod(pfs_name * name, mode_t mode) {
		debug(D_LFC, "chmod %d %d", name->path, mode);

		if(lfc_chmod(name->path, mode) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}
		return 0;
	}

	virtual int rename(pfs_name * oldname, pfs_name * newname) {
		debug(D_LFC, "rename %s %s", oldname->path, newname->path);

		if(lfc_rename(oldname->path + 4, newname->path + 4) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}

		return 0;
	}

	virtual pfs_dir *getdir(pfs_name * name) {
		struct lfc_direnstatg *d;
		lfc_DIR *lfcdir;
		pfs_dir *pfsdir = NULL;

		debug(D_LFC, "getdir %s", name->path);

		lfcdir = lfc_opendirg(name->path + 4, NULL);
		if(lfcdir) {
			pfsdir = new pfs_dir(name);

			while((d = lfc_readdirg(lfcdir))) {
				pfsdir->append(d->d_name);

			}
			lfc_closedir(lfcdir);
			return pfsdir;
		} else {
			errno = lfc_error_to_errno(serrno);
			return 0;
		}
	}

	virtual int chdir(pfs_name * name, char *newpath, int size) {
		debug(D_LFC, "chdir %s", name->path);

		if(lfc_chdir(name->path + 4) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}

		strncpy(newpath, name->path, size);
		return 0;
	}

	virtual int mkdir(pfs_name * name, mode_t mode) {
		debug(D_LFC, "mkdir %s %d", name->path, mode);

		if(lfc_mkdir(name->path + 4, mode) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}

		return 0;
	}

	virtual int rmdir(pfs_name * name) {
		debug(D_LFC, "rmdir %s", name->path);
		if(lfc_rmdir(name->path + 4) < 0) {
			errno = lfc_error_to_errno(serrno);
			return -1;
		}

		return 0;
	}

	virtual int is_seekable() {
		return 1;
	}

};

static pfs_service_lfc pfs_service_lfc_instance;
pfs_service *pfs_service_lfc = &pfs_service_lfc_instance;

#endif
