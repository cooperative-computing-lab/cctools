/* Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
 * Copyright (C) 2022 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
*/

#include "chirp_acl.h"
#include "chirp_alloc.h"
#include "chirp_audit.h"
#include "chirp_filesystem.h"
#include "chirp_fs_confuga.h"
#include "chirp_group.h"
#include "chirp_job.h"
#include "chirp_protocol.h"
#include "chirp_reli.h"
#include "chirp_stats.h"
#include "chirp_thirdput.h"
#include "chirp_types.h"

#include "auth_all.h"
#include "catalog_query.h"
#include "cctools.h"
#include "change_process_title.h"
#include "create_dir.h"
#include "daemon.h"
#include "datagram.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "get_canonical_path.h"
#include "getopt_aux.h"
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "json.h"
#include "jx.h"
#include "jx_print.h"
#include "link.h"
#include "list.h"
#include "load_average.h"
#include "macros.h"
#include "path.h"
#include "pattern.h"
#include "random.h"
#include "stringtools.h"
#include "url_encode.h"
#include "username.h"
#include "uuid.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <fcntl.h>
#include <pwd.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/utsname.h>
#include <sys/wait.h>

#if defined(HAS_ATTR_XATTR_H)
#include <attr/xattr.h>
#elif defined(HAS_SYS_XATTR_H)
#include <sys/xattr.h>
#endif

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define GC_TIMEOUT  (86400)

/* The maximum chunk of memory the server will allocate to handle I/O */
#define MAX_BUFFER_SIZE (16*1024*1024)

struct list *catalog_host_list;
char         chirp_hostname[DOMAIN_NAME_MAX] = "";
char         chirp_owner[USERNAME_MAX] = "";
int          chirp_port = CHIRP_PORT;
char         chirp_project_name[128];
char         chirp_transient_path[PATH_MAX] = "."; /* local file system stuff */
cctools_uuid_t chirp_uuid[1];

static char        address[LINK_ADDRESS_MAX];
static time_t      advertise_alarm = 0;
static int         advertise_timeout = 300; /* five minutes */
static int         config_pipe[2] = {-1, -1};
static char        hostname[DOMAIN_NAME_MAX];
static int         idle_timeout = 60; /* one minute */
static UINT64_T    minimum_space_free = 0;
static UINT64_T    root_quota = 0;
static gid_t       safe_gid = 0;
static uid_t       safe_uid = 0;
static const char *safe_username = 0;
static int         sim_latency = 0;
static int         stall_timeout = 3600; /* one hour */
static time_t      starttime;

/* space_available() is a simple mechanism to ensure that a runaway client does
 * not use up every last drop of disk space on a machine.  This function
 * returns false if consuming the given amount of space will leave less than a
 * fixed amount of headroom on the disk.  Note that get_disk_info() is quite
 * expensive, so we do not call it more than once per second.
 */
static int space_available(INT64_T amount)
{
	static UINT64_T avail;
	static time_t last_check = 0;
	int check_interval = 1;
	time_t current;

	if(minimum_space_free == 0)
		return 1;

	current = time(0);

	if((current - last_check) > check_interval) {
		struct chirp_statfs buf;

		if(chirp_alloc_statfs("/", &buf) == -1) {
			return 0;
		}
		avail = buf.f_bsize * buf.f_bfree;
		last_check = current;
	}

	if((avail - amount) > minimum_space_free) {
		avail -= amount;
		return 1;
	} else {
		errno = ENOSPC;
		return 0;
	}
}

static void downgrade (void)
{
	/* downgrade privileges (necessary so files are created with correct uid) */
	if(safe_username) {
		debug(D_AUTH, "changing to uid %d gid %d", safe_uid, safe_gid);
		if (setgid(safe_gid) == -1)
			fatal("could not setgid: %s", strerror(errno));
		if (setuid(safe_uid) == -1)
			fatal("could not setuid: %s", strerror(errno));
	}

}

static int backend_setup(const char *url)
{
	if(cfs->init(url, chirp_uuid) == -1)
		fatal("could not initialize %s backend filesystem: %s", url, strerror(errno));

	if(!chirp_acl_init_root("/"))
		fatal("could not initialize %s ACL: %s", url, strerror(errno));

	if(chirp_alloc_init(root_quota) == -1)
		fatal("could not initialize %s allocations: %s", url, strerror(errno));

	return 0;
}

static int backend_bootstrap(const char *url)
{
	downgrade();
	return backend_setup(url);
}

static int gc_tickets(const char *url)
{
	downgrade();
	backend_setup(url);

	chirp_acl_gctickets();

	cfs->destroy();

	return 0;
}

static int update_all_catalogs(const char *url)
{
	struct chirp_statfs info;
	struct utsname name;
	int cpus;
	double avg[3];
	UINT64_T memory_total, memory_avail;

	uname(&name);
	string_tolower(name.sysname);
	string_tolower(name.machine);
	string_tolower(name.release);
	load_average_get(avg);
	cpus = load_average_get_cpus();

	downgrade();
	backend_setup(url);

	if(chirp_alloc_statfs("/", &info) < 0) {
		memset(&info, 0, sizeof(info));
	}

	host_memory_info_get(&memory_avail, &memory_total);

	struct jx *j = jx_object(0);

	jx_insert_string (j,"type","chirp");
	jx_insert_integer(j,"avail",info.f_bavail * info.f_bsize);
	jx_insert_string (j,"backend",url);
	jx_insert_string (j,"cpu",name.machine);
	jx_insert_integer(j,"cpus", cpus);
	jx_insert_double  (j,"load1",avg[0]);
	jx_insert_double  (j,"load5",avg[1]);
	jx_insert_double  (j,"load15",avg[2]);
	jx_insert_integer(j,"memory_avail",memory_avail);
	jx_insert_integer(j,"memory_total",memory_total);
	jx_insert_integer(j,"minfree",minimum_space_free);
	jx_insert_string (j,"name",hostname);
	jx_insert_string (j,"opsys",name.sysname);
	jx_insert_string (j,"opsysversion",name.release);
	jx_insert_string (j,"owner",chirp_owner);
	jx_insert_integer(j,"port",chirp_port);
	jx_insert_integer(j,"starttime",starttime);
	jx_insert_integer(j,"total",info.f_blocks * info.f_bsize);
	jx_insert_string (j,"uuid",chirp_uuid->str);

	if (strlen(chirp_project_name)) {
		jx_insert_string(j,"project",chirp_project_name);
	}

	jx_insert(j,
		jx_string("url"),
		jx_format("chirp://%s:%d", hostname, chirp_port));

	jx_insert(j,
		jx_string("version"),
		jx_format("%d.%d.%d", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO));

	chirp_stats_summary(j);

	char *message = jx_print_string(j);

	const char *host;
	LIST_ITERATE(catalog_host_list,host) {
		catalog_query_send_update(host,message,CATALOG_UPDATE_BACKGROUND);
	}

	free(message);
	jx_delete(j);
	cfs->destroy();

	return 0;
}

static int run_in_child_process(int (*func) (const char *a), const char *args, const char *name)
{
	debug(D_PROCESS, "*** %s starting ***", name);

	pid_t pid = fork();
	if(pid == 0) {
		_exit(func(args));
	} else if(pid > 0) {
		int status;
		while(waitpid(pid, &status, 0) != pid) {
		}
		debug(D_PROCESS, "*** %s complete ***", name);
		if(WIFEXITED(status)) {
			debug(D_PROCESS, "pid %d exited with %d", pid, WEXITSTATUS(status));
			return WEXITSTATUS(status);
		} else if(WIFSIGNALED(status)) {
			debug(D_PROCESS, "pid %d failed due to signal %d (%s)", pid, WTERMSIG(status), string_signal(WTERMSIG(status)));
			return -1;
		} else assert(0);
	} else {
		debug(D_PROCESS, "couldn't fork: %s", strerror(errno));
		return -1;
	}
}

/* The parent Chirp server process maintains a pipe connected to all child
 * processes.  When the child must update the global state, it is done by
 * sending a message to the config pipe, which the parent reads and processes.
 * This code relies on the guarantee that all writes of less than PIPE_BUF size
 * are atomic, so here we expect a read to return one or more complete
 * messages, each delimited by a newline.
*/
static void config_pipe_handler(int fd)
{
	char line[PIPE_BUF];
	char flag[PIPE_BUF];
	char subject[PIPE_BUF];
	char address[PIPE_BUF];
	UINT64_T ops, bytes_read, bytes_written;

	while(1) {
		fcntl(fd, F_SETFL, O_NONBLOCK);

		ssize_t length = read(fd, line, PIPE_BUF);
		if(length <= 0)
			return;

		line[length] = 0;

		const char *msg = strtok(line, "\n");
		while(msg) {
			debug(D_DEBUG, "config message: %s", msg);

			if(sscanf(msg, "debug %s", flag) == 1) {
				debug_flags_set(flag);
			} else if(sscanf(msg, "stats %s %s %" SCNu64 " %" SCNu64 " %" SCNu64, address, subject, &ops, &bytes_read, &bytes_written) == 5) {
				chirp_stats_collect(address, subject, ops, bytes_read, bytes_written);
			} else {
				debug(D_NOTICE, "bad config message: %s\n", msg);
			}
			msg = strtok(0, "\n");
		}
	}
}

static void path_fix(char path[CHIRP_PATH_MAX])
{
	char decoded[CHIRP_PATH_MAX];
	decoded[0] = '/'; /* anchor all paths with root */
	url_decode(path, decoded+1, sizeof(decoded)-1); /* remove the percent-hex encoding */
	path_collapse(decoded, path, 1); /* sanitize the decoded path */
}

static int errno_to_chirp(int e)
{
	switch (e) {
	case EACCES:
	case EPERM:
	case EROFS:
		return CHIRP_ERROR_NOT_AUTHORIZED;
	case ENOENT:
		return CHIRP_ERROR_DOESNT_EXIST;
	case EEXIST:
		return CHIRP_ERROR_ALREADY_EXISTS;
	case EFBIG:
		return CHIRP_ERROR_TOO_BIG;
	case ENOSPC:
	case EDQUOT:
		return CHIRP_ERROR_NO_SPACE;
	case ENOMEM:
		return CHIRP_ERROR_NO_MEMORY;
#ifdef ENOATTR
	case ENOATTR:
#endif
	case ENOSYS:
	case EINVAL:
		return CHIRP_ERROR_INVALID_REQUEST;
	case EMFILE:
	case ENFILE:
		return CHIRP_ERROR_TOO_MANY_OPEN;
	case EBUSY:
		return CHIRP_ERROR_BUSY;
	case EAGAIN:
		return CHIRP_ERROR_TRY_AGAIN;
	case EBADF:
		return CHIRP_ERROR_BAD_FD;
	case EISDIR:
		return CHIRP_ERROR_IS_DIR;
	case ENOTDIR:
		return CHIRP_ERROR_NOT_DIR;
	case ENOTEMPTY:
		return CHIRP_ERROR_NOT_EMPTY;
	case EXDEV:
		return CHIRP_ERROR_CROSS_DEVICE_LINK;
	case EHOSTUNREACH:
		return CHIRP_ERROR_GRP_UNREACHABLE;
	case ESRCH:
		return CHIRP_ERROR_NO_SUCH_JOB;
	case ESPIPE:
		return CHIRP_ERROR_IS_A_PIPE;
	case ENAMETOOLONG:
		return CHIRP_ERROR_NAME_TOO_LONG;
	case ENOTSUP:
		return CHIRP_ERROR_NOT_SUPPORTED;
	default:
		debug(D_CHIRP, "zoiks, I don't know how to transform error %d (%s)\n", errno, strerror(errno));
		return CHIRP_ERROR_UNKNOWN;
	}
}

static INT64_T getstream(const char *path, struct link * l, time_t stoptime)
{
	INT64_T fd, total = 0;
	char buffer[65536];

	fd = cfs->open(path, O_RDONLY, S_IRWXU);
	if(fd == -1)
		return fd;

	link_putliteral(l, "0\n", stoptime);

	while(1) {
		INT64_T result;
		INT64_T actual;

		result = cfs->pread(fd, buffer, sizeof(buffer), total);
		if(result <= 0)
			break;

		actual = link_putlstring(l, buffer, result, stoptime);
		if(actual != result)
			break;

		total += actual;
	}

	cfs->close(fd);

	return total;
}

static INT64_T putstream(const char *path, struct link * l, time_t stoptime)
{
	INT64_T fd, total = 0;

	fd = cfs->open(path, O_CREAT | O_TRUNC | O_WRONLY, S_IRWXU);
	if(fd < 0) {
		return -1;
	}

	link_putliteral(l, "0\n", stoptime);

	while(1) {
		char buffer[65536];
		INT64_T streamed;

		streamed = link_read(l, buffer, sizeof(buffer), stoptime);
		if(streamed <= 0)
			goto failure;
		if(!space_available(streamed))
			goto failure;

		INT64_T current;
		if (chirp_alloc_frealloc(fd, total+streamed, &current) == 0) {
			INT64_T actual = cfs->pwrite(fd, buffer, streamed, total);
			if (actual == -1) {
				chirp_alloc_frealloc(fd, current, NULL);
				goto failure;
			} else if (actual < streamed) {
				chirp_alloc_frealloc(fd, actual, NULL);
				goto failure;
			}
			total += streamed;
		} else {
			goto failure;
		}
	}
	goto done;

failure:
	total = -1;
done:
	cfs->close(fd);

	return total;
}

static INT64_T rmall(const char *path)
{
	INT64_T result;
	struct chirp_stat info;

	if(root_quota == 0)
		return cfs->rmall(path);

	result = cfs->stat(path, &info);
	if(result == 0) {
		if(S_ISDIR(info.cst_mode)) {
			struct chirp_dir *dir;
			struct chirp_dirent *d;

			dir = cfs->opendir(path);
			if(dir) {
				while((d = cfs->readdir(dir))) {
					char subpath[CHIRP_PATH_MAX];

					if(strcmp(d->name, ".") == 0 || strcmp(d->name, "..") == 0 || strncmp(d->name, ".__", 3) == 0)
						continue;

					sprintf(subpath, "%s/%s", path, d->name);
					result = rmall(subpath);
					if(result != 0)
						break;
				}
				cfs->closedir(dir);

				if(result == 0) {
					result = cfs->rmdir(path);
				}
			} else {
				result = -1;
			}
		} else {
			INT64_T current;
			if ((result = chirp_alloc_realloc(path, 0, &current)) == 0) {
				result = cfs->unlink(path);
				if (result == -1) {
					chirp_alloc_realloc(path, current, NULL);
				}
			}
		}
	}

	return result;
}

static INT64_T getvarstring (struct link *l, time_t stalltime, void *buffer, INT64_T count, int soak_overflow)
{
	if (count < 0) {
		return errno = EINVAL, -1;
	} else if (!soak_overflow && count > MAX_BUFFER_SIZE) {
		link_soak(l, count, stalltime);
		return errno = ENOMEM, -1;
	}
	if (soak_overflow && count > MAX_BUFFER_SIZE) {
		if (link_read(l, buffer, MAX_BUFFER_SIZE, stalltime) != MAX_BUFFER_SIZE)
			return errno = EINVAL, -1;
		link_soak(l, count-MAX_BUFFER_SIZE, stalltime);
		return MAX_BUFFER_SIZE;
	} else {
		if (link_read(l, buffer, count, stalltime) != count)
			return errno = EINVAL, -1;
		return count;
	}
}

/* A note on integers:
 *
 * Various operating systems employ integers of different sizes for fields such
 * as file size, user identity, and so forth. Regardless of the operating
 * system support, the Chirp protocol must support integers up to 64 bits. So,
 * in the server handling loop, we treat all integers as INT64_T. What the
 * operating system does from there is out of our hands.
 */
static void chirp_handler(struct link *l, const char *addr, const char *subject)
{
	char *esubject;
	buffer_t B[1]; /* output buffer */
	void *buffer = xxmalloc(MAX_BUFFER_SIZE+1); /* general purpose temporary buffer w/ room for NUL */

	if(!chirp_acl_whoami(subject, &esubject))
		return;

	link_tune(l, LINK_TUNE_INTERACTIVE);

	buffer_init(B);
	buffer_abortonfailure(B, 1);
	buffer_max(B, MAX_BUFFER_SIZE+1 /* +1 for NUL */);
	while(1) {
		char line[CHIRP_LINE_MAX] = "";
		time_t idletime = time(0) + idle_timeout;
		time_t stalltime = time(0) + stall_timeout;

		INT64_T result = -1;

		INT64_T fd, length, flags, offset, uid, gid, mode, actime, modtime, stride_length, stride_skip;
		chirp_jobid_t id;
		char path[CHIRP_PATH_MAX] = "";
		char newpath[CHIRP_PATH_MAX] = "";
		char chararg1[CHIRP_LINE_MAX] = "";
		char chararg2[CHIRP_LINE_MAX] = "";

		buffer_rewind(B, 0);
		memset(buffer, 0, MAX_BUFFER_SIZE+1);

		if(chirp_alloc_flush_needed()) {
			if(!link_usleep(l, 1000000, 1, 0)) {
				chirp_alloc_flush();
			}
		}

		if(!link_readline(l, line, sizeof(line), idletime)) {
			debug(D_CHIRP, "timeout: client idle too long\n");
			goto die;
		}

		string_chomp(line);
		if(strlen(line) < 1)
			continue; /* back to while */
		if(line[0] == 4)
			goto die;

		chirp_stats_report(config_pipe[1], addr, subject, advertise_alarm);

		chirp_stats_update(1, 0, 0);

		// Simulate network latency
		if(sim_latency > 0) {
			struct timeval tv;
			tv.tv_sec = 0;
			tv.tv_usec = sim_latency;
			select(0, NULL, NULL, NULL, &tv);
		}

		debug(D_CHIRP, "%s", line);

		if(sscanf(line, "pread %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &offset) == 3) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			result = cfs->pread(fd, buffer, MIN(length, MAX_BUFFER_SIZE), offset);
			if(result > 0) {
				buffer_putlstring(B, buffer, result);
				chirp_stats_update(0, result, 0);
			}
		} else if(sscanf(line, "sread %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &stride_length, &stride_skip, &offset) == 5) {
			if (length < 0 || stride_length < 0 || stride_skip < 0) {
				errno = EINVAL;
				goto failure;
			}
			result = cfs->sread(fd, buffer, MIN(length, MAX_BUFFER_SIZE), stride_length, stride_skip, offset);
			if(result > 0) {
				buffer_putlstring(B, buffer, result);
				chirp_stats_update(0, result, 0);
			}
		} else if(sscanf(line, "pwrite %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &offset) == 3) {
			if ((length = getvarstring(l, stalltime, buffer, length, 1)) == -1)
				goto failure;

			INT64_T oldsize = cfs_fd_size(fd);
			if(oldsize == -1)
				goto failure;
			if(offset < 0) {
				errno = EINVAL;
				goto failure;
			}
			INT64_T newsize = MAX(length+offset, oldsize);

			if(!space_available(newsize-oldsize))
				goto failure;

			INT64_T current;
			if ((result = chirp_alloc_frealloc(fd, newsize, &current)) == 0) {
				result = cfs->pwrite(fd, buffer, length, offset);
				if (result == -1) {
					chirp_alloc_frealloc(fd, current, NULL);
				} else if (result < length) {
					chirp_alloc_frealloc(fd, result, NULL);
				}
			}
			if(result > 0)
				chirp_stats_update(0, 0, result);
		} else if(sscanf(line, "swrite %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &length, &stride_length, &stride_skip, &offset) == 5) {
			if ((length = getvarstring(l, stalltime, buffer, length, 1)) == -1)
				goto failure;

			INT64_T oldsize = cfs_fd_size(fd);
			if(oldsize == -1)
				goto failure;
			if(offset < 0 || oldsize < offset) {
				errno = EINVAL;
				goto failure;
			}

			/* FIXME space_available check is wrong */
			if(!space_available(length))
				goto failure;

			result = cfs->swrite(fd, buffer, length, stride_length, stride_skip, offset);
			if(result > 0) {
				chirp_stats_update(0, 0, result);
			}
		} else if(sscanf(line, "whoami %" SCNd64, &length) == 1) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			result = buffer_putlstring(B, esubject, MIN((size_t)length, strlen(esubject)));
		} else if(sscanf(line, "whoareyou %s %" SCNd64, chararg1, &length) == 2) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			result = chirp_reli_whoami(chararg1, buffer, MIN(length, MAX_BUFFER_SIZE), idletime);
			if(result > 0)
				result = buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "readlink %s %" SCNd64, path, &length) == 2) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			path_fix(path);
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->readlink(path, buffer, MIN(length, MAX_BUFFER_SIZE));
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "getlongdir %s", path) == 1) {
			path_fix(path);
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
				goto failure;

			struct chirp_dir *dir = cfs->opendir(path);
			if(dir) {
				struct chirp_dirent *d;
				link_putliteral(l, "0\n", stalltime);
				while((d = cfs->readdir(dir))) {
					if(!strncmp(d->name, ".__", 3))
						continue;
					chirp_stat_encode(B, &d->info);
					link_printf(l, stalltime, "%s\n%s\n", d->name, buffer_tostring(B));
					buffer_rewind(B, 0);
				}
				cfs->closedir(dir);
				link_putliteral(l, "\n", stalltime);
				result = 0;
				goto done;
			} else {
				goto failure;
			}
		} else if(sscanf(line, "getdir %s", path) == 1) {
			path_fix(path);
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
				goto failure;

			struct chirp_dir *dir = cfs->opendir(path);
			if(dir) {
				struct chirp_dirent *d;
				link_putliteral(l, "0\n", stalltime);
				while((d = cfs->readdir(dir))) {
					if(!strncmp(d->name, ".__", 3))
						continue;
					link_printf(l, stalltime, "%s\n", d->name);
				}
				cfs->closedir(dir);
				link_putliteral(l, "\n", stalltime);
				result = 0;
				goto done;
			} else {
				goto failure;
			}
		} else if(sscanf(line, "getacl %s", path) == 1) {
			CHIRP_FILE *aclfile;

			path_fix(path);

			// Previously, the LIST right was necessary to view the ACL.
			// However, this has caused much confusion with debugging permissions problems.
			// As an experiment, let's trying making getacl accessible to everyone.
			// if(!chirp_acl_check_dir(path,subject,CHIRP_ACL_LIST)) goto failure;

			aclfile = chirp_acl_open(path);
			if(aclfile) {
				char aclsubject[CHIRP_LINE_MAX];
				int aclflags;
				while(chirp_acl_read(aclfile, aclsubject, &aclflags)) {
					buffer_putfstring(B, "%s %s\n", aclsubject, chirp_acl_flags_to_text(aclflags));
				}
				chirp_acl_close(aclfile);
				buffer_putliteral(B, "\n");
				result = 0;
			} else {
				goto failure;
			}
		} else if(sscanf(line, "getfile %s", path) == 1) {
			path_fix(path);
			if(!cfs_isnotdir(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;

			INT64_T fd = cfs->open(path, O_RDONLY, 0);
			if (fd == -1)
				goto failure;

			struct chirp_stat info;
			if (cfs->fstat(fd, &info) == -1) {
				int saved = errno;
				cfs->close(fd);
				errno = saved;
				goto failure;
			}

			if (S_ISDIR(info.cst_mode)) {
				cfs->close(fd);
				errno = EISDIR;
				goto failure;
			}

			length = info.cst_size;

			time_t transmission_stalltime = time(NULL)+(length/1024)+30; /* 1KB/s minimum */
			transmission_stalltime = MAX(stalltime, transmission_stalltime);

			link_printf(l, transmission_stalltime, "%" PRId64 "\n", length);

			INT64_T total = 0;
			while (total < length) {
				char b[65536];
				size_t chunk = MIN(sizeof(b), (size_t)(length-total));

				INT64_T ractual = cfs->pread(fd, b, chunk, total);
				if(ractual <= 0)
					break;

				if(link_putlstring(l, b, ractual, transmission_stalltime) == -1) {
					debug(D_DEBUG, "getfile: write failed (%s), expected to write %" PRId64 " more bytes", strerror(errno), length);
					break;
				}

				total += ractual;
			}
			cfs->close(fd);

			chirp_stats_update(0, total, 0);
			result = total;
			goto done;
		} else if(sscanf(line, "putfile %s %" SCNd64 " %" SCNd64, path, &mode, &length) == 3) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}

			path_fix(path);
			if(!cfs_isnotdir(path))
				goto failure;

			flags = O_CREAT|O_WRONLY;

			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
					flags |= O_EXCL;
				} else {
					goto failure;
				}
			}

			fd = cfs->open(path, flags, mode);
			if (fd < 0)
				goto failure;

			struct chirp_stat info;
			if (cfs->fstat(fd, &info) == -1) {
				int saved = errno;
				cfs->close(fd);
				errno = saved;
				goto failure;
			}

			if(!space_available(length - info.cst_size)) {
				int saved = errno;
				cfs->close(fd);
				errno = saved;
				goto failure;
			}

			INT64_T current;
			if (chirp_alloc_realloc(path, length, &current) == -1) {
				int saved = errno;
				cfs->close(fd);
				errno = saved;
				goto failure;
			}

			if (cfs->ftruncate(fd, 0) == -1) {
				int saved = errno;
				chirp_alloc_realloc(path, current, NULL);
				cfs->close(fd);
				errno = saved;
				goto failure;
			}

			time_t transmission_stalltime = time(NULL)+(length/1024)+30; /* 1KB/s minimum */
			transmission_stalltime = MAX(stalltime, transmission_stalltime);

			link_putliteral(l, "0\n", transmission_stalltime);

			INT64_T total = 0;
			while (total < length) {
				char b[65536];
				size_t chunk = MIN(sizeof(b), (size_t)(length-total));

				INT64_T ractual = link_read(l, b, chunk, transmission_stalltime);

				INT64_T wactual = -1;
				if (ractual > 0)
					wactual = cfs->pwrite(fd, b, ractual, total);

				if(ractual <= 0 || wactual < ractual) {
					int saved = errno;
					if (ractual <= 0)
						debug(D_DEBUG, "putfile: socket read failed (%s), expected %" PRId64 " more bytes", strerror(errno), length-total);
					else
						debug(D_DEBUG, "putfile: file write failed: (%s)", strerror(errno));
					cfs->close(fd);
					if(cfs->unlink(path) == -1)
						debug(D_DEBUG, "putfile: failed to unlink remnant file '%s': %s", path, strerror(errno));
					chirp_alloc_realloc(path, 0, NULL);
					link_soak(l, length - total - MAX(ractual, 0), transmission_stalltime);
					errno = saved;
					goto failure;
				}

				total += ractual;
			}

			chirp_stats_update(0, 0, total);

			if (cfs->close(fd) == -1) {
				/* Confuga does O_EXCL check at close. */
				if (errno == EEXIST) {
					chirp_alloc_realloc(path, current, NULL); /* restore current, nothing was ever changed */
					errno = EEXIST;
				}
				goto failure;
			}
			result = total;
		} else if(sscanf(line, "getstream %s", path) == 1) {
			path_fix(path);
			if(!cfs_isnotdir(path))
				goto failure;
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;

			result = getstream(path, l, stalltime);
			if(result >= 0) {
				chirp_stats_update(0, result, 0);
				debug(D_CHIRP, "= %" SCNd64 " bytes streamed\n", result);
				/* getstream indicates end by closing the connection */
				goto die;
			}
		} else if(sscanf(line, "putstream %s", path) == 1) {
			path_fix(path);
			if(!cfs_isnotdir(path))
				goto failure;

			if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				/* writable, ok to proceed */
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
				if(cfs_exists(path)) {
					errno = EEXIST;
					goto failure;
				} else {
					/* ok to proceed */
				}
			} else {
				goto failure;
			}

			result = putstream(path, l, stalltime);
			if(result >= 0) {
				chirp_stats_update(0, 0, result);
				debug(D_CHIRP, "= %" SCNd64 " bytes streamed\n", result);
				/* putstream indicates end by closing the connection */
				goto die;
			}
		} else if(sscanf(line, "thirdput %s %s %s", path, chararg1, newpath) == 3) {
			const char *hostname = chararg1;
			path_fix(path);
			if (cfs == &chirp_fs_confuga) {
				/* Confuga cannot support thirdput because of auth problems,
				 * see Authentication comment in chirp_receive.
				 */
				errno = EACCES;
				goto failure;
			}
			/* ACL check will occur inside of chirp_thirdput */
			result = chirp_thirdput(subject, path, hostname, newpath, stalltime);
		} else if(sscanf(line, "open %s %s %" SCNd64, path, newpath, &mode) == 3) {
			flags = 0;

			if(strchr(newpath, 'r')) {
				if(strchr(newpath, 'w')) {
					flags = O_RDWR;
				} else {
					flags = O_RDONLY;
				}
			} else if(strchr(newpath, 'w')) {
				flags = O_WRONLY;
			}

			if(strchr(newpath, 'c'))
				flags |= O_CREAT;
			if(strchr(newpath, 't'))
				flags |= O_TRUNC;
			if(strchr(newpath, 'a'))
				flags |= O_APPEND;
			if(strchr(newpath, 'x'))
				flags |= O_EXCL;
#ifdef O_SYNC
			if(strchr(newpath, 's'))
				flags |= O_SYNC;
#endif

			path_fix(path);

			/*
			   This is a little strange.
			   For ordinary files, we check the ACL according
			   to the flags passed to open.  For some unusual
			   cases in Unix, we must also allow open()  for
			   reading on a directory, otherwise we fail
			   with EISDIR.
			 */

			if(cfs_isnotdir(path)) {
				if(chirp_acl_check(path, subject, chirp_acl_from_open_flags(flags))) {
					/* ok to proceed */
				} else if(chirp_acl_check(path, subject, CHIRP_ACL_PUT)) {
					if(flags & O_CREAT) {
						if(cfs_exists(path)) {
							errno = EEXIST;
							goto failure;
						} else {
							/* ok to proceed */
						}
					} else {
						errno = EACCES;
						goto failure;
					}
				} else {
					goto failure;
				}
			} else if(flags == O_RDONLY) {
				if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_LIST))
					goto failure;
			} else {
				errno = EISDIR;
				goto failure;
			}

			if (flags & O_TRUNC) {
				INT64_T current;
				if ((result = chirp_alloc_realloc(path, 0, &current)) == 0) {
					result = cfs->open(path, flags, (int) mode);
					if (result == -1) {
						chirp_alloc_realloc(path, current, NULL);
					}
				}
			} else {
				result = cfs->open(path, flags, (int) mode);
			}
			if(result >= 0) {
				struct chirp_stat info;
				cfs->fstat(result, &info);
				chirp_stat_encode(B, &info);
				buffer_putliteral(B, "\n");
			}
		} else if(sscanf(line, "close %" SCNd64, &fd) == 1) {
			result = cfs->close(fd);
		} else if(sscanf(line, "fchmod %" SCNd64 " %" SCNd64, &fd, &mode) == 2) {
			result = cfs->fchmod(fd, mode);
		} else if(sscanf(line, "fchown %" SCNd64 " %" SCNd64 " %" SCNd64, &fd, &uid, &gid) == 3) {
			result = 0;
		} else if(sscanf(line, "fsync %" SCNd64, &fd) == 1) {
			result = cfs->fsync(fd);
		} else if(sscanf(line, "ftruncate %" SCNd64 " %" SCNd64, &fd, &length) == 2) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}

			if(!space_available(length))
				goto failure;

			INT64_T current;
			if ((result = chirp_alloc_frealloc(fd, length, &current)) == 0) {
				result = cfs->ftruncate(fd, length);
				if (result == -1) {
					chirp_alloc_frealloc(fd, current, NULL);
				}
				if(result >= 0) {
					chirp_stats_update(0, 0, length);
				}
			}
		} else if(sscanf(line, "fgetxattr %" SCNd64 " %s", &fd, chararg1) == 2) {
			result = cfs->fgetxattr(fd, chararg1, buffer, MAX_BUFFER_SIZE);
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "flistxattr %" SCNd64, &fd) == 1) {
			result = cfs->flistxattr(fd, buffer, MAX_BUFFER_SIZE);
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "fsetxattr %" SCNd64 " %s %" SCNd64 " %" SCNd64, &fd, chararg1, &length, &flags) == 4) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			if(!space_available(length))
				goto failure;
			result = cfs->fsetxattr(fd, chararg1, buffer, length, flags);
			if(result > 0)
				chirp_stats_update(0, 0, result);
		} else if(sscanf(line, "fremovexattr %" SCNd64 " %s", &fd, chararg1) == 2) {
			result = cfs->fremovexattr(fd, chararg1);
		} else if(sscanf(line, "unlink %s", path) == 1) {
			path_fix(path);
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				INT64_T current;
				if ((result = chirp_alloc_realloc(path, 0, &current)) == 0) {
					result = cfs->unlink(path);
					if (result == -1) {
						chirp_alloc_realloc(path, current, NULL);
					}
					if(result >= 0) {
						chirp_stats_update(0, 0, current);
					}
				}
			} else {
				goto failure;
			}
		} else if(sscanf(line, "access %s %" SCNd64, path, &flags) == 2) {
			path_fix(path);
			int chirp_flags = chirp_acl_from_access_flags(flags);
			/* If filename is a directory, then we change execute flags to list flags. */
			if(cfs_isdir(path) && (chirp_flags & CHIRP_ACL_EXECUTE)) {
				chirp_flags ^= CHIRP_ACL_EXECUTE;	/* remove execute flag */
				chirp_flags |= CHIRP_ACL_LIST;	/* change to list */
			}
			if(!chirp_acl_check(path, subject, chirp_flags))
				goto failure;
			result = cfs->access(path, flags);
		} else if(sscanf(line, "chmod %s %" SCNd64, path, &mode) == 2) {
			path_fix(path);
			if(chirp_acl_check_dir(path, subject, CHIRP_ACL_WRITE) || chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = cfs->chmod(path, mode);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "chown %s %" SCNd64 " %" SCNd64, path, &uid, &gid) == 3) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = 0;
		} else if(sscanf(line, "lchown %s %" SCNd64 " %" SCNd64, path, &uid, &gid) == 3) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = 0;
		} else if(sscanf(line, "truncate %s %" SCNd64, path, &length) == 2) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			if(!space_available(length))
				goto failure;
			INT64_T current;
			if ((result = chirp_alloc_realloc(path, length, &current)) == 0) {
				result = cfs->truncate(path, length);
				if (result == -1) {
					chirp_alloc_realloc(path, current, NULL);
				}
				if(result >= 0) {
					chirp_stats_update(0, 0, length);
				}
			}
		} else if(sscanf(line, "rename %s %s", path, newpath) == 2) {
			path_fix(path);
			path_fix(newpath);
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_READ | CHIRP_ACL_DELETE))
				goto failure;
			if(!chirp_acl_check(newpath, subject, CHIRP_ACL_WRITE))
				goto failure;
			INT64_T newcurrent;
			INT64_T oldcurrent;
			if ((result = chirp_alloc_realloc(path, 0, &oldcurrent)) == 0) {
				if ((result = chirp_alloc_realloc(newpath, cfs_file_size(path), &newcurrent)) == 0) {
					result = cfs->rename(path, newpath);
					if (result == -1) {
						chirp_alloc_realloc(path, oldcurrent, NULL);
						chirp_alloc_realloc(newpath, newcurrent, NULL);
					}
				} else {
					chirp_alloc_realloc(path, oldcurrent, NULL);
				}
			}
		} else if(sscanf(line, "getxattr %s %s", path, chararg1) == 2) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->getxattr(path, chararg1, buffer, MAX_BUFFER_SIZE);
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "lgetxattr %s %s", path, chararg1) == 2) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->lgetxattr(path, chararg1, buffer, MAX_BUFFER_SIZE);
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "listxattr %s", path) == 1) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->listxattr(path, buffer, MAX_BUFFER_SIZE);
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "llistxattr %s", path) == 1) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->llistxattr(path, buffer, MAX_BUFFER_SIZE);
			if(result > 0)
				buffer_putlstring(B, buffer, result);
		} else if(sscanf(line, "setxattr %s %s %" SCNd64 " %" SCNd64, path, chararg1, &length, &flags) == 4) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			if(!space_available(length))
				goto failure;
			result = cfs->setxattr(path, chararg1, buffer, length, flags);
			if(result > 0)
				chirp_stats_update(0, 0, result);
		} else if(sscanf(line, "lsetxattr %s %s %" SCNd64 " %" SCNd64, path, chararg1, &length, &flags) == 4) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			if(!space_available(length))
				goto failure;
			result = cfs->lsetxattr(path, chararg1, buffer, length, flags);
			if(result > 0)
				chirp_stats_update(0, 0, result);
		} else if(sscanf(line, "removexattr %s %s", path, chararg1) == 2) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = cfs->removexattr(path, chararg1);
		} else if(sscanf(line, "lremovexattr %s %s", path, chararg1) == 2) {
			path_fix(path);
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = cfs->lremovexattr(path, chararg1);
		} else if(sscanf(line, "link %s %s", path, newpath) == 2) {
			/* Can only hard link to files on which you already have r/w perms */
			path_fix(path);
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_READ | CHIRP_ACL_WRITE))
				goto failure;
			path_fix(newpath);
			if(!chirp_acl_check(newpath, subject, CHIRP_ACL_WRITE))
				goto failure;
			if(root_quota > 0) {
				errno = EPERM;
				goto failure;
			}
			result = cfs->link(path, newpath);
		} else if(sscanf(line, "symlink %s %s", path, newpath) == 2) {
			/* Note that the link target (path) may be any arbitrary data. */
			/* Access permissions are checked when data is actually accessed. */
			path_fix(newpath);
			if(!chirp_acl_check(newpath, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = cfs->symlink(path, newpath);
		} else if(sscanf(line, "setacl %s %s %s", path, chararg1, chararg2) == 3) {
			path_fix(path);
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_ADMIN))
				goto failure;
			result = chirp_acl_set(path, chararg1, chirp_acl_text_to_flags(chararg2), 0);
		} else if(sscanf(line, "resetacl %s %s", path, chararg1) == 2) {
			path_fix(path);
			if(!chirp_acl_check_dir(path, subject, CHIRP_ACL_ADMIN))
				goto failure;
			result = chirp_acl_set(path, subject, chirp_acl_text_to_flags(chararg1) | CHIRP_ACL_ADMIN, 1);
		} else if(sscanf(line, "ticket_register %s %s %" SCNd64, chararg1, chararg2, &length) == 3) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			char *newsubject = chararg1;
			char *duration = chararg2;
			if(strcmp(newsubject, "self") == 0)
				strcpy(newsubject, esubject);
			if(strcmp(esubject, newsubject) != 0 && strcmp(esubject, chirp_super_user) != 0) {	/* must be superuser to create a ticket for someone else */
				errno = EACCES;
				goto failure;
			}
			result = chirp_acl_ticket_create(subject, newsubject, buffer, duration);
		} else if(sscanf(line, "ticket_delete %s", chararg1) == 1) {
			result = chirp_acl_ticket_delete(subject, chararg1);
		} else if(sscanf(line, "ticket_modify %s %s %s", chararg1, path, chararg2) == 3) {
			path_fix(path);
			result = chirp_acl_ticket_modify(subject, chararg1, path, chirp_acl_text_to_flags(chararg2));
		} else if(sscanf(line, "ticket_get %s", chararg1) == 1) {
			/* ticket_subject is ticket:MD5SUM */
			char *ticket_esubject;
			char *ticket;
			time_t expiration;
			char **ticket_rights;
			result = chirp_acl_ticket_get(subject, chararg1, &ticket_esubject, &ticket, &expiration, &ticket_rights);
			if(result == 0) {
				buffer_putfstring(B, "%zu\n%s%zu\n%s%llu\n", strlen(ticket_esubject), ticket_esubject, strlen(ticket), ticket, (unsigned long long) expiration);
				free(ticket_esubject);
				free(ticket);
				char **tr = ticket_rights;
				for(; tr[0] && tr[1]; tr += 2) {
					buffer_putfstring(B, "%s %s\n", tr[0], tr[1]);
					free(tr[0]);
					free(tr[1]);
				}
				buffer_putliteral(B, "0\n");
				free(ticket_rights);
			}
		} else if(sscanf(line, "ticket_list %s", chararg1) == 1) {
			/* ticket_subject is the owner of the ticket, not ticket:MD5SUM */
			char **ticket_subjects;
			if(strcmp(chararg1, "self") == 0)
				strcpy(chararg1, esubject);
			int super = strcmp(subject, chirp_super_user) == 0;	/* note subject instead of esubject; super user must be authenticated as himself */
			if(!super && strcmp(chararg1, esubject) != 0) {
				errno = EACCES;
				goto failure;
			}
			result = chirp_acl_ticket_list(subject, &ticket_subjects);
			if(result == 0) {
				char **ts = ticket_subjects;
				for(; ts && ts[0]; ts++) {
					buffer_putfstring(B, "%zu\n%s", strlen(ts[0]), ts[0]);
					free(ts[0]);
				}
				buffer_putliteral(B, "0\n");
				free(ticket_subjects);
			}
		} else if(sscanf(line, "mkdir %s %" SCNd64, path, &mode) == 2) {
			path_fix(path);
			if(chirp_acl_check(path, subject, CHIRP_ACL_RESERVE)) {
				result = cfs->mkdir(path, mode);
				if(result == 0) {
					if(chirp_acl_init_reserve(path, subject)) {
						result = 0;
					} else {
						cfs->rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = cfs->mkdir(path, mode);
				if(result == 0) {
					if(chirp_acl_init_copy(path)) {
						result = 0;
					} else {
						cfs->rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(cfs_isdir(path)) {
				errno = EEXIST;
				goto failure;
			} else {
				errno = EACCES;
				goto failure;
			}
		} else if(sscanf(line, "rmdir %s", path) == 1) {
			path_fix(path);
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				/* rmdir only works if the directory is user-visibly empty, and we don't track allocations for empty directories */
				result = cfs->rmdir(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "rmall %s", path) == 1) {
			path_fix(path);
			if(chirp_acl_check_link(path, subject, CHIRP_ACL_DELETE) || chirp_acl_check_dir(path, subject, CHIRP_ACL_DELETE)) {
				result = rmall(path);
			} else {
				goto failure;
			}
		} else if(sscanf(line, "utime %s %" SCNd64 " %" SCNd64, path, &actime, &modtime) == 3) {
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = cfs->utime(path, actime, modtime);
		} else if(sscanf(line, "fstat %" SCNd64, &fd) == 1) {
			struct chirp_stat info;
			result = cfs->fstat(fd, &info);
			if (result >= 0) {
				chirp_stat_encode(B, &info);
				buffer_putliteral(B, "\n");
			}
		} else if(sscanf(line, "fstatfs %" SCNd64, &fd) == 1) {
			struct chirp_statfs info;
			result = chirp_alloc_fstatfs(fd, &info);
			if (result >= 0) {
				chirp_statfs_encode(B, &info);
				buffer_putliteral(B, "\n");
			}
		} else if(sscanf(line, "statfs %s", path) == 1) {
			struct chirp_statfs info;
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_statfs(path, &info);
			if (result >= 0) {
				chirp_statfs_encode(B, &info);
				buffer_putliteral(B, "\n");
			}
		} else if(sscanf(line, "stat %s", path) == 1) {
			struct chirp_stat info;
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = cfs->stat(path, &info);
			if (result >= 0) {
				chirp_stat_encode(B, &info);
				buffer_putliteral(B, "\n");
			}
		} else if(sscanf(line, "lstat %s", path) == 1) {
			struct chirp_stat info;
			path_fix(path);
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = cfs->lstat(path, &info);
			if (result >= 0) {
				chirp_stat_encode(B, &info);
				buffer_putliteral(B, "\n");
			}
		} else if(sscanf(line, "lsalloc %s", path) == 1) {
			INT64_T size, inuse;
			path_fix(path);
			if(!chirp_acl_check_link(path, subject, CHIRP_ACL_LIST))
				goto failure;
			result = chirp_alloc_lsalloc(path, newpath, &size, &inuse);
			if(result >= 0) {
				assert(newpath[0]);
				buffer_putfstring(B, "%s %" PRId64 " %" PRId64 "\n", newpath, size, inuse);
			}
		} else if(sscanf(line, "mkalloc %s %" SCNd64 " %" SCNd64, path, &length, &mode) == 3) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			path_fix(path);
			if(chirp_acl_check(path, subject, CHIRP_ACL_RESERVE)) {
				result = chirp_alloc_mkalloc(path, length, mode);
				if(result == 0) {
					if(chirp_acl_init_reserve(path, subject)) {
						result = 0;
					} else {
						cfs->rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else if(chirp_acl_check(path, subject, CHIRP_ACL_WRITE)) {
				result = chirp_alloc_mkalloc(path, length, mode);
				if(result == 0) {
					if(chirp_acl_init_copy(path)) {
						result = 0;
					} else {
						cfs->rmdir(path);
						errno = EACCES;
						goto failure;
					}
				}
			} else {
				goto failure;
			}
		} else if(sscanf(line, "localpath %s", path) == 1) {
			struct chirp_stat info;
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_LIST) && !chirp_acl_check(path, "system:localuser", CHIRP_ACL_LIST))
				goto failure;
			result = cfs->stat(path, &info);
			if(result == 0) {
				result = buffer_putstring(B, path);
			}
		} else if(sscanf(line, "audit %s", path) == 1) {
			struct hash_table *table;

			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_ADMIN))
				goto failure;

			table = chirp_audit(path);
			if(table) {
				char *key;
				struct chirp_audit *entry;

				link_printf(l, stalltime, "%d\n", hash_table_size(table));
				hash_table_firstkey(table);
				while(hash_table_nextkey(table, &key, (void *) &entry)) {
					link_printf(l, stalltime, "%s %" PRId64 " %" PRId64 " %" PRId64 "\n", key, entry->nfiles, entry->ndirs, entry->nbytes);
				}
				chirp_audit_delete(table);
				result = 0;
				goto done;
			} else {
				goto failure;
			}
		} else if(sscanf(line, "md5 %s", path) == 1) {
			/* backwards compatibility */
			unsigned char digest[CHIRP_DIGEST_MAX];
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->hash(path, "md5", digest);
			if (result >= 0) {
				buffer_putlstring(B, (char *)digest, result);
			} else {
				result = errno_to_chirp(errno);
			}
		} else if(sscanf(line, "hash %s %s", chararg1, path) == 2) {
			unsigned char digest[CHIRP_DIGEST_MAX];
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_READ))
				goto failure;
			result = cfs->hash(path, chararg1, digest);
			if (result >= 0) {
				buffer_putlstring(B, (char *)digest, result);
			} else {
				result = errno_to_chirp(errno);
			}
		} else if(sscanf(line, "setrep %s %" SCNd64, path, &length) == 2) {
			if (length < 0) {
				errno = EINVAL;
				goto failure;
			}
			path_fix(path);
			if(!chirp_acl_check(path, subject, CHIRP_ACL_WRITE))
				goto failure;
			result = cfs->setrep(path, length);
		} else if(sscanf(line, "debug %s", chararg1) == 1) {
			if(strcmp(esubject, chirp_super_user) != 0) {
				errno = EPERM;
				goto failure;
			}
			result = 0;
			// send this message to the parent for processing.
			strcat(line, "\n");
			write(config_pipe[1], line, strlen(line));
			debug_flags_set(chararg1);
		} else if(sscanf(line, "search %s %s %" PRId64, chararg1, path, &flags) == 3) {
			link_putliteral(l, "0\n", stalltime);
			char *start = path;
			const char *pattern = chararg1;

			for(;;) {
				char fixed[CHIRP_PATH_MAX];
				char *end;
				if((end = strchr(start, CHIRP_SEARCH_DELIMITER)) != NULL)
					*end = '\0';

				strcpy(fixed, start);
				path_fix(fixed);

				if(cfs->access(fixed, F_OK) == -1) {
					link_printf(l, stalltime, "%d:%d:%s:\n", ENOENT, CHIRP_SEARCH_ERR_OPEN, fixed);
				} else if(!chirp_acl_check(fixed, subject, CHIRP_ACL_WRITE)) {
					link_printf(l, stalltime, "%d:%d:%s:\n", EPERM, CHIRP_SEARCH_ERR_OPEN, fixed);
				} else {
					int found = cfs->search(subject, fixed, pattern, flags, l, stalltime);
					if(found && (flags & CHIRP_SEARCH_STOPATFIRST))
						break;
				}

				if(end != NULL) {
					start = end + 1;
					*end = CHIRP_SEARCH_DELIMITER;
				} else {
					break;
				}
			}
			link_putliteral(l, "\n", stalltime);
			goto done;
		} else if(sscanf(line, "job_create %" PRId64, &length) == 1) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			debug(D_CHIRP, "--> job_create `%.*s'", (int)length, (char *)buffer);
			json_value *J = json_parse(buffer, length);
			if (J) {
				result = chirp_job_create(&id, J, esubject);
				json_value_free(J);
				if (result) {
					errno = result;
					goto failure;
				}
				result = id;
			} else {
				debug(D_DEBUG, "does not parse as json!");
				errno = EINVAL;
				goto failure;
			}
		} else if(sscanf(line, "job_commit %" PRId64, &length) == 1) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			debug(D_CHIRP, "--> job_commit `%.*s'", (int)length, (char *)buffer);
			json_value *J = json_parse(buffer, length);
			if (J) {
				result = chirp_job_commit(J, esubject);
				json_value_free(J);
				if (result) {
					errno = result;
					goto failure;
				}
			} else {
				debug(D_DEBUG, "does not parse as json!");
				errno = EINVAL;
				goto failure;
			}
		} else if(sscanf(line, "job_kill %" PRId64, &length) == 1) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			debug(D_DEBUG, "--> job_kill `%.*s'", (int)length, (char *)buffer);
			json_value *J = json_parse(buffer, length);
			if (J) {
				result = chirp_job_kill(J, esubject);
				json_value_free(J);
				if (result) {
					errno = result;
					goto failure;
				}
			} else {
				debug(D_DEBUG, "does not parse as json!");
				errno = EINVAL;
				goto failure;
			}
		} else if(sscanf(line, "job_status %" PRId64, &length) == 1) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			debug(D_CHIRP, "--> job_status `%.*s'", (int)length, (char *)buffer);
			json_value *J = json_parse(buffer, length);
			if (J) {
				result = chirp_job_status(J, esubject, B);
				if (result) {
					errno = result;
					goto failure;
				} else {
					result = buffer_pos(B);
				}
				json_value_free(J);
			} else {
				debug(D_DEBUG, "does not parse as json!");
				errno = EINVAL;
				goto failure;
			}
		} else if(sscanf(line, "job_wait %" SCNCHIRP_JOBID_T " %" SCNd64, &id, &length) == 2) {
			result = chirp_job_wait(id, esubject, length, B);
			if (result) {
				errno = result;
				goto failure;
			} else {
				result = buffer_pos(B);
			}
		} else if(sscanf(line, "job_reap %" PRId64, &length) == 1) {
			if ((length = getvarstring(l, stalltime, buffer, length, 0)) == -1)
				goto failure;
			debug(D_DEBUG, "--> job_reap `%.*s'", (int)length, (char *)buffer);
			json_value *J = json_parse(buffer, length);
			if (J) {
				result = chirp_job_reap(J, esubject);
				json_value_free(J);
				if (result) {
					errno = result;
					goto failure;
				}
			} else {
				debug(D_DEBUG, "does not parse as json!");
				errno = EINVAL;
				goto failure;
			}
		} else {
			errno = ENOSYS;
			goto failure;
		}
		goto result;

failure:
		result = -1;
result:
		if (result < 0)
			result = errno_to_chirp(errno);
		if (link_printf(l, stalltime, "%" PRId64 "\n", result) == -1)
			goto die;
		if(result >= 0 && buffer_pos(B)) {
			if (link_putlstring(l, buffer_tostring(B), buffer_pos(B), stalltime) == -1)
				goto die;
		}

done:
		if (result < 0)
			debug(D_CHIRP, "= %" PRId64 " (%s)", result, strerror(errno));
		else
			debug(D_CHIRP, "= %" PRId64, result);
	}
die:
	buffer_free(B);
	free(esubject);
	free(buffer);
}

static void chirp_receive(struct link *link, char url[CHIRP_PATH_MAX])
{
	char *atype, *asubject;
	char typesubject[AUTH_TYPE_MAX + AUTH_SUBJECT_MAX];
	char addr[LINK_ADDRESS_MAX];
	int port;

	link_address_remote(link, addr, &port);

	change_process_title("chirp_server [%s:%d] [backend starting]", addr, port, typesubject);

	/* Authentication problems:
	 *
	 * Confuga and the thirdput RPC both use the auth module when acting as
	 * Chirp clients. This conflicts with the Chirp server's authentication
	 * (auth_accept, below) because auth uses static data structures for both
	 * the client and server. So, we need to separate them somehow. Ideally, we
	 * would have an auth context that is passed around for all operations
	 * involving authentication, including the chirp_reli API. Unfortunately
	 * this would be a very invasive change and we have to account for two
	 * different clients (Confuga and chirp_thirdput) both using the chirp_reli
	 * interface but should be using different authentication systems on
	 * connect. Additionally, one's connection to one Chirp server should be
	 * separate from another, as they use different authentication mechanisms.
	 *
	 * The intermediate solution is to disable thirdput for Confuga and add a
	 * simple "clone" and "swap" method for auth which allows us to switch
	 * between client and server auth. This is still somewhat messy because
	 * ticket authentication requires looking up tickets in the backend file
	 * system. Fortunately, these are metadata files in Confuga so it does not
	 * require talking to a storage node.
	 */
	struct auth_state *server_state = auth_clone();

	/* Chirp's backend file system must be loaded here. HDFS loads in the JVM
	 * which does not play nicely with fork. So, we only manipulate the backend
	 * file system in a child process which actually handles client requests.
	 *
	 * XXX Downgrade permissions *after auth*. [This is actually a nasty hack
	 * as we should not make files as root in the backend. Fortunately, for
	 * now, the initial bootstrap backend_setup does necessary ACL/etc.
	 * creation so we should only ever read files between now and downgrade
	 * (below).
	 */
	backend_setup(url);

	change_process_title("chirp_server [%s:%d] [authenticating]", addr, port, typesubject);

	struct auth_state *backend_state = auth_clone();
	auth_replace(server_state);

	auth_ticket_server_callback(chirp_acl_ticket_callback);

	if(auth_accept(link, &atype, &asubject, time(0) + idle_timeout)) {
		auth_replace(backend_state);

		sprintf(typesubject, "%s:%s", atype, asubject);
		free(atype);
		free(asubject);

		debug(D_LOGIN, "%s from %s:%d", typesubject, addr, port);

		downgrade(); /* downgrade privileges after authentication */

		/* See above comment concerning authentication. */
		if (cfs != &chirp_fs_confuga) {
			/* Enable only globus, hostname, and address authentication for third-party transfers. */
			auth_clear();
			if(auth_globus_has_delegated_credential()) {
				auth_globus_use_delegated_credential(1);
				auth_globus_register();
			}
			auth_hostname_register();
			auth_address_register();
		}

		change_process_title("chirp_server [%s:%d] [%s]", addr, port, typesubject);

		chirp_handler(link, addr, typesubject);
		chirp_alloc_flush();
		chirp_stats_report(config_pipe[1], addr, typesubject, 0);

		debug(D_LOGIN, "disconnected");
	} else {
		auth_free(backend_state);
		debug(D_LOGIN, "authentication failed from %s:%d", addr, port);
	}

	link_close(link);

	cfs->destroy();
}

void killeveryone (int sig)
{
	int i;
	int n = sysconf(_SC_OPEN_MAX);

	/* This process sleeps for a short time in between kills, immediately close
	 * all fds to free up (bound) sockets.
	 */
	for (i = 0; i < n; i++)
		close(i);

	/* start with sig */
	kill(0, sig);
	sleep(1);
	kill(0, SIGTERM);
	sleep(1);
	kill(0, SIGQUIT);
	sleep(1);
	kill(0, SIGKILL);
	_exit(EXIT_FAILURE);
}

void shutdown_clean(int sig)
{
	sigset_t set;
	struct sigaction act;
	pid_t pid;

	pid = fork(); /* how perverse */
	if (pid == 0) {
		return killeveryone(sig);
	} else if (pid == -1) {
		return killeveryone(sig); /* do it ourselves */
	}

	/* Now we want Chirp to terminate due to signal delivery. A good reason
	 * for this is proper termination status to the parent. Additionally, some
	 * signals (should) generate a core dump.
	 */
	act.sa_handler = SIG_DFL;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	sigaction(sig, &act, NULL);
	sigemptyset(&set);
	sigaddset(&set, sig);
	sigprocmask(SIG_UNBLOCK, &set, NULL);
	raise(sig); /* this should kill us */
	_exit(EXIT_FAILURE); /* if it does not... */
}

static void install_handler(int sig, void (*handler) (int sig))
{
	struct sigaction s;
	s.sa_handler = handler;
	sigfillset(&s.sa_mask); /* block all signals during handler execution */
	s.sa_flags = 0;
	sigaction(sig, &s, 0);
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "use: %s [options]\n", cmd);
	fprintf(stdout, "The most common options are:\n");
	fprintf(stdout, " %-30s URL of storage directory, like `file://path' or `hdfs://host:port/path'.\n", "-r,--root=<url>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<name>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Send status updates to this host. (default: `%s')\n", "-u,--advertise=<host>", CATALOG_HOST);
	fprintf(stdout, " %-30s Show version info.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
	fprintf(stdout, "\n");
	fprintf(stdout, "Less common options are:\n");
	fprintf(stdout, " %-30s Use this file as the default ACL.\n", "-A,--default-acl=<file>");
	fprintf(stdout, " %-30s Directories without an ACL inherit from parent directories.\n","   --inherit-default-acl");
	fprintf(stdout, " %-30s Enable this authentication method.\n", "-a,--auth=<method>");
	fprintf(stdout, " %-30s Write process identifier (PID) to file.\n", "-B,--pid-file=<file>");
	fprintf(stdout, " %-30s Run as a daemon.\n", "-b,--background");
	fprintf(stdout, " %-30s Do not create a core dump, even due to a crash.\n", "-C,--no-core-dump");
	fprintf(stdout, " %-30s Challenge directory for unix filesystem authentication.\n", "-c,--challenge-dir=<dir>");
	fprintf(stdout, " %-30s Exit if parent process dies.\n", "-E,--parent-death");
	fprintf(stdout, " %-30s Leave this much space free in the filesystem.\n", "-F,--free-space=<size>");
	fprintf(stdout, " %-30s Base url for group lookups. (default: disabled)\n", "-G,--group-url=<url>");
	fprintf(stdout, " %-30s Run as lower privilege user. (root protection)\n", "-i,--user=<user>");
	fprintf(stdout, " %-30s Listen only on this network interface.\n", "-I,--interface=<addr>");
	fprintf(stdout, " %-30s Enable Chirp job execution. (default: OFF)\n", "   --jobs");
	fprintf(stdout, " %-30s Maximum concurrent jobs. (default: %d)\n", "   --job-concurrency", chirp_job_concurrency);
	fprintf(stdout, " %-30s Execution time limit for jobs. (default: %ds)\n", "   --job-time-limit", chirp_job_time_limit);
	fprintf(stdout, " %-30s Set the maximum number of clients to accept at once. (default unlimited)\n", "-M,--max-clients=<count>");
	fprintf(stdout, " %-30s Use this name when reporting to the catalog.\n", "-n,--catalog-name=<name>");
	fprintf(stdout, " %-30s Rotate debug file once it reaches this size.\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Superuser for all directories. (default: none)\n", "-P,--superuser=<user>");
	fprintf(stdout, " %-30s Listen on this port. (default: %d; arbitrary: 0)\n", "-p,--port=<port>", chirp_port);
	fprintf(stdout, " %-30s Project this Chirp server belongs to.\n", "   --project-name=<name>");
	fprintf(stdout, " %-30s Enforce this root quota in software.\n", "-Q,--root-quota=<size>");
	fprintf(stdout, " %-30s Read-only mode.\n", "-R,--read-only");
	fprintf(stdout, " %-30s Abort stalled operations after this long. (default: %ds)\n", "-s,--stalled=<time>", stall_timeout);
	fprintf(stdout, " %-30s Maximum time to cache group information. (default: %ds)\n", "-T,--group-cache-exp=<time>", chirp_group_cache_time);
	fprintf(stdout, " %-30s Disconnect idle clients after this time. (default: %ds)\n", "-t,--idle-clients=<time>", idle_timeout);
	fprintf(stdout, " %-30s Send status updates at this interval. (default: 5m)\n", "-U,--catalog-update=<time>");
	fprintf(stdout, " %-30s Use alternate password file for unix authentication.\n", "-W,--passwd=<file>");
	fprintf(stdout, " %-30s The name of this server's owner. (default: `whoami`)\n", "-w,--owner=<user>");
	fprintf(stdout, " %-30s Location of transient data. (default: `.')\n", "-y,--transient=<dir>");
	fprintf(stdout, " %-30s Select port at random and write it to this file. (default: disabled)\n", "-Z,--port-file=<file>");
	fprintf(stdout, " %-30s Set max timeout for unix filesystem authentication. (default: 5s)\n", "-z,--unix-timeout=<file>");
	fprintf(stdout, "\n");
	fprintf(stdout, "Where debug flags are: ");
	debug_flags_print(stdout);
	fprintf(stdout, "\n\n");
}

int main(int argc, char *argv[])
{
	enum {
		LONGOPT_JOBS                             = INT_MAX-0,
		LONGOPT_JOB_CONCURRENCY                  = INT_MAX-1,
		LONGOPT_JOB_TIME_LIMIT                   = INT_MAX-2,
		LONGOPT_INHERIT_DEFAULT_ACL              = INT_MAX-3,
		LONGOPT_PROJECT_NAME                     = INT_MAX-4,
	};

	static const struct option long_options[] = {
		{"advertise", required_argument, 0, 'u'},
		{"auth", required_argument, 0, 'a'},
		{"catalog-name", required_argument, 0, 'n'},
		{"challenge-dir", required_argument, 0, 'c'},
		{"catalog-update", required_argument, 0, 'U'},
		{"background", no_argument, 0, 'b'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"default-acl", required_argument, 0, 'A'},
		{"inherit-default-acl", no_argument, 0, LONGOPT_INHERIT_DEFAULT_ACL},
		{"free-space", required_argument, 0, 'F'},
		{"group-cache-exp", required_argument, 0, 'T'},
		{"group-url", required_argument, 0, 'G'},
		{"help", no_argument, 0, 'h'},
		{"idle-clients", required_argument, 0, 't'},
		{"interface", required_argument, 0, 'I'},
		{"jobs", no_argument, 0, LONGOPT_JOBS},
		{"job-concurrency", required_argument, 0, LONGOPT_JOB_CONCURRENCY},
		{"job-time-limit", required_argument, 0, LONGOPT_JOB_TIME_LIMIT},
		{"max-clients", required_argument, 0, 'M'},
		{"no-core-dump", no_argument, 0, 'C'},
		{"owner", required_argument, 0, 'w'},
		{"parent-check", required_argument, 0, 'e'},
		{"parent-death", no_argument, 0, 'E'},
		{"passwd", required_argument, 0, 'W'},
		{"pid-file", required_argument, 0, 'B'},
		{"port", required_argument, 0, 'p'},
		{"port-file", required_argument, 0, 'Z'},
		{"project-name", required_argument, 0, LONGOPT_PROJECT_NAME},
		{"read-only", no_argument, 0, 'R'},
		{"root", required_argument, 0, 'r'},
		{"root-quota", required_argument, 0, 'Q'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"stalled", required_argument, 0, 's'},
		{"superuser", required_argument, 0, 'P'},
		{"transient", required_argument, 0, 'y'},
		{"unix-timeout", required_argument, 0, 'z'},
		{"user", required_argument, 0, 'i'},
		{"version", no_argument, 0, 'v'},
		{0, 0, 0, 0}
	};

	struct link *link;
	int c;
	time_t current;
	int is_daemon = 0;
	char pidfile[PATH_MAX] = "";
	int exit_if_parent_fails = 0;
	int dont_dump_core = 0;
	time_t gc_alarm = 0;
	const char *manual_hostname = 0;
	int max_child_procs = 100;
	const char *listen_on_interface = 0;
	int total_child_procs = 0;
	int did_explicit_auth = 0;
	char port_file[PATH_MAX] = "";

	random_init();
	change_process_title_init(argv);
	change_process_title("chirp_server");

	catalog_host_list = list_create();

	debug_config("chirp_server");

	/* Ensure that all files are created private by default. */
	umask(0077);

	while((c = getopt_long(argc, argv, "A:a:B:bCc:d:Ee:F:G:hI:i:l:M:n:O:o:P:p:Q:Rr:s:T:t:U:u:vW:w:y:Z:z:", long_options, NULL)) > -1) {
		switch (c) {
		case 'A':
			{
				char path[PATH_MAX];
				path_absolute(optarg, path, 1);
				chirp_acl_default(path);
			}
			break;
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
			did_explicit_auth = 1;
			break;
		case 'b':
			is_daemon = 1;
			break;
		case 'B':
			path_absolute(optarg, pidfile, 0);
			break;
		case 'c':
			{
				char path[PATH_MAX];
				path_absolute(optarg, path, 1);
				auth_unix_challenge_dir(path);
			}
			break;
		case 'C':
			dont_dump_core = 1;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'e':
		case 'E':
			exit_if_parent_fails = 1;
			break;
		case 'F':
			minimum_space_free = string_metric_parse(optarg);
			break;
		case 'G':
			strncpy(chirp_group_base_url, optarg, sizeof(chirp_group_base_url)-1);
			break;
		case 'i':
			safe_username = optarg;
			break;
		case 'n':
			manual_hostname = optarg;
			break;
		case 'M':
			max_child_procs = atoi(optarg);
			break;
		case 'p':
			chirp_port = atoi(optarg);
			break;
		case 'P':
			chirp_super_user = optarg;
			break;
		case 'Q':
			root_quota = string_metric_parse(optarg);
			break;
		case 't':
			idle_timeout = string_time_parse(optarg);
			break;
		case 'T':
			chirp_group_cache_time = string_time_parse(optarg);
			break;
		case 's':
			stall_timeout = string_time_parse(optarg);
			break;
		case 'r':
			if (strlen(optarg) >= sizeof(chirp_url))
				fatal("root url too long");
			strcpy(chirp_url, optarg);
			break;
		case 'R':
			chirp_acl_force_readonly();
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'u':
			list_push_head(catalog_host_list, strdup(optarg));
			break;
		case 'U':
			advertise_timeout = string_time_parse(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			return 1;
		case 'w':
			strcpy(chirp_owner, optarg);
			break;
		case 'W':
			{
				char path[PATH_MAX];
				path_absolute(optarg, path, 1);
				auth_unix_passwd_file(path);
			}
			break;
		case 'I':
			listen_on_interface = optarg;
			break;
		case 'y':
			path_absolute(optarg, chirp_transient_path, 0);
			break;
		case 'z':
			auth_unix_timeout_set(atoi(optarg));
			break;
		case 'Z':
			path_absolute(optarg, port_file, 0);
			chirp_port = 0;
			break;
		case 'l':
			/* not documented, internal testing */
			sim_latency = atoi(optarg);
			break;
		case LONGOPT_INHERIT_DEFAULT_ACL:
			chirp_acl_inherit_default(1);
			break;
		case LONGOPT_JOBS:
			chirp_job_enabled = 1;
			break;
		case LONGOPT_JOB_CONCURRENCY:
			chirp_job_concurrency = strtoul(optarg, NULL, 10);
			break;
		case LONGOPT_JOB_TIME_LIMIT:
			chirp_job_time_limit = atoi(optarg);
			break;
		case LONGOPT_PROJECT_NAME:
			strncpy(chirp_project_name, optarg, sizeof(chirp_project_name)-1);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	if(is_daemon)
		daemonize(0, pidfile);
	if(is_daemon && exit_if_parent_fails)
		fatal("daemon cannot check if parent has exit (-e)");

	setpgid(0,0);

	/* Ensure that all files are created private by default (again because of daemonize). */
	umask(0077);

	cctools_version_debug(D_DEBUG, argv[0]);

	cfs_normalize(chirp_url);
	/* translate relative paths to absolute ones */
	{
		char path[PATH_MAX];
		path_absolute(chirp_transient_path, path, 0);
		strcpy(chirp_transient_path, path);
		debug(D_CHIRP, "transient directory: `%s'", chirp_transient_path);
	}

	chdir("/"); /* no more relative path access from this point on */

	if(!create_dir(chirp_transient_path, S_IRWXU)) {
		fatal("could not create transient data directory '%s': %s", chirp_transient_path, strerror(errno));
	}

	if(pipe(config_pipe) < 0)
		fatal("could not create internal pipe: %s", strerror(errno));

	if(dont_dump_core) {
		struct rlimit rl;
		rl.rlim_cur = rl.rlim_max = 0;
		setrlimit(RLIMIT_CORE, &rl);
	}

	current = time(0);
	debug(D_NOTICE, "*** %s starting at %s", argv[0], ctime(&current));

	if(!chirp_owner[0]) {
		if(!username_get(chirp_owner)) {
			strcpy(chirp_owner, "unknown");
		}
	}

	if(!did_explicit_auth) {
		auth_register_all();
	}

	if(!list_size(catalog_host_list)) {
		list_push_head(catalog_host_list, CATALOG_HOST);
	}

	if(getuid() == 0) {
		if(!safe_username) {
			fatal(
				"Sorry, I refuse to run as root without certain safeguards.\n"
				"Please give me a safe username with the -i <user> option.\n"
				"After using root access to authenticate users,\n"
				"I will use the safe username to access data on disk."
			);
		} else {
			if (pattern_match(safe_username, "^%d+$") >= 0) {
				safe_uid = safe_gid = atoi(safe_username);
			} else {
				struct passwd *p = getpwnam(safe_username);
				if(!p) {
					fatal("unknown user: %s", safe_username);
				}
				safe_uid = p->pw_uid;
				safe_gid = p->pw_gid;
			}
		}
	} else if(safe_username) {
		fatal("Sorry, the -i option doesn't make sense unless I am already running as root.");
	}

	cfs = cfs_lookup(chirp_url);

	if(run_in_child_process(backend_bootstrap, chirp_url, "backend bootstrap") != 0) {
		fatal("couldn't setup %s", chirp_url);
	}

	link = link_serve_address(listen_on_interface, chirp_port);

	if(!link) {
		if(listen_on_interface) {
			fatal("couldn't listen on interface %s port %d: %s", listen_on_interface, chirp_port, strerror(errno));
		} else {
			fatal("couldn't listen on port %d: %s", chirp_port, strerror(errno));
		}
	}

	link_address_local(link, address, &chirp_port);

	debug(D_DEBUG, "now listening port on port %d\n", chirp_port);

	if(strlen(port_file))
		opts_write_port_file(port_file, chirp_port);

	starttime = time(0);
	if(manual_hostname) {
		strcpy(hostname, manual_hostname);
	} else {
		domain_name_cache_guess(hostname);
	}

	install_handler(SIGPIPE, SIG_IGN);
	install_handler(SIGHUP, SIG_IGN);
	install_handler(SIGXFSZ, SIG_IGN);
	install_handler(SIGINT, shutdown_clean);
	install_handler(SIGTERM, shutdown_clean);
	install_handler(SIGQUIT, shutdown_clean);

	chirp_job_schedd = fork();
	if (chirp_job_schedd == 0) {
		int rc;

		close(config_pipe[0]);
		config_pipe[0] = -1;

		change_process_title("chirp_server [scheduler]");

		downgrade();
		backend_setup(chirp_url);
		rc = chirp_job_schedule();
		cfs->destroy();

		if(rc == 0) {
			/* normal exit, parent probably died */
			exit(EXIT_SUCCESS);
		} else if(rc == ENOSYS) {
			debug(D_DEBUG, "no scheduler available, quitting!");
			exit(EXIT_SUCCESS);
		} else {
			fatal("schedule rc = %d: %s", rc, strerror(rc));
		}
	} else if (chirp_job_schedd > 0) {
		debug(D_CHIRP, "forked scheduler %d", (int)chirp_job_schedd);
	} else {
		fatal("could not start scheduler");
	}

	while(1) {
		pid_t pid;
		int status;

		if(exit_if_parent_fails && getppid() == 1) {
			fatal("stopping because parent process died.");
			exit(0);
		}

		while((pid = waitpid(-1, &status, WNOHANG)) > 0) {
			if(WIFEXITED(status))
				debug(D_PROCESS, "pid %d exited with %d (%d total child procs)", pid, WEXITSTATUS(status), total_child_procs);
			else if(WIFSIGNALED(status))
				debug(D_PROCESS, "pid %d failed due to signal %d (%s) (%d total child procs)", pid, WTERMSIG(status), string_signal(WTERMSIG(status)), total_child_procs);
			else assert(0);
			total_child_procs--;
		}

		if(time(0) >= advertise_alarm) {
			run_in_child_process(update_all_catalogs, chirp_url, "catalog update");
			advertise_alarm = time(0) + advertise_timeout;
			chirp_stats_cleanup();
		}

		if(time(0) >= gc_alarm) {
			run_in_child_process(gc_tickets, chirp_url, "ticket cleanup");
			gc_alarm = time(0) + GC_TIMEOUT;
		}

		/* Wait for action on one of two ports: the main TCP port, or the internal pipe. */
		/* If the limit of child procs has been reached, don't watch the TCP port. */

		fd_set rfds;
		FD_ZERO(&rfds);
		FD_SET(config_pipe[0], &rfds);
		if(max_child_procs == 0 || total_child_procs < max_child_procs) {
			FD_SET(link_fd(link), &rfds);
		}
		int maxfd = MAX(link_fd(link), config_pipe[0]) + 1;

		/* Wait for activity on the listening port or the config pipe */
		struct timeval timeout = {.tv_sec = 1};
		if(select(maxfd, &rfds, 0, 0, &timeout) < 0)
			continue;

		/* If the network port is active, accept the connection and fork the handler. */

		if(FD_ISSET(link_fd(link), &rfds)) {
			char addr[LINK_ADDRESS_MAX];
			int port;
			struct link *l = link_accept(link, time(0) + 5);
			if(!l)
				continue;

			link_address_remote(l, addr, &port);

			pid = fork();
			if(pid == 0) {
				link_close(link);
				close(config_pipe[0]);
				config_pipe[0] = -1;
				chirp_receive(l, chirp_url);
				_exit(0);
			} else if(pid > 0) {
				total_child_procs++;
				debug(D_PROCESS, "created pid %d (%d total child procs)", pid, total_child_procs);
			} else {
				debug(D_PROCESS, "couldn't fork: %s", strerror(errno));
			}
			link_close(l);
		}

		/* If the config pipe is active, read and process those messages. */

		if(FD_ISSET(config_pipe[0], &rfds)) {
			config_pipe_handler(config_pipe[0]);
		}
	}
}

/* vim: set noexpandtab tabstop=8: */
