/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_client.h"
#include "chirp_reli.h"
#include "chirp_recursive.h"
#include "chirp_protocol.h"
#include "chirp_acl.h"
#include "chirp_matrix.h"

#include "b64.h"
#include "cctools.h"
#include "timestamp.h"
#include "debug.h"
#include "auth_all.h"
#include "auth_ticket.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "list.h"
#include "domain_name_cache.h"
#include "getopt_aux.h"
#include "path.h"

#ifdef HAS_LIBREADLINE
#include "readline/readline.h"
#include "readline/history.h"
#endif

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if CCTOOLS_OPSYS_CYGWIN || CCTOOLS_OPSYS_DARWIN || CCTOOLS_OPSYS_FREEBSD
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#endif

#define BUFFER_SIZE 32768

static int timeout = 3600;
static time_t stoptime = 0;
static char current_host[CHIRP_PATH_MAX] = { 0 };

static char current_local_dir[CHIRP_PATH_MAX];
static char current_remote_dir[CHIRP_PATH_MAX];
static char current_subject[CHIRP_LINE_MAX];

int interactive_mode = 0;

static int long_information = 0;

struct command {
	char *name;
	int must_be_open;
	int minargs, maxargs;
	char *help;
	INT64_T(*handler) (int argc, char **argv);
};

/* Standard C does not let:
   static struct command list[];
   Thus, we have to write the size of the array instead of the
   array being computed automatically when initialized below.
   Hopefully we do not have commands to chirp that often.
*/

static struct command list[100];

static void acl_simple(char **acl)
{
	if(!strcmp(*acl, "read"))
		*acl = "rl";
	if(!strcmp(*acl, "write"))
		*acl = "rwld";
	if(!strcmp(*acl, "admin"))
		*acl = "rwldva";
	if(!strcmp(*acl, "reserve"))
		*acl = "lv";
	if(!strcmp(*acl, "none"))
		*acl = ".";
}

static INT64_T do_close(int argc, char **argv)
{
	current_host[0] = 0;
	current_remote_dir[0] = 0;
	return 0;
}

static INT64_T do_open(int argc, char **argv)
{
	do_close(0, 0);

	if(chirp_reli_whoami(argv[1], current_subject, sizeof(current_subject), stoptime) >= 0) {
		strcpy(current_host, argv[1]);
		strcpy(current_remote_dir, "/");
		if(interactive_mode)
			fprintf(stderr, "connected to %s as %s\n", current_host, current_subject);
		return 0;
	} else {
		return -1;
	}
}

static void complete_local_path(const char *file, char *full_path)
{
	char temp[CHIRP_PATH_MAX];
	if(file[0] != '/') {
		string_nformat(temp, sizeof(temp), "%s/%s", current_local_dir, file);
	} else {
		strcpy(temp, file);
	}
	path_collapse(temp, full_path, 1);
}

static void complete_remote_path(const char *file, char *full_path)
{
	char temp[CHIRP_PATH_MAX];
	if(file[0] != '/') {
		string_nformat(temp, sizeof(temp), "%s/%s", current_remote_dir, file);
	} else {
		strcpy(temp, file);
	}
	path_collapse(temp, full_path, 1);
}

static INT64_T do_cat(int argc, char **argv)
{
	INT64_T actual;
	int i;

	for(i = 1; i < argc; i++) {
		char full_path[CHIRP_PATH_MAX];
		complete_remote_path(argv[i], full_path);
		actual = chirp_reli_getfile(current_host, full_path, stdout, stoptime);
		if(actual < 0) {
			fprintf(stderr, "%s: %s\n", full_path, strerror(errno));
		}
	}

	return 0;
}

static INT64_T do_cd(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	struct chirp_stat info;
	complete_remote_path(argv[1], full_path);
	if(chirp_reli_stat(current_host, full_path, &info, stoptime) < 0) {
		return -1;
	} else {
		if(S_ISDIR(info.cst_mode)) {
			path_collapse(full_path, current_remote_dir, 1);
			return 0;
		} else {
			errno = ENOTDIR;
			return -1;
		}
	}
}

static INT64_T do_lcd(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_local_path(argv[1], full_path);
	if(chdir(full_path) == 0) {
		getcwd(current_local_dir, CHIRP_PATH_MAX);
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_pwd(int argc, char **argv)
{
	printf("%s\n", current_remote_dir);
	return 0;
}

static INT64_T do_lpwd(int argc, char **argv)
{
	printf("%s\n", current_local_dir);
	return 0;
}

static INT64_T do_get(int argc, char **argv)
{
	char target_full_path[CHIRP_PATH_MAX];
	char source_full_path[CHIRP_PATH_MAX];
	timestamp_t start, stop;
	double elapsed;
	INT64_T result;

	if(!argv[2])
		argv[2] = (char *) path_basename(argv[1]);

	complete_remote_path(argv[1], source_full_path);
	complete_local_path(argv[2], target_full_path);

	start = timestamp_get();
	result = chirp_recursive_get(current_host, source_full_path, target_full_path, stoptime);
	stop = timestamp_get();

	elapsed = (double)(stop - start) / 1000000.0;

	if(result > 0) {
		fprintf(stderr, "%sB read in %.2fs ", string_metric(result, -1, 0), elapsed);
		fprintf(stderr, "(%sB/s)\n", string_metric(result / elapsed, -1, 0));
	}

	return result;
}

static INT64_T do_put(int argc, char **argv)
{
	char target_full_path[CHIRP_PATH_MAX];
	char source_full_path[CHIRP_PATH_MAX];
	timestamp_t start, stop;
	double elapsed;
	INT64_T result;

	if(!argv[2])
		argv[2] = (char *) path_basename(argv[1]);

	complete_local_path(argv[1], source_full_path);
	complete_remote_path(argv[2], target_full_path);

	start = timestamp_get();
	result = chirp_recursive_put(current_host, source_full_path, target_full_path, stoptime);
	stop = timestamp_get();

	elapsed = (stop - start) / 1000000.0;

	if(result > 0) {
		fprintf(stderr, "%sB written in %.2fs ", string_metric(result, -1, 0), elapsed);
		fprintf(stderr, "(%sB/s)\n", string_metric(result / elapsed, -1, 0));
	}

	return result;
}

static INT64_T do_ticket_create(int argc, char **argv)
{
	char name[CHIRP_PATH_MAX] = "";
	const char *subject = NULL;
	time_t duration = 86400;	/* one day */
	size_t bits = 1024;

	int i;

	/* fix split bug */
	if(argv[1] == NULL)
		argc = 0;

	for(i = 1; i < argc; i++) {
		if(*argv[i] == '-') {
			if(i == argc) {	/* is the argument option beyond the argv array? */
				fprintf(stderr, "missing option argument to create: %s\n", argv[i]);
				return -1;
			}
			if(strcmp(argv[i], "-o") == 0 || strcmp(argv[i], "-output") == 0) {
				strncpy(name, argv[++i], CHIRP_PATH_MAX);
				name[CHIRP_PATH_MAX - 1] = '\0';
			} else if(strcmp(argv[i], "-s") == 0 || strcmp(argv[i], "-subject") == 0) {
				subject = argv[++i];
			} else if(strcmp(argv[i], "-d") == 0 || strcmp(argv[i], "-duration") == 0) {
				duration = (time_t) strtoull(argv[++i], NULL, 10);
				if(duration == 0) {
					fprintf(stderr, "invalid ticket duration: %s\n", argv[i]);
					return -1;
				}
			} else if(strcmp(argv[i], "-b") == 0 || strcmp(argv[i], "-bits") == 0) {
				bits = (size_t)strtoul(argv[++i], NULL, 10);
				if(bits == 0) {
					fprintf(stderr, "invalid number of bits: %s\n", argv[i]);
					return -1;
				}
			} else {
				fprintf(stderr, "invalid option to create: %s\n", argv[i]);
				return -1;
			}
		} else
			break;
	}

	INT64_T result = chirp_reli_ticket_create(current_host, name, bits, stoptime);
	if(result < 0) {
		fprintf(stderr, "could not create ticket\n");
		return result;
	}
	fprintf(stderr, "ticket '%s': successfully created with %zu bits.\n", name, bits);

	result = chirp_reli_ticket_register(current_host, name, subject, duration, stoptime);
	if(result < 0) {
		fprintf(stderr, "could not register ticket\n");
		return result;
	}
	fprintf(stderr, "ticket '%s': successfully registered.\n", name);

	for(; i < argc; i += 2) {
		if(i == argc - 1) {
			fprintf(stderr, "ticket '%s': directory '%s' requires an aclmask.\n", name, argv[i]);
			errno = EINVAL;
			return -1;
		}
		char *path = argv[i];
		char *aclmask = argv[i + 1];
		acl_simple(&aclmask);
		fprintf(stderr, "ticket '%s': directory '%s' aclmask = '%s'.\n", name, path, aclmask);
		result = chirp_reli_ticket_modify(current_host, name, path, aclmask, stoptime);
		if(result < 0) {
			fprintf(stderr, "ticket '%s': could not set acl mask '%s' for directory '%s'\n", name, argv[i + 1], argv[i]);
			return -1;
		}
	}
	return 0;
}

static INT64_T do_ticket_register(int argc, char **argv)
{
	assert(argc == 3 || argc == 4);
	if(argc == 3) {
		return chirp_reli_ticket_register(current_host, argv[1], NULL, (time_t) strtoull(argv[2], NULL, 10), stoptime);
	} else {
		return chirp_reli_ticket_register(current_host, argv[1], argv[2], (time_t) strtoull(argv[3], NULL, 10), stoptime);
	}
}

static INT64_T do_ticket_delete(int argc, char **argv)
{
	return chirp_reli_ticket_delete(current_host, argv[1], stoptime);
}

static INT64_T do_ticket_list(int argc, char **argv)
{
	char **list;

	INT64_T result;
	if(argc == 1)
		result = chirp_reli_ticket_list(current_host, "self", &list, stoptime);
	else
		result = chirp_reli_ticket_list(current_host, argv[1], &list, stoptime);

	if(result == 0) {
		char **tmp = list;
		for(; tmp && tmp[0]; tmp++) {
			printf("%s\n", tmp[0]);
			free(tmp[0]);
		}
		free(list);
	}
	return result;
}

static INT64_T do_ticket_get(int argc, char **argv)
{
	char *subject;
	char *ticket;
	time_t duration;
	char **rights;
	INT64_T result = chirp_reli_ticket_get(current_host, argv[1], &subject, &ticket, &duration, &rights, stoptime);
	if(result >= 0) {
		printf("%s\n", subject);
		free(subject);
		/* base64 encode the ticket so it fits on one line */
		buffer_t B[1];
		buffer_init(B);
		buffer_abortonfailure(B, 1);
		b64_encode(ticket, strlen(ticket), B);
		printf("%s\n", buffer_tostring(B));
		buffer_free(B);
		free(ticket);
		printf("%llu\n", (unsigned long long) duration);
		char **tmp = rights;
		for(; tmp && tmp[0] && tmp[1]; tmp += 2) {
			printf("%s %s\n", tmp[0], tmp[1]);
			free(tmp[0]);
			free(tmp[1]);
		}
		free(rights);
	}
	return result;
}

static INT64_T do_ticket_modify(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[2], full_path);

	acl_simple(&argv[3]);

	return chirp_reli_ticket_modify(current_host, argv[1], full_path, argv[3], stoptime);
}

static INT64_T do_setacl(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);

	acl_simple(&argv[3]);

	return chirp_reli_setacl(current_host, full_path, argv[2], argv[3], stoptime);
}

static INT64_T do_resetacl(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);

	acl_simple(&argv[2]);

	return chirp_reli_resetacl(current_host, full_path, argv[2], stoptime);
}

static void print_one_acl(const char *line, void *stream)
{
	fprintf(stream, "%s\n", line);
}

static INT64_T do_getacl(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];

	if(!argv[1])
		argv[1] = ".";
	complete_remote_path(argv[1], full_path);

	return chirp_reli_getacl(current_host, full_path, print_one_acl, stdout, stoptime);
}

static INT64_T do_link(int argc, char **argv)
{
	INT64_T result;
	char path[CHIRP_PATH_MAX]; /* the new link */
	int sym = (argc == 4 && strcmp(argv[1], "-s") == 0);

	if(sym) {
		const char *target = argv[2]; /* uninterpreted */
		complete_remote_path(argv[3], path);
		result = chirp_reli_symlink(current_host, target, path, stoptime);
	} else {
		char target[CHIRP_PATH_MAX];
		complete_remote_path(argv[1], target);
		complete_remote_path(argv[2], path);
		result = chirp_reli_link(current_host, target, path, stoptime);
	}

	return result;
}

static int ls_all_mode = 0;

static void long_ls_callback(const char *name, struct chirp_stat *info, void *arg)
{
	char timestr[1024];

	if(name[0] == '.' && !ls_all_mode)
		return;

	time_t t = info->cst_ctime;
	time_t current = time(0);

	if((current - t) > (60 * 60 * 24 * 365)) {
		strftime(timestr, sizeof(timestr), "%b %d  %Y", localtime(&t));
	} else {
		strftime(timestr, sizeof(timestr), "%b %d %H:%M", localtime(&t));
	}

	printf("%c%c%c%c%c%c%c%c%c%c %4" PRId64 " %8" PRId64 " %8" PRId64 " %8" PRId64 " %s %s\n", S_ISDIR(info->cst_mode) ? 'd' : '-', info->cst_mode & 0400 ? 'r' : '-', info->cst_mode & 0200 ? 'w' : '-', info->cst_mode & 0100 ? 'x' : '-',
		   info->cst_mode & 0040 ? 'r' : '-', info->cst_mode & 0020 ? 'w' : '-', info->cst_mode & 0010 ? 'x' : '-', info->cst_mode & 0004 ? 'r' : '-', info->cst_mode & 0002 ? 'w' : '-', info->cst_mode & 0001 ? 'x' : '-', info->cst_nlink, info->cst_uid,
		   info->cst_gid, info->cst_size, timestr, name);
}

static void ls_callback(const char *name, void *arg)
{
	if(name[0] == '.' && !ls_all_mode)
		return;
	printf("%s\n", name);
}

static INT64_T do_ls(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	int long_mode = 0;

	const char *options = argv[1];
	const char *file = argv[2];

	if(!options)
		options = "";
	if(!file)
		file = "";

	if(options[0] != '-') {
		file = options;
		options = "-";
	}

	ls_all_mode = 0;

	options++;
	while(*options) {
		switch (*options) {
		case 'l':
			long_mode = 1;
			break;
		case 'a':
			ls_all_mode = 1;
			break;
		default:
			printf("ls: unknown option: %c\n", *options);
			errno = EINVAL;
			return -1;
		}
		options++;
	}

	if(!file[0])
		file = ".";
	complete_remote_path(file, full_path);

	struct chirp_dir *dir;
	struct chirp_dirent *d;

	dir = chirp_reli_opendir(current_host, full_path, stoptime);
	if(dir) {
		while((d = chirp_reli_readdir(dir))) {
			if(long_mode) {
				long_ls_callback(d->name, &d->info, 0);
			} else {
				ls_callback(d->name, 0);
			}
		}
		chirp_reli_closedir(dir);
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_rm(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);
	return chirp_reli_rmall(current_host, full_path, stoptime);
}

static INT64_T do_rmdir(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);
	return chirp_reli_rmdir(current_host, full_path, stoptime);
}

static INT64_T do_mkdir(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	INT64_T result;
	int create_parents = (argc == 3 && strcmp(argv[1], "-p") == 0);

	if(create_parents) {
		complete_remote_path(argv[2], full_path);
		result = chirp_reli_mkdir_recursive(current_host, full_path, 0777, stoptime);
	} else {
		complete_remote_path(argv[1], full_path);
		result = chirp_reli_mkdir(current_host, full_path, 0777, stoptime);
	}

	if(result < 0 && errno == EEXIST)
		result = 0;

	return result;
}

static INT64_T do_stat(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	struct chirp_stat info;
	time_t t;

	complete_remote_path(argv[1], full_path);

	if(chirp_reli_stat(current_host, full_path, &info, stoptime) < 0) {
		return -1;
	} else {
		printf("device:  %" PRId64 "\n", info.cst_dev);
		printf("inode:   %" PRId64 "\n", info.cst_ino);
		printf("mode:    %04" PRIu64 "\n", info.cst_mode);
		printf("nlink:   %" PRId64 "\n", info.cst_nlink);
		printf("uid:     %" PRId64 "\n", info.cst_uid);
		printf("gid:     %" PRId64 "\n", info.cst_gid);
		printf("rdevice: %" PRId64 "\n", info.cst_rdev);
		printf("size:    %" PRId64 "\n", info.cst_size);
		printf("blksize: %" PRId64 "\n", info.cst_blksize);
		printf("blocks:  %" PRId64 "\n", info.cst_blocks);
		t = info.cst_atime;
		printf("atime:   %s", ctime(&t));
		t = info.cst_mtime;
		printf("mtime:   %s", ctime(&t));
		t = info.cst_ctime;
		printf("ctime:   %s", ctime(&t));
		return 0;
	}
}

static INT64_T do_statfs(int argc, char **argv)
{
	struct chirp_statfs info;
	int metric_power = -1;

	if(argc > 1) {
		if(!strcmp(argv[1], "-k")) {
			metric_power = 1;
		} else if(!strcmp(argv[1], "-m")) {
			metric_power = 2;
		} else if(!strcmp(argv[1], "-g")) {
			metric_power = 3;
		} else if(!strcmp(argv[1], "-t")) {
			metric_power = 4;
		} else {
			errno = EINVAL;
			return -1;
		}
	}

	if(chirp_reli_statfs(current_host, "/", &info, stoptime) < 0) {
		return -1;
	} else {
		printf("/\n");
		printf("%sB TOTAL\n", string_metric(info.f_blocks * info.f_bsize, metric_power, 0));
		printf("%sB INUSE\n", string_metric((info.f_blocks - info.f_bfree) * info.f_bsize, metric_power, 0));
		return 0;
	}
}

static INT64_T do_mv(int argc, char **argv)
{
	char old_full_path[CHIRP_PATH_MAX];
	char new_full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], old_full_path);
	complete_remote_path(argv[2], new_full_path);
	return chirp_reli_rename(current_host, old_full_path, new_full_path, stoptime);
}

static INT64_T do_chmod(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	unsigned mode;
	sscanf(argv[1], "%o", &mode);
	complete_remote_path(argv[2], full_path);
	return chirp_reli_chmod(current_host, full_path, mode, stoptime);
}

static INT64_T do_debug(int argc, char **argv)
{
	if(argv[1]) {
		if(debug_flags_set(argv[1])) {
			return 0;
		} else {
			printf("Valid debugging flags are: ");
			debug_flags_print(stdout);
			printf("\n");
			errno = EINVAL;
			return -1;
		}
	} else {
		debug_flags_clear();
		return 0;
	}

}

static INT64_T do_remote_debug(int argc, char **argv)
{
	return chirp_reli_remote_debug(current_host, argv[1], stoptime);
}

static INT64_T do_whoami(int argc, char **argv)
{
	char name[CHIRP_LINE_MAX];
	INT64_T result;

	result = chirp_reli_whoami(current_host, name, sizeof(name), stoptime);
	if(result >= 0) {
		name[result] = 0;
		printf("%s\n", name);
	}
	return result;
}

static INT64_T do_whoareyou(int argc, char **argv)
{
	char name[CHIRP_LINE_MAX];
	INT64_T result;

	result = chirp_reli_whoareyou(current_host, argv[1], name, sizeof(name), stoptime);
	if(result >= 0) {
		name[result] = 0;
		printf("%s\n", name);
	}
	return result;
}

static INT64_T do_hash(int argc, char **argv)
{
	INT64_T result;
	char full_path[CHIRP_LINE_MAX];
	unsigned char digest[CHIRP_DIGEST_MAX];

	complete_remote_path(argv[2], full_path);

	result = chirp_reli_hash(current_host, full_path, argv[1], digest, stoptime);
	if(result > 0) {
		int i;
		for (i = 0; i < result; i++)
			printf("%02X", (unsigned)digest[i]);
		printf("\t%s\n", full_path);
	}
	return result;

}

static INT64_T do_md5(int argc, char **argv)
{
	char *nargv[] = {
		"hash",
		"md5",
		argv[1],
		NULL
	};
	return do_hash(3, nargv);
}

static INT64_T do_setrep(int argc, char **argv)
{
	char full_path[CHIRP_LINE_MAX];

	complete_remote_path(argv[1], full_path);
	int nreps = atoi(argv[2]);

	return chirp_reli_setrep(current_host, full_path, nreps, stoptime);
}

static INT64_T do_localpath(int argc, char **argv)
{
	char localpath[CHIRP_LINE_MAX];
	char full_path[CHIRP_LINE_MAX];
	INT64_T result;

	if(argc == 2) {
		complete_remote_path(argv[1], full_path);
	} else {
		complete_remote_path(".", full_path);
	}

	result = chirp_reli_localpath(current_host, full_path, localpath, sizeof(localpath), stoptime);
	if(result >= 0) {
		localpath[result] = 0;
		printf("%s\n", localpath);
	}
	return result;
}

static INT64_T do_audit(int argc, char **argv)
{
	struct chirp_audit *list;
	INT64_T result;
	int raw_mode = 0;

	if(argc > 1) {
		if(!strcmp(argv[1], "-r")) {
			raw_mode = 1;
		} else {
			printf("audit: unknown option: %s\n", argv[1]);
			return -1;
		}
	}

	result = chirp_reli_audit(current_host, "/", &list, stoptime);
	if(result >= 0) {
		int i;
		if(!raw_mode)
			printf("   FILES     DIRS      DATA OWNER\n");
		for(i = 0; i < result; i++) {
			if(raw_mode) {
				printf("%" PRId64 " %" PRId64 " %" PRId64 " %s\n", list[i].nfiles, list[i].ndirs, list[i].nbytes, list[i].name);
			} else {
				printf("%8" PRId64 " %8" PRId64 " %8sB %s\n", list[i].nfiles, list[i].ndirs, string_metric(list[i].nbytes, -1, 0), list[i].name);
			}
		}
		free(list);
	}

	return result;
}

static INT64_T do_timeout(int argc, char **argv)
{
	timeout = atoi(argv[1]);
	fprintf(stderr, "Timeout is %d seconds.\n", timeout);
	return 0;
}

static INT64_T do_quit(int argc, char **argv)
{
	exit(0);
	return -1;
}

static INT64_T do_help(int argc, char **argv)
{
	int i;
	printf("Commands:\n");
	for(i = 0; list[i].name; i++) {
		printf("%-12s %s\n", list[i].name, list[i].help);
	}
	printf("\nDebugging subsystems are:\n");
	debug_flags_print(stdout);
	printf("\n");
	return 0;
}

static INT64_T do_thirdput(int argc, char **argv)
{
	INT64_T result;
	char full_path[CHIRP_PATH_MAX];
	char remote_path[CHIRP_PATH_MAX];
	time_t stop, start;

	complete_remote_path(argv[1], full_path);
	string_nformat(remote_path, sizeof(remote_path), "/%s", argv[3]);

	start = time(0);
	result = chirp_reli_thirdput(current_host, full_path, argv[2], remote_path, stoptime);
	stop = time(0);
	if(stop == start)
		stop++;

	if(result > 0) {
		fprintf(stderr, "%" PRId64 " bytes transferred in %d seconds ", result, (int) (stop - start));
		fprintf(stderr, "(%.1lfMB/s)\n", result / (double) (stop - start) / 1024.0 / 1024.0);
	}

	return result;
}

static INT64_T do_mkalloc(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);
	return chirp_reli_mkalloc(current_host, full_path, string_metric_parse(argv[2]), S_IRWXU, stoptime);
}

static INT64_T do_lsalloc(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	char alloc_path[CHIRP_PATH_MAX];
	INT64_T total, inuse;
	INT64_T result;

	if(argc != 2)
		argv[1] = ".";

	complete_remote_path(argv[1], full_path);

	result = chirp_reli_lsalloc(current_host, full_path, alloc_path, &total, &inuse, stoptime);

	if(result >= 0) {
		printf("%s\n", alloc_path);
		printf("%sB TOTAL\n", string_metric(total, -1, 0));
		printf("%sB INUSE\n", string_metric(inuse, -1, 0));
	}

	return result;
}

static INT64_T do_matrix_create(int argc, char **argv)
{
	char path[CHIRP_PATH_MAX];
	struct chirp_matrix *m;

	complete_remote_path(argv[1], path);

	m = chirp_matrix_create(current_host, path, atoi(argv[2]), atoi(argv[3]), sizeof(double), atoi(argv[4]), stoptime);
	if(m) {
		chirp_matrix_close(m, stoptime);
		return 0;
	} else {
		return 1;
	}
}

static INT64_T do_matrix_list(int argc, char **argv)
{
	char path[CHIRP_PATH_MAX];
	struct chirp_matrix *m;

	complete_remote_path(argv[1], path);

	m = chirp_matrix_open(current_host, path, stoptime);
	if(m) {
		printf("host:   %s\n", current_host);
		printf("path:   %s\n", path);
		printf("width:  %d\n", chirp_matrix_width(m));
		printf("height: %d\n", chirp_matrix_height(m));
		printf("esize:  %d\n", chirp_matrix_element_size(m));
		printf("nhosts: %d\n", chirp_matrix_nhosts(m));
		printf("nfiles: %d\n", chirp_matrix_nfiles(m));
		chirp_matrix_close(m, stoptime);
		return 0;
	} else {
		return 1;
	}
}

static INT64_T do_matrix_delete(int argc, char **argv)
{
	char path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], path);
	return chirp_matrix_delete(current_host, path, stoptime);
}

static char *strerrsource(int errsource)
{
	switch (errsource) {
	case CHIRP_SEARCH_ERR_OPEN:
		return "Open";
	case CHIRP_SEARCH_ERR_READ:
		return "Read";
	case CHIRP_SEARCH_ERR_CLOSE:
		return "Close";
	case CHIRP_SEARCH_ERR_STAT:
		return "Stat";
	default:
		return "Unknown";
	}
}

static INT64_T do_search(int argc, char **argv)
{
	CHIRP_SEARCH *S;
	int flags = 0;
	if (argc == 4 && *argv[1] == '-') {
		if (strchr(argv[1], 'i'))
			flags |= CHIRP_SEARCH_INCLUDEROOT;
		if (strchr(argv[1], 'm'))
			flags |= CHIRP_SEARCH_METADATA;
		if (strchr(argv[1], 's'))
			flags |= CHIRP_SEARCH_STOPATFIRST;
	}

	if (argc == 4)
		S = chirp_reli_opensearch(current_host, argv[2], argv[3], flags, stoptime);
	else
		S = chirp_reli_opensearch(current_host, argv[1], argv[2], flags, stoptime);
	struct chirp_searchent *res;

	while((res = chirp_client_readsearch(S)) != NULL) {
		if(res->err) {
			printf("%s error on %s: %s\n", strerrsource(res->errsource), res->path, strerror(res->err));
			continue;
		}

		printf("%-30s", res->path);

		if(flags & CHIRP_SEARCH_METADATA)
			printf("\t" INT64_FORMAT "\t" INT64_FORMAT "\n", res->info.cst_size, res->info.cst_ino);
		else
			printf("\n");
	}

	chirp_client_closesearch(S);
	return 0;
}

static INT64_T do_xattr_get(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);
	char data[65536];
	INT64_T size;

	if((size = chirp_reli_getxattr(current_host, full_path, argv[2], data, sizeof(data), stoptime)) > 0) {
		write(STDOUT_FILENO, data, (size_t) size);
		write(STDOUT_FILENO, "\n", 1);
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_xattr_list(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);
	char data[65536];
	INT64_T size;

	if((size = chirp_reli_listxattr(current_host, full_path, data, sizeof(data), stoptime)) > 0) {
		char *current;
		for(current = data; *current; current = current + strlen(current) + 1) {
			write(STDOUT_FILENO, current, strlen(current));
			write(STDOUT_FILENO, "\n", 1);
		}
		return 0;
	} else {
		return -1;
	}
}

static INT64_T do_xattr_set(int argc, char **argv)
{
	char full_path[CHIRP_PATH_MAX];
	complete_remote_path(argv[1], full_path);
	INT64_T size;

	if(argc == 3) {
		if((size = chirp_reli_removexattr(current_host, full_path, argv[2], stoptime)) >= 0) {
			return 0;
		} else {
			return -1;
		}
	} else {
		if((size = chirp_reli_setxattr(current_host, full_path, argv[2], argv[3], strlen(argv[3]), 0, stoptime)) >= 0) {
			return 0;
		} else {
			return -1;
		}
	}
}

static INT64_T do_job_create(int argc, char **argv)
{
	chirp_jobid_t id;
	INT64_T result = chirp_reli_job_create(current_host, argv[1], &id, stoptime);
	if (result == 0) {
		fprintf(stdout, "%" PRICHIRP_JOBID_T "\n", id);
		fflush(stdout);
	}
	return result;
}

static INT64_T do_job_commit(int argc, char **argv)
{
	return chirp_reli_job_commit(current_host, argv[1], stoptime);
}

static INT64_T do_job_kill(int argc, char **argv)
{
	return chirp_reli_job_kill(current_host, argv[1], stoptime);
}

static INT64_T do_job_status(int argc, char **argv)
{
	char *status;
	INT64_T result = chirp_reli_job_status(current_host, argv[1], &status, stoptime);
	if (result > 0) {
		fprintf(stdout, "%s\n", status);
		free(status);
	}
	return result;
}

static INT64_T do_job_wait(int argc, char **argv)
{
	chirp_jobid_t id;
	INT64_T timeout = -1;
	char *status;
	sscanf(argv[1], "%" SCNCHIRP_JOBID_T, &id);
	if (argc >= 3)
		sscanf(argv[2], "%" SCNd64, &timeout);
	INT64_T result = chirp_reli_job_wait(current_host, id, timeout, &status, stoptime);
	if (result > 0) {
		fprintf(stdout, "%s\n", status);
		free(status);
	}
	return result;
}

static INT64_T do_job_reap(int argc, char **argv)
{
	return chirp_reli_job_reap(current_host, argv[1], stoptime);
}


/* If adding a command below, remember to modify the size of
 * struct command list[] at the top of this file. */
static struct command list[] = {
	{"audit", 1, 0, 1, "[-r]", do_audit},
	{"cat", 1, 1, 100, "<file> [file2] [file3] ...", do_cat},
	{"cd", 1, 1, 1, "<remotedir>", do_cd},
	{"chmod", 1, 2, 2, "<mode> <path>", do_chmod},
	{"close", 1, 0, 0, "", do_close},
	{"debug", 0, 0, 1, "[subsystem]", do_debug},
	{"df", 1, 0, 1, "[-k|-m|-g|-t]", do_statfs},
	{"exit", 0, 0, 0, "", do_quit},
	{"get", 1, 1, 2, "<remotefile> [localfile]", do_get},
	{"getacl", 1, 0, 1, "[remotepath]", do_getacl},
	{"help", 0, 0, 0, "", do_help},
	{"lcd", 0, 1, 1, "<localdir>", do_lcd},
	{"listacl", 1, 0, 1, "[remotepath]", do_getacl},
	{"localpath", 1, 0, 1, "[remotepath]", do_localpath},
	{"lpwd", 0, 0, 0, "", do_lpwd},
	{"ln", 1, 2, 3, "[-s] <path> <new path>", do_link},
	{"ls", 1, 0, 2, "[-la] [remotepath]", do_ls},
	{"lsalloc", 1, 0, 1, "[path]", do_lsalloc},
	{"matrix_create", 1, 4, 4, "<path> <width> <height> <nhosts>", do_matrix_create},
	{"matrix_delete", 1, 1, 1, "<path>", do_matrix_delete},
	{"matrix_list", 1, 1, 1, "<path>", do_matrix_list},
	{"hash", 1, 2, 2, "<algorithm> <path>", do_hash},
	{"md5", 1, 1, 1, "<path>", do_md5},
	{"mkalloc", 1, 2, 2, "<path> <size>", do_mkalloc},
	{"mkdir", 1, 1, 2, "[-p] <dir>", do_mkdir},
	{"mv", 1, 2, 2, "<oldname> <newname>", do_mv},
	{"open", 0, 1, 1, "<host>", do_open},
	{"put", 1, 1, 2, "<localfile> [remotefile]", do_put},
	{"pwd", 1, 0, 0, "", do_pwd},
	{"quit", 0, 0, 0, "", do_quit},
	{"remote_debug", 1, 1, 1, "[subsystem]", do_remote_debug},
	{"resetacl", 1, 2, 2, "<remotepath> <rwldax>", do_resetacl},
	{"rm", 1, 1, 1, "<file>", do_rm},
	{"rmdir", 1, 1, 1, "<dir>", do_rmdir},
	{"search", 1, 2, 3, "[-ims] <directory> <pattern>", do_search},
	{"setacl", 1, 3, 3, "<remotepath> <user> <rwldax>", do_setacl},
	{"setrep", 1, 2, 2, "<path> <nreps>", do_setrep},
	{"stat", 1, 1, 1, "<file>", do_stat},
	{"thirdput", 1, 3, 3, "<file> <3rdhost> <3rdfile>", do_thirdput},
	{"ticket_create", 1, 0, 100, "[-o[utput] <ticket filename>] [-s[ubject] <subject/user>] [-d[uration] <duration>] [-b[its] <bits>] [[<directory> <acl>] ...]", do_ticket_create},
	{"ticket_delete", 1, 1, 1, "<name>", do_ticket_delete},
	{"ticket_get", 1, 1, 1, "<name>", do_ticket_get},
	{"ticket_list", 1, 0, 1, "<name>", do_ticket_list},
	{"ticket_modify", 1, 3, 3, "<name> <directory> <aclmask>", do_ticket_modify},
	{"ticket_register", 1, 2, 3, "<name> [<subject>] <duration>", do_ticket_register},
	{"timeout", 0, 1, 1, "<seconds>", do_timeout},
	{"whoami", 1, 0, 0, "", do_whoami},
	{"whoareyou", 1, 1, 1, "<hostname>", do_whoareyou},
	{"xattr_get", 1, 2, 2, "<file> <attribute>", do_xattr_get},
	{"xattr_list", 1, 1, 1, "<file>", do_xattr_list},
	{"xattr_set", 1, 2, 3, "<file> <attribute> [value]", do_xattr_set},
	{"job_create", 1, 1, 1, "<json>", do_job_create},
	{"job_commit", 1, 1, 1, "<id>", do_job_commit},
	{"job_kill", 1, 1, 1, "<id>", do_job_kill},
	{"job_status", 1, 1, 1, "<id>", do_job_status},
	{"job_wait", 1, 1, 2, "<id> [timeout]", do_job_wait},
	{"job_reap", 1, 1, 1, "<json>", do_job_reap},
	{0, 0, 0, 0, 0, 0},
};

static int process_command(int argc, char **argv)
{
	int i;

	for(i = 0; list[i].name; i++) {
		if(!strcmp(list[i].name, argv[0])) {
			if(!current_host[0] && list[i].must_be_open) {
				printf("not connected\n");
				return 0;
			} else if(((argc - 1) >= list[i].minargs) && ((argc - 1) <= list[i].maxargs)) {
				stoptime = time(0) + timeout;
				if(list[i].handler(argc, argv) < 0) {
					fprintf(stderr, "couldn't %s: %s\n", argv[0], strerror(errno));
					return 0;
				} else {
					return 1;
				}
			} else {
				printf("use: %s %s\n", argv[0], list[i].help);
				return 0;
			}
		}
	}

	printf("unknown command: %s  (try 'help')\n", argv[0]);
	return 0;
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "use: %s [options] [hostname] [command]\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Require this authentication mode.\n", "-a,--auth=<flag>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem.\n", "-d,--debug=<flag>");
	fprintf(stdout, " %-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Comma-delimited list of tickets to use for authentication.\n", "-i,--tickets=<files>");
	fprintf(stdout, " %-30s Long transfer information.\n", "-l,--verbose");
	fprintf(stdout, " %-30s Set remote operation timeout.\n", "-t,--timeout=<time>");
	fprintf(stdout, " %-30s Show program version.\n", "-v,--version");
	fprintf(stdout, " %-30s This message.\n", "-h,--help");
}

int main(int argc, char *argv[])
{
	char *temp;
	int did_explicit_auth = 0;
	char *tickets = NULL;
	char prompt[2*CHIRP_LINE_MAX];
	char line[CHIRP_LINE_MAX];
	char **user_argv = 0;
	int user_argc;
	int c;
	INT64_T result = 0;

	debug_config(argv[0]);

	static const struct option long_options[] = {
		{"auth", required_argument, 0, 'a'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"tickets", required_argument, 0, 'i'},
		{"verbose", no_argument, 0, 'l'},
		{"timeout", required_argument, 0, 't'},
		{"version", no_argument, 0, 'v'},
		{"help", no_argument, 0, 'h'},
		{0, 0, 0, 0}
	};

	while((c = getopt_long(argc, argv, "+a:d:hi:lo:t:v", long_options, NULL)) > -1) {
		switch (c) {
		case 'a':
			if (!auth_register_byname(optarg))
				fatal("could not register authentication method `%s': %s", optarg, strerror(errno));
			did_explicit_auth = 1;
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'h':
			show_help(argv[0]);
			exit(0);
			break;
		case 'i':
			tickets = strdup(optarg);
			break;
		case 'l':
			long_information = 1;
			break;
		case 't':
			timeout = string_time_parse(optarg);
			if (timeout == -1)
				fatal("invalid timeout: %s", strerror(errno));
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(0);
			break;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(!did_explicit_auth)
		auth_register_all();
	if(tickets) {
		auth_ticket_load(tickets);
		free(tickets);
	} else if(getenv(CHIRP_CLIENT_TICKETS)) {
		auth_ticket_load(getenv(CHIRP_CLIENT_TICKETS));
	} else {
		auth_ticket_load(NULL);
	}

	getcwd(current_local_dir, CHIRP_PATH_MAX);

	/* interactive mode if input is a TTY but we are not simply executing a
	 * command from argv */
	interactive_mode = isatty(0) && !((argc - optind) > 1);

	if(optind < argc) {
		stoptime = time(0) + timeout;
		if(do_open(1, &argv[optind - 1])) {
			fprintf(stderr, "couldn't open %s: %s\n", argv[optind], strerror(errno));
			return 1;
		}
	}

	if((argc - optind) > 1) {
		return !process_command(argc - optind - 1, &argv[optind + 1]);
	}

	while(1) {
		if(interactive_mode) {
			string_nformat(prompt, sizeof(prompt), " chirp:%s:%s> ", current_host, current_remote_dir);
		} else {
			prompt[0] = 0;
		}

#ifdef HAS_LIBREADLINE
		temp = readline(prompt);
		if(!temp)
			break;
		strcpy(line, temp);
		free(temp);
#else
		printf("%s", prompt);
		fflush(stdout);
		if(!fgets(line, CHIRP_LINE_MAX, stdin))
			break;
#endif

		if(!line[0])
			continue;

		if(!interactive_mode && (temp = strchr(line, '#'))) {	/* comment? */
			for(temp--; temp > line && isspace((int) *temp); temp--);	/* preceding space? */
			if(temp <= line)
				continue;	/* else not comment */
		}
#ifdef HAS_LIBREADLINE
		add_history(line);
#endif

		{
			char *start = line, *last = strlen(line) + line;
			while(*start != '\0') {	/* process compound commands */
				char *end = strchr(start, ';');
				while(end != NULL && end != start && *(end - 1) == '\\')
					end = strchr(end + 1, ';');
				if(end == NULL)
					end = start + strlen(start);
				*end = '\0';

				if(user_argv)
					free(user_argv);
				string_split_quotes(start, &user_argc, &user_argv);
				if(user_argc == 0) {
					start++;
					continue;
				}
				result = process_command(user_argc, user_argv);
				start = end == last ? last : end + 1;
			}
		}
		if(!interactive_mode && !result)
			break;
	}

	if(result) {
		return 0;
	} else {
		return 1;
	}
}

/* vim: set noexpandtab tabstop=4: */
