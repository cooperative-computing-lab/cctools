/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_reli.h"
#include "chirp_recursive.h"

#include "stringtools.h"
#include "list.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>

#ifdef CCTOOLS_OPSYS_DARWIN
#define fopen64 fopen
#define open64 open
#define lseek64 lseek
#define stat64 stat
#define fstat64 fstat
#define lstat64 lstat
#define fseeko64 fseeko
#endif

static void add_to_list(const char *name, void *list)
{
	list_push_tail(list, strdup(name));
}

static INT64_T do_get_one_dir(const char *hostport, const char *source_file, const char *target_file, int mode, time_t stoptime)
{
	char new_source_file[CHIRP_PATH_MAX];
	char new_target_file[CHIRP_PATH_MAX];
	struct list *work_list;
	const char *name;
	INT64_T result;
	INT64_T total = 0;

	work_list = list_create();

	result = mkdir(target_file, mode);
	if(result == 0 || errno == EEXIST) {
		result = chirp_reli_getdir(hostport, source_file, add_to_list, work_list, stoptime);
		if(result >= 0) {
			while((name = list_pop_head(work_list))) {
				if(!strcmp(name, "."))
					continue;
				if(!strcmp(name, ".."))
					continue;
				sprintf(new_source_file, "%s/%s", source_file, name);
				sprintf(new_target_file, "%s/%s", target_file, name);
				result = chirp_recursive_get(hostport, new_source_file, new_target_file, stoptime);
				free((char *) name);
				if(result < 0)
					break;
				total += result;
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}

	while((name = list_pop_head(work_list)))
		free((char *) name);

	list_delete(work_list);

	if(result >= 0) {
		return total;
	} else {
		return -1;
	}
}

static INT64_T do_get_one_link(const char *hostport, const char *source_file, const char *target_file, time_t stoptime)
{
	char linkdata[CHIRP_PATH_MAX];
	INT64_T result;

	result = chirp_reli_readlink(hostport, source_file, linkdata, sizeof(linkdata), stoptime);
	if(result >= 0) {
		linkdata[result] = 0;
		unlink(target_file);
		result = symlink(linkdata, target_file);
		if(result >= 0)
			result = 0;
	}

	return result;
}

static INT64_T do_get_one_file(const char *hostport, const char *source_file, const char *target_file, int mode, INT64_T length, time_t stoptime)
{
	FILE *file;
	int save_errno;
	INT64_T actual;

	file = fopen64(target_file, "w");
	if(!file)
		return -1;

	fchmod(fileno(file), mode);

	actual = chirp_reli_getfile(hostport, source_file, file, stoptime);
	if(actual != length) {
		save_errno = errno;
		fclose(file);
		errno = save_errno;
		return -1;
	}

	if(length >= 0) {
		fclose(file);
		return length;
	} else {
		save_errno = errno;
		fclose(file);
		errno = save_errno;
		return -1;
	}
}

INT64_T chirp_recursive_get(const char *hostport, const char *source_file, const char *target_file, time_t stoptime)
{
	INT64_T result;
	struct chirp_stat info;

	result = chirp_reli_lstat(hostport, source_file, &info, stoptime);
	if(result >= 0) {
		if(S_ISLNK(info.cst_mode)) {
			result = do_get_one_link(hostport, source_file, target_file, stoptime);
		} else if(S_ISDIR(info.cst_mode)) {
			result = do_get_one_dir(hostport, source_file, target_file, info.cst_mode, stoptime);
		} else if(S_ISREG(info.cst_mode)) {
			result = do_get_one_file(hostport, source_file, target_file, info.cst_mode, info.cst_size, stoptime);
		} else {
			result = 0;
		}
	}

	return result;
}

static INT64_T do_put_one_dir(const char *hostport, const char *source_file, const char *target_file, int mode, time_t stoptime)
{
	char new_source_file[CHIRP_PATH_MAX];
	char new_target_file[CHIRP_PATH_MAX];
	struct list *work_list;
	const char *name;
	INT64_T result;
	INT64_T total = 0;

	struct dirent *d;
	DIR *dir;

	work_list = list_create();

	result = chirp_reli_mkdir(hostport, target_file, mode, stoptime);
	if(result == 0 || errno == EEXIST) {
		result = 0;
		dir = opendir(source_file);
		if(dir) {
			while((d = readdir(dir))) {
				if(!strcmp(d->d_name, "."))
					continue;
				if(!strcmp(d->d_name, ".."))
					continue;
				list_push_tail(work_list, strdup(d->d_name));
			}
			closedir(dir);
			while((name = list_pop_head(work_list))) {
				sprintf(new_source_file, "%s/%s", source_file, name);
				sprintf(new_target_file, "%s/%s", target_file, name);
				result = chirp_recursive_put(hostport, new_source_file, new_target_file, stoptime);
				free((char *) name);
				if(result < 0)
					break;
				total += result;
			}
		} else {
			result = -1;
		}
	} else {
		result = -1;
	}

	while((name = list_pop_head(work_list)))
		free((char *) name);

	list_delete(work_list);

	if(result < 0) {
		return -1;
	} else {
		return total;
	}
}

static INT64_T do_put_one_link(const char *hostport, const char *source_file, const char *target_file, time_t stoptime)
{
	char linkdata[CHIRP_PATH_MAX];
	INT64_T result;

	result = readlink(source_file, linkdata, sizeof(linkdata));
	if(result > 0) {
		linkdata[result] = 0;
		chirp_reli_unlink(hostport, target_file, stoptime);
		result = chirp_reli_symlink(hostport, linkdata, target_file, stoptime);
		if(result >= 0)
			result = 0;
	}

	return result;
}

static INT64_T do_put_one_file(const char *hostport, const char *source_file, const char *target_file, int mode, INT64_T length, time_t stoptime)
{
	FILE *file;
	int save_errno;

	file = fopen64(source_file, "r");
	if(!file)
		return -1;

	length = chirp_reli_putfile(hostport, target_file, file, mode, length, stoptime);

	if(length >= 0) {
		fclose(file);
		return length;
	} else {
		save_errno = errno;
		fclose(file);
		errno = save_errno;
		return -1;
	}
}

static INT64_T do_put_one_fifo(const char *hostport, const char *source_file, const char *target_file, int mode, time_t stoptime)
{
	FILE *file;
	int save_errno;
	struct chirp_file *cf = 0;
	INT64_T result;
	INT64_T offset = 0;

	file = fopen64(source_file, "r");
	if(!file)
		return -1;

	cf = chirp_reli_open(hostport, target_file, O_WRONLY|O_CREAT|O_TRUNC, 0600, stoptime);
	if(cf) {
		size_t n;
		char buffer[65536];
		while((n = fread(buffer, sizeof(char), 65536, file))) {
			if(chirp_reli_pwrite(cf, buffer, n, offset, stoptime) < 0)
				goto fail;
			offset += n;
		}
		if (chirp_reli_close(cf, stoptime) < 0)
			goto fail;
	}

	result = offset;
	goto out;
fail:
	result = -1;
	goto out;
out:
	save_errno = errno;
	fclose(file);
	errno = save_errno;
	return result;
}

INT64_T chirp_recursive_put(const char *hostport, const char *source_file, const char *target_file, time_t stoptime)
{
	INT64_T result;
	struct stat64 info;
	struct stat64 linfo;

	result = lstat64(source_file, &linfo);
	if(result >= 0) {
		if(S_ISLNK(linfo.st_mode) && (strncmp("/dev", source_file, 4) == 0 || strncmp("/proc", source_file, 5) == 0)) {
			result = stat64(source_file, &info);	/* for /dev/fd/n and similar */
			if(result == -1)
				return 0;
		} else
			info = linfo;
		mode_t mode = info.st_mode;
		if(S_ISLNK(mode)) {
			result = do_put_one_link(hostport, source_file, target_file, stoptime);
		} else if(S_ISDIR(mode)) {
			result = do_put_one_dir(hostport, source_file, target_file, 0700, stoptime);
		} else if(S_ISBLK(mode) || S_ISCHR(mode) || S_ISFIFO(mode)) {
			result = do_put_one_fifo(hostport, source_file, target_file, info.st_mode, stoptime);
		} else if(S_ISREG(mode)) {
			result = do_put_one_file(hostport, source_file, target_file, info.st_mode, info.st_size, stoptime);
		} else {
			result = 0;
		}
	}

	return result;
}

/* vim: set noexpandtab tabstop=8: */
