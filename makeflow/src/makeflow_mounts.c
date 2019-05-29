/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <assert.h>
#include <errno.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "copy_tree.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "http_query.h"
#include "list.h"
#include "makeflow_log.h"
#include "md5.h"
#include "mkdir_recursive.h"
#include "makeflow_mounts.h"
#include "path.h"
#include "shell.h"
#include "stringtools.h"
#include "unlink_recursive.h"
#include "xxmalloc.h"

#define HTTP_TIMEOUT 300

/* create_link creates a link from link_name to link_target.
 * first try to create a hard link, then try to create a symlink when failed to create a hard link.
 * return 0 on success, non-zero on failure.
 */
int create_link(const char *link_target, const char *link_name) {
	if(link(link_target, link_name)) {
		debug(D_DEBUG, "link(%s, %s) failed: %s!\n", link_target, link_name, strerror(errno));
		if(symlink(link_target, link_name)) {
			debug(D_DEBUG, "symlink(%s, %s) failed: %s!\n", link_target, link_name, strerror(errno));
			return -1;
		}
	}
	return 0;
}

/* mount_install_http downloads a dependency from source to cache_path.
 * @param source: a http or https url.
 * @param cache_path: a file path in the cache dir.
 * return 0 on success; return -1 on failure.
 */
int mount_install_http(const char *source, const char *cache_path) {
	char *command = string_format("wget -O %s %s", cache_path, source);

	int status;
	int rc = shellcode(command, NULL, NULL, 0, NULL, NULL, &status);

	if(rc) {
		debug(D_DEBUG, "`%s` failed!\n", command);
		free(command);
		return -1;
	}

	free(command);
	return 0;
}

/* mount_check_http checks whether a http url is available by sending a HEAD request to it.
 * return 0 on success; return -1 on failure.
 */
int mount_check_http(const char *url) {
	struct link *link = http_query(url, "HEAD", time(0) + HTTP_TIMEOUT);
	if(!link) {
		debug(D_DEBUG, "http_query(%s, \"HEAD\", ...) failed!\n", url);
		fprintf(stderr, "http_query(%s, \"HEAD\", ...) failed!\n", url);
		return -1;
	}
	link_close(link);
	return 0;
}

/* mount_install_local copies source to target.
 * @param source: a local file path which must exist already.
 * @param target: a local file path which must not exist.
 * @param cache_path: a file path in the cache dir.
 * @param s_type: the file type of source.
 * return 0 on success; return -1 on failure.
 */
int mount_install_local(const char *source, const char *target, const char *cache_path, file_type s_type) {
	switch(s_type) {
	case FILE_TYPE_REG:
		if(copy_file_to_file(source, cache_path) < 0) {
			debug(D_DEBUG, "copy_file_to_file from %s to %s failed.\n", source, cache_path);
			return -1;
		}
		break;
	case FILE_TYPE_LNK:
		if(copy_symlink(source, cache_path)) {
			debug(D_DEBUG, "copy_symlink from %s to %s failed.\n", source, cache_path);
			return -1;
		}
		break;
	case FILE_TYPE_DIR:
		if(copy_dir(source, cache_path)) {
			debug(D_DEBUG, "copy_dir from %s to %s failed.\n", source, cache_path);
			return -1;
		}
		break;
	default:
		break;
	}

	return 0;
}

/* mount_check checks the validity of source and target.
 * It also sets the file type when source is a local path.
 * @param source: the source location of a dependency, which can be a local file path or http URL.
 * @param target: a local file path which must not exist.
 * @param s_type: the file type of source
 * return 0 on success; return -1 on failure.
 */
int mount_check(const char *source, const char *target, file_type *s_type) {
	if(!source || !*source) {
		debug(D_DEBUG, "the source (%s) can not be empty!\n", source);
		fprintf(stderr, "the source (%s) can not be empty!\n", source);
		return -1;
	}

	if(!target || !*target) {
		debug(D_DEBUG, "the target (%s) can not be empty!\n", target);
		fprintf(stderr, "the target (%s) can not be empty!\n", target);
		return -1;
	}

	/* Check whether the target is an absolute path. */
	if(target[0] == '/') {
		debug(D_DEBUG, "the target (%s) should not be an absolute path!\n", target);
		fprintf(stderr, "the target (%s) should not be an absolute path!\n", target);
		return -1;
	}

	/* check whether target includes .. */
	if(path_has_doubledots(target)) {
		debug(D_DEBUG, "the target (%s) include ..!\n", target);
		fprintf(stderr, "the target (%s) include ..!\n", target);
		return -1;
	}

	if(!strncmp(source, "http://", 7)) {
		return mount_check_http(source);
	} else if(strncmp(source, "https://", 8)) {
		/* check source when it is not http or https */
		/* Check whether source already exists. */
		if(access(source, F_OK)) {
			debug(D_DEBUG, "the source (%s) does not exist!\n", source);
			fprintf(stderr, "the source (%s) does not exist!\n", source);
			return -1;
		}

		/* check whether source is REG, LNK, DIR */
		if((*s_type = check_file_type(source)) == FILE_TYPE_UNSUPPORTED) {
			fprintf(stderr, "source should be regular file, link, or dir!\n");
			return -1;
		}

		/* check whether source is an ancestor directory of target */
		if(is_subdir(source, target)) {
			debug(D_DEBUG, "source (%s) is an ancestor of target (%s), and can not be copied into target!\n", source, target);
			fprintf(stderr, "source (%s) is an ancestor of target (%s), and can not be copied into target!\n", source, target);
			return -1;
		}
	}

	return 0;
}

/* md5_cal_source calculates the checksum of a file path.
 * @param source: the source location of a dependency, which can be a local file path or http URL.
 * @is_local: whether source is a lcoal path.
 */
char *md5_cal_source(const char *source, int is_local) {
	char *cache_name = NULL;

	if(is_local) {
		char *s_real = NULL;

		/* for local path, calculate the checksum of its realpath */
		s_real = realpath(source, NULL);
		if(!s_real) {
			debug(D_DEBUG, "realpath(`%s`) failed: %s!\n", source, strerror(errno));
			return NULL;
		}

		cache_name = md5_cal(s_real);

		if(!cache_name) {
			debug(D_DEBUG, "md5_cal(%s) failed: %s!\n", s_real, strerror(errno));
		}
		free(s_real);
	} else {
		cache_name = md5_cal(source);
	}
	return cache_name;
}

/* amend_cache_path adds ../ in front of cache_path.
 * @param cache_path: a file path.
 * @param depth: how many `../` should be added.
 */
char *amend_cache_path(char *cache_path, int depth) {
	char *p = NULL, *t = NULL;
	char *link_cache_path = NULL;
	int i;

	if(depth <= 0) return cache_path;

	/* calculate how many ../ should be attached to cache_path. */
	p = malloc(sizeof(char) * depth*3);
	if(!p) {
		debug(D_DEBUG, "malloc failed: %s!\n", strerror(errno));
		return NULL;
	}

	t = p;
	for(i=0; i<depth; i++) {
		*t++ = '.';
		*t++ = '.';
		*t++ = '/';
	}
	*(--t) = '\0';

	link_cache_path = path_concat(p, cache_path);
	if(!link_cache_path) {
		debug(D_DEBUG, "malloc failed: %s!\n", strerror(errno));
	}

	free(p);
	return link_cache_path;
}


/* mount_install copies source to target.
 * @param source: the source location of a dependency, which can be a local file path or http URL.
 * @param target: a local file path which must not exist.
 * @param cache_dir: the dirname of the cache used to store all the dependencies specified in a mountfile.
 * @param df: a dag_file structure
 * @param type: the mount type
 * return 0 on success; return -1 on failure.
 */
int mount_install(const char *source, const char *target, const char *cache_dir, struct dag_file *df, dag_file_source_t *type) {
	char *cache_name = NULL;;
	char *cache_path = NULL;
	char *dirpath = NULL, *p = NULL;
	int depth;
	file_type s_type;

	/* check the validity of source and target */
	if(mount_check(source, target, &s_type)) {
		debug(D_DEBUG, "mount_check(%s, %s) failed: %s!\n", source, target, strerror(errno));
		return -1;
	}

	/* set up the type of the source: https, http or local */
	if(!strncmp(source, "https://", 8)) {
		*type = DAG_FILE_SOURCE_HTTPS;
	} else if(!strncmp(source, "http://", 7)) {
		*type = DAG_FILE_SOURCE_HTTP;
	} else {
		*type = DAG_FILE_SOURCE_LOCAL;
	}

	/* calculate the filename in the cache dir */
	cache_name = md5_cal_source(source, *type == DAG_FILE_SOURCE_LOCAL);
	if(!cache_name) {
		debug(D_DEBUG, "md5_cal_source(%s) failed: %s!\n", source, strerror(errno));
		return -1;
	}

	cache_path = path_concat(cache_dir, cache_name);
	if(!cache_path) {
		free(cache_name);
		return -1;
	}

	/* if cache_path does not exist, copy it from source to cache_path. */
	if(access(cache_path, F_OK)) {
		int r = 0;
		if(*type == DAG_FILE_SOURCE_HTTPS || *type == DAG_FILE_SOURCE_HTTP) {
			r = mount_install_http(source, cache_path);
		} else {
			r = mount_install_local(source, target, cache_path, s_type);
		}

		if(r) {
			free(cache_name);
			free(cache_path);
			return r;
		}
	}

	if(!df->cache_name) df->cache_name = cache_name;

	/* calculate the depth of target relative to CWD. For example, if target = "a/b/c", path_depth returns 3. */
	depth = path_depth(target);
	if(depth < 1) {
		debug(D_DEBUG, "path_depth(%s) failed!\n", target);
		return -1;
	}
	debug(D_DEBUG, "path_depth(%s) = %d!\n", target, depth);

	/* Create the parent directories for target.
	 * If target is "dir1/dir2/file", then create dir1 and dir2 using `mkdir -p dir1/dir2`.
	 */
	p = xxstrdup(target);

	dirpath = dirname(p); /* Please do NOT free dirpath, free p instead. */
	if(access(dirpath, F_OK) && !create_dir(dirpath, 0755)) {
		free(p);
		debug(D_DEBUG, "failed to create the parent directories of the target (%s)!\n", target);
		return -1;
	}
	free(p);

	/* if target already exists, do nothing here. */
	if(!access(target, F_OK)) {
		free(cache_path);
		return 0;
	}

	/* link target to the file in the cache dir */
	if(depth == 1) {
		if(create_link(cache_path, target)) {
			debug(D_DEBUG, "create_link(%s, %s) failed!\n", cache_path, target);
			free(cache_path);
			return -1;
		}
		free(cache_path);
		return 0;
	}

	/* if target does not exist, construct the link_target of target so that target points to the file under the cache_dir. */
	if(link(cache_path, target)) {
		char *link_cache_path = NULL;

		debug(D_DEBUG, "link(%s, %s) failed: %s!\n", cache_path, target, strerror(errno));

		/* link_cache_path must not equals to cache_path, because depth here is > 1. */
		link_cache_path = amend_cache_path(cache_path, depth-1);
		if(!link_cache_path) {
			debug(D_DEBUG, "amend_cache_path(%s, %d) failed: %s!\n", cache_path, depth, strerror(errno));
			free(cache_path);
			return -1;
		}
		free(cache_path);

		if(create_link(link_cache_path, target)) {
			debug(D_DEBUG, "create_link(%s, %s) failed!\n", link_cache_path, target);
			free(link_cache_path);
			return -1;
		}
		free(link_cache_path);
		return 0;
	} else {
		free(cache_path);
		return 0;
	}
}

int makeflow_mounts_parse_mountfile(const char *mountfile, struct dag *d) {
	FILE *f;
	char line[PATH_MAX*2 + 1]; /* each line of the mountfile includes the target path, a space and the source path. */
	size_t lineno = 0;
	int err_num = 0;

	debug(D_MAKEFLOW, "The --mounts option: %s\n", mountfile);

	if(access(mountfile, F_OK)) {
		debug(D_DEBUG, "the mountfile (%s) does not exist!\n", mountfile);
		return -1;
	}

	f = fopen(mountfile, "r");
	if(!f) {
		debug(D_DEBUG, "couldn't open mountfile (%s): %s\n", mountfile, strerror(errno));
		return -1;
	}

	while(fgets(line, sizeof(line), f)) {
		char target[PATH_MAX], source[PATH_MAX];
		char *p;
		struct dag_file *df;
		file_type s_type;

		lineno++;

		if(line[0] == '\n') {
			continue;
		}

		if(line[0] == '#') {
			debug(D_MAKEFLOW, "line %zu is a comment: %s", lineno, line);
			continue;
		}

		debug(D_MAKEFLOW, "Processing line %zu of the mountfile: %s", lineno, line);
		if(sscanf(line, "%s %s", target, source) != 2) {
			debug(D_DEBUG, "The %zuth line of the mountfile (%s) has an error! The correct format is: <target> <source>\n", lineno, mountfile);
			fclose(f);
			return -1;
		}

		path_remove_trailing_slashes(source);
		path_remove_trailing_slashes(target);

		/* set up dag_file->source */
		df = dag_file_from_name(d, target);
		if(!df) {
			debug(D_MAKEFLOW, "%s is not in the dag_file list", target);
			continue;
		}

		if(mount_check(source, target, &s_type)) {
			debug(D_DEBUG, "mount_check(%s, %s) failed: %s!\n", source, target, strerror(errno));
			err_num++;
			continue;
		}

		p = xxstrdup(source);

		/* df->source may already be set based on the information from the makeflow log file, so free it first. */
		if(df->source) free(df->source);

		df->source = p;
	}

	if(fclose(f)) {
		debug(D_DEBUG, "fclose(`%s`) failed: %s!\n", mountfile, strerror(errno));
		return -1;
	}

	if(err_num) return -1;

	return 0;
}

/* check_cache_dir checks the validity of the cache dir, and create it if possible.
 * @param cache: a file path.
 * return 0 on success, return non-zero on failure.
 */
int check_cache_dir(const char *cache) {
	struct stat st;

	/* Checking principle: the cache must locate under the CWD. */
	if(!cache || !*cache) {
		debug(D_DEBUG, "the cache (%s) can not be empty!\n", cache);
		fprintf(stderr, "the cache (%s) can not be empty!\n", cache);
		return -1;
	}

	/* Check whether the cache is an absolute path. */
	if(cache[0] == '/') {
		debug(D_DEBUG, "the cache (%s) should not be an absolute path!\n", cache);
		fprintf(stderr, "the cache (%s) should not be an absolute path!\n", cache);
		return -1;
	}

	/* check whether cache includes .. */
	if(path_has_doubledots(cache)) {
		debug(D_DEBUG, "the cache (%s) include ..!\n", cache);
		fprintf(stderr, "the cache (%s) include ..!\n", cache);
		return -1;
	}

	/* check whether cache includes any symlink link, this check prevent the makeflow breaks out the CWD. */
	if(path_has_symlink(cache)) {
		debug(D_DEBUG, "the cache (%s) should not include any symbolic link!\n", cache);
		fprintf(stderr, "the cache (%s) should not include any symbolic link!\n", cache);
		return -1;
	}

	/* Check whether cache already exists. */
	if(access(cache, F_OK)) {
		mode_t default_dirmode = S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH;
		if(errno != ENOENT) {
			debug(D_DEBUG, "access(%s) failed: %s\n", cache, strerror(errno));
			return -1;
		}

		debug(D_DEBUG, "the cache (%s) does not exist, creating it ...\n", cache);
		if(mkdir_recursive(cache, default_dirmode)) {
			debug(D_DEBUG, "mkdir_recursive(%s) failed: %s\n", cache, strerror(errno));
			return -1;
		}
		return 0;
	}

	/* check whether cache is a dir */
	if(lstat(cache, &st) == 0) {
		if(!S_ISDIR(st.st_mode)) {
			debug(D_DEBUG, "the cache (%s) should be a dir!\n", cache);
			fprintf(stderr, "the cache (%s) should be a dir!\n", cache);
			return -1;
		}
	} else {
		debug(D_DEBUG, "lstat(%s) failed: %s!\n", cache, strerror(errno));
		fprintf(stderr, "lstat(%s) failed: %s!\n", cache, strerror(errno));
		return -1;
	}

	return 0;
}

int makeflow_mounts_install(struct dag *d) {
	struct list *list;
	struct dag_file *df;

	if(!d) return 0;

	/* log the cache dir info */
	makeflow_log_cache_event(d, d->cache_dir);

	list = dag_input_files(d);
	if(!list) return 0;

	list_first_item(list);
	while((df = (struct dag_file *)list_next_item(list))) {
		dag_file_source_t type;
		if(!df->source)
			continue;

		if(mount_install(df->source, df->filename, d->cache_dir, df, &type)) {
			list_delete(list);
			return -1;
		}

		/* log the dependency */
		makeflow_log_mount_event(d, df->filename, df->source, df->cache_name, type);
	}
	list_delete(list);
	return 0;
}

/* check_link_relation checks whether s is a hardlink or symlink to t.
 * @param s: an existing file path
 * @param t: an existing file path
 * return 0 if s is hardlink or symlink to t; otherwise return non-zero.
 */
int check_link_relation(const char *s, const char *t) {
	struct stat st_s, st_t;

	if(stat(s, &st_s)) {
		fprintf(stderr, "lstat(%s) failed: %s!\n", s, strerror(errno));
		return -1;
	}

	if(stat(t, &st_t)) {
		fprintf(stderr, "lstat(%s) failed: %s!\n", t, strerror(errno));
		return -1;
	}

	if(st_s.st_ino == st_t.st_ino) return 0;
	else return -1;
}

int makeflow_mount_check_consistency(const char *target, const char *source, const char *source_log, const char *cache_dir, const char *cache_name) {
	char *cache_path;

	/* check whether the <source> field in the mountfile matches the <source> field in the log file */
	if(strcmp(source, source_log)) {
		fprintf(stderr, "The <source> field in the mountfile (%s)  and the <source> field in the makeflow log file (%s) for the <target (%s) does not match!\n", source, source_log, target);
		return -1;
	}

	cache_path = path_concat(cache_dir, cache_name);
	if(!cache_path) {
		debug(D_DEBUG, "malloc failed: %s!\n", strerror(errno));
		return -1;
	}

	/* check whether the file already exists under the cache_dir */
	if(access(cache_path, F_OK)) { /* cache_path does not exist */
		/* If cache_path does not exist, the target must not exist. */
		if(!access(target, F_OK)) {
			fprintf(stderr, "The file (%s) already exists, and can not be specified in the mountfile!\n", target);
			free(cache_path);
			return -1;
		}
	} else { /* cache_path already exists */
		/* If cache_path already exists, the target can either point to cache_path or not exist. */
		if(!access(target, F_OK)) { /* the target already exists */
			/* check wether the target is a hard link or symlink to cache_path */
			if(check_link_relation(target, cache_path)) {
				fprintf(stderr, "The file (%s) already exists and is not a hard link or symlink to the cache file (%s)!\n", target, cache_path);
				free(cache_path);
				return -1;
			}
		}
	}

	free(cache_path);
	return 0;
}

/* check_mount_target checks whether the validity of the target of each mount entry.
 * @param d: a dag structure
 * return 0 on success; return non-zero on failure.
 */
int makeflow_mount_check_target(struct dag *d) {
	struct list *list;
	struct dag_file *df;

	if(!d) return 0;

	/* if --cache is not specified, and the log file does not include cache dir info, create the cache_dir. */
	if(d->cache_dir) {
		/* check the validity of the cache_dir and create it if neccessary and feasible. */
		if(check_cache_dir(d->cache_dir)) {
			return -1;
		}

	} else {
		/* Create a unique cache dir */
		char *cache_dir = xxstrdup(".makeflow_cache.XXXXXX");

		if(mkdtemp(cache_dir) == NULL) {
			debug(D_DEBUG, "mkdtemp(%s) failed: %s\n", cache_dir, strerror(errno));
			free(cache_dir);
			return -1;
		}

		d->cache_dir = cache_dir;
	}

	/* check the validity of the target of each mount entry */
	list = dag_input_files(d);
	if(!list) return 0;

	list_first_item(list);
	while((df = (struct dag_file *)list_next_item(list))) {
		char *cache_name;
		if(!df->source)
			continue;

		cache_name = md5_cal_source(df->source, (strncmp(df->source, "http://", 7) && strncmp(df->source, "https://", 8)));
		if(!cache_name) {
			return -1;
		}

		if(makeflow_mount_check_consistency(df->filename, df->source, df->source, d->cache_dir, cache_name)) {
			free(cache_name);
			list_delete(list);
			return -1;
		}
		free(cache_name);
	}
	list_delete(list);
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
