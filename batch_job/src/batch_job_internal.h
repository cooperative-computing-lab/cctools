/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_JOB_INTERNAL_H_
#define BATCH_JOB_INTERNAL_H_

#include <sys/stat.h>

#include <limits.h>
#include <stdlib.h>

#include "batch_job.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "unlink_recursive.h"
#include "hash_table.h"
#include "itable.h"

#define BATCH_JOB_LINE_MAX 8192

struct batch_queue_module {
	batch_queue_type_t type;
	char typestr[128];

	int (*create) (struct batch_queue *Q);
	int (*free) (struct batch_queue *Q);
	int (*port) (struct batch_queue *Q);
	void (*option_update) (struct batch_queue *Q, const char *what, const char *value); /* called when an option is changed */

	struct {
		batch_job_id_t (*submit) (struct batch_queue *Q, const char *command, const char *inputs, const char *outputs, struct jx *env_list, const struct rmsummary *resources);
		batch_job_id_t (*wait) (struct batch_queue *Q, struct batch_job_info *info, time_t stoptime);
		int (*remove) (struct batch_queue *Q, batch_job_id_t id);
	} job;

	struct {
		int (*chdir) (struct batch_queue *q, const char *path);
		int (*getcwd) (struct batch_queue *q, char *buf, size_t size);
		int (*mkdir) (struct batch_queue *q, const char *path, mode_t mode, int recursive);
		int (*putfile) (struct batch_queue *q, const char *lpath, const char *rpath);
		int (*rename) (struct batch_queue *q, const char *lpath, const char *rpath);
		int (*stat) (struct batch_queue *q, const char *path, struct stat *buf);
		int (*unlink) (struct batch_queue *q, const char *path);
	} fs;
};

struct batch_queue {
	batch_queue_type_t type;

	char logfile[PATH_MAX];
	struct hash_table *options;
	struct hash_table *features;
	struct itable *job_table;
	struct hash_table   *tv_file_table;
	struct vine_manager *tv_manager;
	struct work_queue   *wq_manager;
	const struct batch_queue_module *module;
};

#define batch_queue_stub_create(name)  static int batch_queue_##name##_create (struct batch_queue *Q) { return 0; }
#define batch_queue_stub_free(name)  static int batch_queue_##name##_free (struct batch_queue *Q) { return 0; }
#define batch_queue_stub_port(name)  static int batch_queue_##name##_port (struct batch_queue *Q) { return 0; }
#define batch_queue_stub_option_update(name)  static void batch_queue_##name##_option_update (struct batch_queue *Q, const char *what, const char *value) { return; }

#define batch_fs_stub_chdir(name)  static int batch_fs_##name##_chdir (struct batch_queue *Q, const char *path) { return chdir(path); }
#define batch_fs_stub_getcwd(name)  static int batch_fs_##name##_getcwd (struct batch_queue *Q, char *buf, size_t size) { getcwd(buf, size); return 0; }
#define batch_fs_stub_mkdir(name)  static int batch_fs_##name##_mkdir (struct batch_queue *Q, const char *path, mode_t mode, int recursive) { if (recursive) return create_dir(path, mode); else return mkdir(path, mode); }
#define batch_fs_stub_putfile(name)  static int batch_fs_##name##_putfile (struct batch_queue *Q, const char *lpath, const char *rpath) { return copy_file_to_file(lpath, rpath); }
#define batch_fs_stub_rename(name)  static int batch_fs_##name##_rename (struct batch_queue *Q, const char *lpath, const char *rpath) { return create_dir_parents(rpath, 0755) && !rename(lpath, rpath) ? 0 : -1; }
#define batch_fs_stub_stat(name)  static int batch_fs_##name##_stat (struct batch_queue *Q, const char *path, struct stat *buf) { return stat(path, buf); }
#define batch_fs_stub_unlink(name)  static int batch_fs_##name##_unlink (struct batch_queue *Q, const char *path) { return unlink_recursive(path); }

#endif

/* vim: set noexpandtab tabstop=8: */
