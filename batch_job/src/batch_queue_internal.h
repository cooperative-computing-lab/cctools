/*
Copyright (C) 2024 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_JOB_INTERNAL_H_
#define BATCH_JOB_INTERNAL_H_

#include <sys/stat.h>

#include <limits.h>
#include <stdlib.h>

#include "batch_queue.h"
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

	batch_queue_id_t (*submit) (struct batch_queue *Q, struct batch_job *bt );
	batch_queue_id_t (*wait) (struct batch_queue *Q, struct batch_job_info *info, time_t stoptime);
	int (*remove) (struct batch_queue *Q, batch_queue_id_t id);
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

#endif

/* vim: set noexpandtab tabstop=8: */
