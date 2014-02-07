/*
 * Copyright (C) 2013 The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef CHIRP_FS_LOCAL_SCHEDULER_H
#define CHIRP_FS_LOCAL_SCHEDULER_H

#include "chirp_job.h"
#include "chirp_sqlite.h"

int chirp_fs_local_job_dbinit (sqlite3 *db);
int chirp_fs_local_job_postcommit (sqlite3 *db, chirp_jobid_t id);
int chirp_fs_local_job_schedule (sqlite3 *db);

#endif
