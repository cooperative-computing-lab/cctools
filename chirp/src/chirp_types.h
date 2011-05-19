/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file chirp_types.h
All of the structure and type definitions used by the Chirp API.
*/

#ifndef CHIRP_TYPES_H
#define CHIRP_TYPES_H

#include "int_sizes.h"
#include "chirp_protocol.h"

#include <sys/types.h>
#include <time.h>
#include <fcntl.h>

/** Describes the properties of a file, much like the Unix <tt>stat</tt> structure.
Note that @ref cst_uid, @ref cst_gid, and @ref cst_mode are provided for backwards compatibility, but are
ignored by Chirp when enforcing access control.  See @ref chirp_reli_setacl and @ref chirp_reli_getacl
for more information about enforcing access controls.
@see chirp_reli_stat, chirp_reli_lstat, chirp_reli_fstat, chirp_reli_open
*/

struct chirp_stat {
	INT64_T cst_dev;	/**< The device number on which the file is stored. */
	INT64_T cst_ino;	/**< The inode number of the file. */
	INT64_T cst_mode;	/**< The Unix mode bits of the file. */
	INT64_T cst_nlink;	/**< The number of hard links to this file. */
	INT64_T cst_uid;	/**< The Unix UID of the file's owner. */
	INT64_T cst_gid;	/**< The Unix GID of the file's group varship. */
	INT64_T cst_rdev;	/**< The device number, if this represents a device. */
	INT64_T cst_size;	/**< The size of the file, in bytes. */
	INT64_T cst_blksize;	/**< The recommended transfer block size for accessing this file.  */
	INT64_T cst_blocks;	/**< The number of blocks consumed by this file in the file system.  Note that this value has nothing to do with @ref cst_blksize.  The size of the actual storage blocks is given by <tt>f_bsize</tt> in @ref chirp_statfs. */
	INT64_T cst_atime;	/**< The last time the file was accessed, in <tt>time_t</tt> format. */
	INT64_T cst_mtime;	/**< The last time the file data was modified,  <tt>time_t</tt> format. */
	INT64_T cst_ctime;	/**< The last time the inode was changed, in <tt>time_t</tt> format. */
};

/** Describes the properties of a file system, much like the Unix <tt>statfs</tt> structure.
@see chirp_reli_statfs, chirp_reli_fstatfs
*/

struct chirp_statfs {
	INT64_T f_type;		/**< The integer type of the filesystem */
	INT64_T f_blocks;	/**< The total number of blocks in the filesystem. */
	INT64_T f_bavail;	/**< The number of blocks available to an ordinary user. */
	INT64_T f_bsize;	/**< The size in bytes of a block. */
	INT64_T f_bfree;	/**< The number of blocks free. */
	INT64_T f_files;	/**< The maximum number of files (inodes) on the filesystem. */
	INT64_T f_ffree;	/**< The number of files (inodes) currently in use. */
};

/** Describes a directory entry returned by @ref chirp_reli_readdir */

struct chirp_dirent {
	char *name;			/**< The name of the directory entry. */
	struct chirp_stat info;		/**< The properties of the directory entry. */
	struct chirp_dirent *next;
};

/** Describes the type of a bulk I/O operation. Used by @ref chirp_bulkio */

typedef enum {
	CHIRP_BULKIO_PREAD,  /**< Perform a chirp_reli_pread.*/
	CHIRP_BULKIO_PWRITE, /**< Perform a chirp_reli_pwrite.*/
	CHIRP_BULKIO_SREAD,  /**< Perform a chirp_reli_sread.*/
	CHIRP_BULKIO_SWRITE, /**< Perform a chirp_reli_swrite.*/
	CHIRP_BULKIO_FSTAT,  /**< Perform a chirp_reli_fstat.*/
	CHIRP_BULKIO_FSYNC,   /**< Perform a chirp_reli_fsync.*/
} chirp_bulkio_t;

/** Describes a bulk I/O operation.
An array of chirp_bulkio structures passed to @ref chirp_reli_bulkio describes a list of multiple operatons to be performed simultaneously.  Not all fields are relevant to all operations.
*/

struct chirp_bulkio {
	chirp_bulkio_t type;	   /**< The type of I/O to perform. */
	struct chirp_file *file;   /**< The file to access for all operations. */
	struct chirp_stat *info;   /**< Pointer to a data buffer for FSTAT */
	void *buffer;		   /**< Pointer to data buffer for PREAD, PWRITE, SREAD, and SWRITE */
	INT64_T length;		   /**< Length of the data, in bytes, for PREAD, WRITE, SREAD, and SWRITE. */
	INT64_T stride_length;	   /**< Length of each stride for SREAD and SWRITE. */
	INT64_T stride_skip;	   /**< Distance between start of each stride for SREAD and SWRITE. */
	INT64_T offset;		   /**< Starting offset in file for PREAD, PWRITE, SREAD, and SWRITE. */
	INT64_T result;		   /**< On completion, contains result of operation. */
	INT64_T errnum;		   /**< On failure, contains the errno for the call. */
};

/** Descibes the space consumed by a single user on a Chirp server.
@see chirp_reli_audit
*/

struct chirp_audit {
	char name[CHIRP_PATH_MAX];	/**< The identity of the user. */
	INT64_T nfiles;			/**< The number of files owned by that user. */
	INT64_T ndirs;			/**< The number of directories owned by that user. */
	INT64_T nbytes;			/**< The total bytes consumed by that user. */
};

/** Describes the progress of an active storage job through its lifetime.
@see chirp_job_state, chirp_reli_job_wait, chirp_reli_job_list
*/

typedef enum {
	CHIRP_JOB_STATE_BEGIN = 0,	/**< The job has been created, but not yet committed */
	CHIRP_JOB_STATE_IDLE = 1,	/**< The job has been committed, and is free to run. */
	CHIRP_JOB_STATE_RUNNING = 2,	/**< The job is currently running. */
	CHIRP_JOB_STATE_SUSPENDED = 3,	/**< The job has been temporarily suspended. */
	CHIRP_JOB_STATE_COMPLETE = 4,	/**< The job ran all the way to completion. */
	CHIRP_JOB_STATE_FAILED = 5,	/**< The job could not be run at all. */
	CHIRP_JOB_STATE_KILLED = 6,	/**< The job was forcibly killed by the owner or administrator. */
} chirp_job_state_t;

/** Gives a readable string corresponding to a job state.
@return A constant string corresponding to the job state, e.g. "BEGIN".
*/

const char *chirp_job_state_string(chirp_job_state_t state);

/** Describes the current state of an active storage job on a Chirp server.
@see chirp_reli_job_wait, chirp_reli_job_list
*/

struct chirp_job_state {
	INT64_T jobid;				/**< The unique job identifier */
	char command[CHIRP_PATH_MAX];		/**< The actual command to be run */
	char owner[CHIRP_PATH_MAX];		/**< The owner of the job */
	chirp_job_state_t state;		/**< The current state of the job. */
	int exit_code;				/**< If completed, the exit code of the job. */
	time_t submit_time;			/**< The time at which @ref chirp_reli_job_begin was invoked */
	time_t start_time;			/**< The time at which the job most recently began executing */
	time_t stop_time;			/**< The time at which the job completed or was aborted. */
	int pid;				/**< If running, the local process ID of the job. */
};

/** A callback function typedef used to display a directory or access control list.
A function matching this type is called by @ref chirp_reli_getdir
to display or otherwise act upon each line in a directory or access control list.
@param path The short name of the file, directory, or ACL to display.
@param arg  A convenience pointer corresponding to the <tt>arg</tt> passed from @ref chirp_reli_getdir.
@see chirp_reli_getdir, chirp_reli_getacl
*/

typedef void (*chirp_dir_t) (const char *path, void *arg);

/** A callback function typedef used to display a detailed directory.
A function matching this type is called by @ref chirp_reli_getlongdir
to display or otherwise act upon each line in a directory listing.
@param path The short name of the file or directory to display.
@param info The details of the named file.
@param arg  A convenience pointer corresponding to the <tt>arg</tt> passed from @ref chirp_reli_getlongdir.
@see chirp_reli_getlongdir
*/

typedef void (*chirp_longdir_t) (const char *path, struct chirp_stat * info, void *arg);

/** A callback function typedef used to display a job state.
A function matching this type is called by @ref chirp_reli_job_list
to display or otherwise act upon all active storage jobs in a server.
@param state The name and detailed state of one job.
@param arg  A convenience pointer corresponding to the <tt>arg</tt> passed from @ref chirp_reli_job_list
@see chirp_reli_job_list
*/

typedef void (*chirp_joblist_t) (struct chirp_job_state * state, void *arg);

/** A callback function typedef used to display a file's location(s).
A function matching this type is called by @ref chirp_reli_locate
to display or otherwise act upon each location at which a given file is stored.
@param location The location of a file (usually hostname:local_path).
@param arg  A convenience pointer corresponding to the <tt>arg</tt> passed from @ref chirp_reli_locate.
@see chirp_reli_locate
*/

typedef void (*chirp_loc_t) (const char *location, void *arg);

#endif
