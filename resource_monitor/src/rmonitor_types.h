#if defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_FREEBSD)
  #include <sys/param.h>
  #include <sys/mount.h>
  #include <sys/resource.h>
#else
  #include  <sys/vfs.h>
#endif

#include <sys/types.h>
#include <sys/stat.h>

#ifdef HAS_SYS_STATFS_H
#include <sys/statfs.h>
#endif

#ifdef HAS_SYS_STATVFS_H
#include <sys/statvfs.h>
#endif

#include "path_disk_size_info.h"

#include "int_sizes.h"

#ifndef RMONITOR_TYPES_H
#define RMONITOR_TYPES_H

#define ONE_MEGABYTE 1048576  /* this many bytes */
#define ONE_SECOND   1000000  /* this many usecs */

#define MAX_FILE_DESCRIPTOR_COUNT 500 /* maximum depth of file tree walking */

//time in usecs, no seconds:
struct rmonitor_cpu_time_info
{
	uint64_t accumulated;
	uint64_t delta;
};

struct rmonitor_mem_info
{
	uint64_t virtual;
	uint64_t referenced;
	uint64_t resident;
	uint64_t swap;

	/* resident values, itemized. */
	uint64_t private;
	uint64_t shared;

	char    *map_name;
	uint64_t map_start;
	uint64_t map_end;

	uint64_t text;
	uint64_t data;
};

struct rmonitor_io_info
{
	uint64_t chars_read;
	uint64_t chars_written;

	uint64_t bytes_faulted;

	uint64_t delta_chars_read;
	uint64_t delta_chars_written;

	uint64_t delta_bytes_faulted;
};

struct rmonitor_file_info
{
	uint64_t n_references;
	uint64_t n_opens;
	uint64_t n_closes;
	uint64_t n_reads;
	uint64_t n_writes;
	int      is_output;
	off_t size_on_open;            /* in bytes */
	off_t size_on_close;           /* in bytes */
	dev_t device;
};


struct rmonitor_wdir_info
{
	char     *path;
	int      files;
	off_t    byte_count;

	struct path_disk_size_info *state;
	struct rmonitor_filesys_info *fs;
};

struct rmonitor_filesys_info
{
	int             id;
	char           *path;            // Sample path on the filesystem.
	struct statfs   disk;            // Current result of statfs call minus disk_initial.
	struct statfs   disk_initial;    // Result of the first time we call statfs.

	int initial_loaded_flag;         // Flag to indicate whether statfs has been called
									 // already on this fs (that is, whether disk_initial
									 // has a valid value).
};

struct rmonitor_process_info
{
	pid_t       pid;
	const char *cmd;
	int         running;
	int         waiting;

	struct rmonitor_mem_info      mem;
	struct rmonitor_cpu_time_info cpu;
	struct rmonitor_io_info       io;
	struct rmonitor_wdir_info    *wd;
};

#endif
