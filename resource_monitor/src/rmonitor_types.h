#if defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_FREEBSD)
  #include <sys/param.h>
  #include <sys/mount.h>
  #include <sys/resource.h>
#else
  #include  <sys/vfs.h>
#endif

#include <sys/types.h>
#include <sys/stat.h>

#ifdef HAS_FTS_H
#include <fts.h>
#endif

#include "int_sizes.h"

#ifndef RMONITOR_TYPES_H
#define RMONITOR_TYPES_H

#define ONE_MEGABYTE 1048576  /* this many bytes */
#define ONE_SECOND   1000000  /* this many usecs */    

//time in usecs, no seconds:
struct cpu_time_info
{
	uint64_t accumulated;
	uint64_t delta;
};

struct mem_info
{
	uint64_t virtual; 
	uint64_t resident;
	uint64_t swap;
	uint64_t shared;
	uint64_t text;
	uint64_t data;
};

struct io_info
{
	uint64_t chars_read;
	uint64_t chars_written;

	uint64_t bytes_faulted;

	uint64_t delta_chars_read;
	uint64_t delta_chars_written;

	uint64_t delta_bytes_faulted;
};

struct wdir_info
{
	char     *path;
	int      files;
	int      directories;
	off_t    byte_count;
	blkcnt_t block_count;

	struct filesys_info *fs;
};

struct filesys_info
{
	int             id;
	char           *path;            // Sample path on the filesystem.
	struct statfs   disk;            // Current result of statfs call minus disk_initial.
	struct statfs   disk_initial;    // Result of the first time we call statfs.

	int initial_loaded_flag;         // Flag to indicate whether statfs has been called
	                                 // already on this fs (that is, whether disk_initial
	                                 // has a valid value).
};

struct process_info
{
	pid_t       pid;
	const char *cmd;
	int         running;
	int         waiting;

	struct mem_info      mem;
	struct cpu_time_info cpu;
	struct io_info       io;

	struct wdir_info *wd;
};

#endif
