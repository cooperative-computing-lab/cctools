/*
Copyright (C) 2013- The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

#ifndef __RMSUMMARY_H
#define __RMSUMMARY_H

#include <stdlib.h>

#include "int_sizes.h"

// These fields are defined as signed integers, even though they
// will only contain positive numbers. This is to conversion to
// signed quantities when comparing to maximum limits.
struct rmsummary
{
	char    *command;

	int64_t  start;
	int64_t  end;

	char    *exit_type;
	int64_t  signal;
	char    *limits_exceeded;
	int64_t  exit_status;

	int64_t  wall_time;
	int64_t  max_concurrent_processes;
	int64_t  cpu_time;
	int64_t  virtual_memory; 
	int64_t  resident_memory; 
	int64_t  swap_memory; 
	int64_t  bytes_read;
	int64_t  bytes_written;
	int64_t  workdir_number_files_dirs;
	int64_t  workdir_footprint;

	int64_t  fs_nodes;
};

struct rmsummary_field
{
	char   *name;
	size_t  offset;
	int     type;
	union { uint64_t integer;
		double   real;
		char    *string;
	}       value;
};

/**  Reads a single summary file from filename **/
struct rmsummary *resource_monitor_parse_summary_file(char *filename);

void rmsummary_print(FILE *stream, struct rmsummary *s);



#endif
