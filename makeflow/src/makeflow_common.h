/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>
#include <stdarg.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>

#include "dag.h"
#include "visitors.h"

#ifndef MAKEFLOW_COMMON_H
#define MAKEFLOW_COMMON_H


int dag_depth(struct dag *d);
int dag_width_uniform_task(struct dag *d);
int dag_width_guaranteed_max(struct dag *d);
int dag_width(struct dag *d, int nested_jobs);

struct dag *dag_from_file(const char *filename);

void set_makeflow_exe(const char *makeflow_name);
const char *get_makeflow_exe(void);

#endif

/* vim: set noexpandtab tabstop=4: */
