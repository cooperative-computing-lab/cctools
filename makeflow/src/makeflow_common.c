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

#include "cctools.h"
#include "catalog_query.h"
#include "create_dir.h"
#include "copy_stream.h"
#include "work_queue_catalog.h"
#include "datagram.h"
#include "disk_info.h"
#include "domain_name_cache.h"
#include "link.h"
#include "macros.h"
#include "hash_table.h"
#include "itable.h"
#include "debug.h"
#include "work_queue.h"
#include "work_queue_internal.h"
#include "delete_dir.h"
#include "stringtools.h"
#include "load_average.h"
#include "get_line.h"
#include "int_sizes.h"
#include "list.h"
#include "xxmalloc.h"
#include "getopt_aux.h"
#include "rmonitor.h"
#include "random.h"
#include "path.h"

#include "dag.h"
#include "lexer.h"
#include "buffer.h"

#include "makeflow_common.h"

char *makeflow_exe = NULL;

void set_makeflow_exe(const char *makeflow_name)
{
	makeflow_exe = xxstrdup(makeflow_name);
}

const char *get_makeflow_exe()
{
	return makeflow_exe;
}








