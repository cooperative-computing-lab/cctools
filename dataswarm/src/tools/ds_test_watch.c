/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This example program shows the behavior of the DS_WATCH flag,
which allows dataswarm to progressively return the output of
a program that produces a graduate log file.  By simply adding
the DS_WATCH flag to the output of the program, dataswarm will
periodically check for output and return it to the manager
as each task runs.  Observe the files named output.0, output.1,
etc to see the output as it is created.
*/

#include "dataswarm.h"

#include "cctools.h"
#include "debug.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "itable.h"
#include "list.h"
#include "get_line.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

void ds_mainloop( struct ds_manager *q )
{
	struct ds_task *t;
	int i;

	for(i=0;i<10;i++) {
		char output[256];
		sprintf(output,"output.%d",i);
		t = ds_task_create("./ds_test_watch_trickle.sh");
		ds_task_specify_file(t, "ds_test_watch_trickle.sh", "ds_test_watch_trickle.sh", DS_INPUT, DS_CACHE );
		ds_task_specify_file(t, output, "output", DS_OUTPUT, DS_WATCH );
		ds_submit(q, t);
	}

	while(!ds_empty(q)) {
		t = ds_wait(q,5);
		if(t) ds_task_delete(t);
	}
}

/* vim: set noexpandtab tabstop=4: */
