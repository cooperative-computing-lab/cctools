/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "work_queue.h"

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


void work_queue_mainloop( struct work_queue *q )
{
	struct work_queue_task *t;
	int i;

	for(i=0;i<10;i++) {
		char output[256];
		sprintf(output,"output.%d",i);
		t = work_queue_task_create("./trickle.sh");
		work_queue_task_specify_file(t, "trickle.sh", "trickle.sh", WORK_QUEUE_INPUT, WORK_QUEUE_CACHE );
		work_queue_task_specify_file(t, output, "output", WORK_QUEUE_OUTPUT, WORK_QUEUE_WATCH );
		work_queue_submit(q, t);
	}


	work_queue_submit(q,t);

	while(!work_queue_empty(q)) {
		t = work_queue_wait(q,5);
		if(t) work_queue_task_delete(t);
	}
}

/* vim: set noexpandtab tabstop=4: */
