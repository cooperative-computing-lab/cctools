/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "process.h"
#include "list.h"

#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>

static struct list * complete_list = 0;

static void alarm_handler( int sig )
{
	// do nothing except interrupt the wait
}

static int process_work( int timeout )
{
	void *old_handler = 0;
	int flags = 0;

	if(timeout==0) {
		flags = WNOHANG;
	} else {
		flags = 0;
		old_handler = signal(SIGALRM,alarm_handler);
		alarm(timeout);
	}

	struct process_info p;

	p.pid = wait4(-1,&p.status,flags,&p.rusage);
	if(p.pid<=0) return 0;

	struct process_info *i = malloc(sizeof(*i));
	*i = p;

	list_push_tail(complete_list,i);

	return 1;
}

struct process_info * process_wait( int timeout )
{
	struct process_info *p;

	if(!complete_list) complete_list = list_create();

	p = list_pop_head(complete_list);
	if(p) return p;

	process_work(timeout);

	return list_pop_head(complete_list);
}

void process_putback( struct process_info *p )
{
	if(!complete_list) complete_list = list_create();

	list_push_tail(complete_list,p);	
}

int process_pending()
{
	if(!complete_list) complete_list = list_create();

	if(list_size(complete_list)>0) return 1;

	return process_work(0);
}
