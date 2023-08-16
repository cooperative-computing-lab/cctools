/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "process.h"
#include "list.h"

#include <sys/time.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/wait.h>

#include <unistd.h>
#include <stdlib.h>
#include <signal.h>

static struct list *complete_list = 0;

static void alarm_handler(int sig)
{
	// do nothing except interrupt the wait
}

static int process_work(int timeout)
{
	int flags = 0;
	struct sigaction new_action, old_action;

	if(timeout == 0) {
		flags = WNOHANG;
	} else {
		flags = 0;
		new_action.sa_handler = alarm_handler;
		sigemptyset(&new_action.sa_mask);
		new_action.sa_flags = 0;
		sigaction(SIGALRM, &new_action, &old_action);
		alarm(timeout);
	}

	struct process_info p;

	p.pid = wait4(-1, &p.status, flags, &p.rusage);

	if(timeout != 0) {
		alarm(0);
		sigaction(SIGALRM, &old_action, NULL);
	}

	if(p.pid <= 0) {
		return 0;
	}

	struct process_info *i = malloc(sizeof(*i));
	*i = p;

	list_push_tail(complete_list, i);

	return 1;
}

struct process_info *process_wait(int timeout)
{
	struct process_info *p;

	if(!complete_list)
		complete_list = list_create();

	p = list_pop_head(complete_list);
	if(p)
		return p;

	process_work(timeout);

	return list_pop_head(complete_list);
}

static int pid_compare( void *a, const void *b )
{
	pid_t *pa = (pid_t *)a;
	pid_t *pb = (pid_t *)b;

	return *pa==*pb;
}

struct process_info *process_waitpid( pid_t pid, int timeout)
{
	if(!complete_list)
		complete_list = list_create();

	do {
		struct process_info *p = list_find( complete_list, pid_compare, &pid );
		if(p) return list_remove(complete_list,(void*)p);

	} while(process_work(timeout));

	return 0;
}

int process_kill_waitpid(pid_t pid, int timeout)
{
	int current_signal = SIGTERM; // start by sending SIGTERM, move on to SIGKILL
	struct process_info* p; // return value from process_waitpid
	while (kill(pid, current_signal) == 0) { // send signal to process
		if (current_signal == SIGKILL) { // if we have already attempted to send SIGTERM and SIGKILL, we should give up
			break;
		}
		current_signal = SIGKILL; // on the next attempt, we will send a SIGKILL to the child process
		p = process_waitpid(pid, timeout);
		if (p) { // successfully waited process with given PID 
			free(p);
			return 1;
		}
		else // if 0 is returned, no such PID was waited for in timeout seconds
		{
			continue;
		}
	}
	return 0;
}

void process_putback(struct process_info *p)
{
	if(!complete_list)
		complete_list = list_create();

	list_push_tail(complete_list, p);
}

int process_pending()
{
	if(!complete_list)
		complete_list = list_create();

	if(list_size(complete_list) > 0)
		return 1;

	return process_work(0);
}

/* vim: set noexpandtab tabstop=8: */
