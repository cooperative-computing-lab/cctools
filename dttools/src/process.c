/*
Copyright (C) 2008- The University of Notre Dame
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

int process_kill_waitpid(pid_t pid, int timeout, int max_attempts)
{
	int flags = 0;
        struct sigaction new_action, old_action;

	if(timeout == 0) {
                flags = WNOHANG; // if the timeout is zero, we want waitpid to return immediately
        } else { // set up the signal handler for alarm to get us out of wait
                flags = 0;
                new_action.sa_handler = alarm_handler;
                sigemptyset(&new_action.sa_mask);
                new_action.sa_flags = 0;
                sigaction(SIGALRM, &new_action, &old_action);
        }
	int num_attempts = 0; // keep track of how many time we attempted to signal process pid
	int current_signal = SIGTERM; // start by sending SIGTERM, move on to SIGKILL
	int status; 
	int ret_val = 1; // return value from this function: 0 means process exited cleanly, 1 means there was an error
	while (kill(pid, current_signal) == 0) // send signal to process
	{
		if (timeout != 0) {alarm(timeout);} // and set an alarm for timeout seconds
		num_attempts++;
		if (num_attempts == max_attempts) // change to sending SIGKILL after max_attempts tries
		{
			current_signal = SIGKILL;
		}
		else if (num_attempts >= max_attempts * 2) // and give up after 2 * max_attempt tries
		{
			break;
		}
        	pid_t return_pid = waitpid(pid, &status, flags); // we will wait for the process to return for timeout seconds before the alarm signal is sent and we break out of wait
		if (return_pid < 0) // handle errors
		{
			if (errno == EINTR) // this is the case that the alarm woke us up so we try again to signal
			{
				continue;
			}
			else // if there is any other error, it is fatal and we should just exit
			{
				break;
			}
		}
		else if (return_pid == 0) // if the return value is 0, it means we set the timeout to zero and nothing changed, so we try to signal and wait for the process again
		{
			continue;
		}
		else // if we get a positive return value, the process exited successfully
		{
			ret_val = 0;
			break;
		}
	}
        if(timeout != 0) { // clear any alarms that are currently ongoing and reset the alarm signal handler before we exit
                alarm(0);
                sigaction(SIGALRM, &old_action, NULL);
        }

        return ret_val;	
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

/* vim: set noexpandtab tabstop=4: */
