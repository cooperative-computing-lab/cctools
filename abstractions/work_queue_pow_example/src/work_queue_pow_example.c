#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <ctype.h>

#include <sys/stat.h>

#include "debug.h"
#include "work_queue.h"
#include "text_array.h"
#include "hash_table.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "macros.h"

#define EXAMPLE_LINE_MAX 4096

int total_done = 0;

struct pair {
    int m;
    int n;
};

struct pair getwork() {
    struct pair tmp;
    if(!feof(stdin)) {
	if(scanf("%i",&tmp.m) != 1)
	{
	    fprintf(stderr,"Invalid value for m.\n");
	    tmp.m = -1;
	    tmp.n = -1;
	}
	if(!feof(stdin)) {
	    if(scanf("%i",&tmp.n) != 1)
	    {
		fprintf(stderr,"Invalid value for n.\n");
		tmp.m = -1;
		tmp.n = -1;
	    }
	}
	else {
	    tmp.m = -1;
	    tmp.n = -1;
	}
    }
    else {
	tmp.m = -1;
	tmp.n = -1;
    }
    return tmp;
}

int work_accept( struct work_queue_task * task)
{
    if (task->return_status != 0) return 0;
    // other applications may:
    // check task->output file for correct output if desired
    // or
    // store task->output if desired
    total_done++;
    fprintf(stderr,"Completed task with command: %s\n",task->command_line);
    fprintf(stderr,"%i tasks done so far.\n",total_done);
    return 1;
}

void do_failure( struct work_queue_task * task)
{
    fprintf(stderr,"Task with command \"%s\" returend with return status: %i\n", task->command_line,task->return_status);
    // other applications may resubmit task or take other action, if desired
}

struct work_queue_task* work_create( )
{
	struct pair p=getwork(); //generate new work to do (generate, read in from file, prompt user, etc.).
	if(p.m == -1)
	    return NULL;
	char input_file[EXAMPLE_LINE_MAX];
	char output_file[EXAMPLE_LINE_MAX];	
	char input_data[EXAMPLE_LINE_MAX];
	char cmd[EXAMPLE_LINE_MAX];
	sprintf(input_file, "in_%i_%i.txt",p.m,p.n);
	sprintf(output_file, "out_%i_%i.txt",p.m,p.n);
	sprintf(input_data, "%i %i",p.m,p.n);
	sprintf(cmd, "./pow.exe < %s > out.txt",input_file);
	struct work_queue_task* t = work_queue_task_create(cmd);
	fprintf(stderr,"Created task with command: %s\n",cmd);
	work_queue_task_specify_input_file(t,"pow.exe","pow.exe");
	work_queue_task_specify_input_buf(t,input_data,strlen(input_data),input_file);
	work_queue_task_specify_output_file(t,"out.txt",output_file);
	return t;
}

int main() { // or other primary control function.
    int i, sum=0;
    time_t short_timeout = 10;
    struct work_queue *q;
    struct work_queue_task *task;
    
    q = work_queue_create( 9068 , time(0)+60 ); // create a queue
    if(!q) // if it could not be created
    {
	fprintf(stderr,"Could not create queue.\n");
	return 1;
    }
    
    while(1) {
	while(work_queue_hungry(q)) { // while the work queue can support more tasks
	    task = work_create();
	    if(!task) { // if we're out of work to do
		break;
	    }
	    else {
		work_queue_submit(q,task);
	    }
	}
	if(!task && work_queue_empty(q)) {
	    break; // we're out of work and there are no more tasks that the queue knows about that haven't been handled: we're done
	}
	
	task = work_queue_wait(q,short_timeout); //wait for a little while to see if there are any completed tasks
	if(task) { // if there were completed tasks, handle them.
	    if(work_accept(task)) {
		work_queue_task_delete(task); // delete the task structure. We've completed its work.
	    } else {
		do_failure(task); // handle the task's failure, perhaps by logging it or submitting a new task.
	    }
	}
    }

    for(i=0; i<10; i++)
	sum+=work_queue_shut_down_workers(q,0);
    fprintf(stderr,"%i workers shut down.\n",sum);
    work_queue_delete(q);

    return 0;
}


