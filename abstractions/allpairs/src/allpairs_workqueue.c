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
#define MAXFILENAME 4096

int total_done = 0;

struct ragged_array {
    char** arr;
    int row_count;
    int current;
};

struct ragged_array getsetarray(char *setdir) {

    int len = MAXFILENAME;
    int numset=0;
    char* setfile;
    char* tmpstr;
    char** set;
    char** tmpptr;
    int setarraysize=10;

    struct ragged_array retset;
    retset.arr = NULL;
    retset.row_count = 0;

    tmpstr = (char *) malloc(MAXFILENAME * sizeof(char));
    if(tmpstr == NULL) {fprintf(stderr,"Allocating input string failed!\n"); return retset;}
    setfile = (char*) malloc((strlen(setdir)+1+strlen("set.list")+1)*sizeof(char));
    if(setfile == NULL) {fprintf(stderr,"Allocating set name failed!\n"); return retset;}
    set = (char **) malloc(setarraysize * sizeof(char *));
    if(set == NULL) {fprintf(stderr,"Allocating ragged array failed!\n"); return retset;}

    sprintf(setfile,"%s/set.list",setdir);
    FILE*  setfileID = fopen(setfile, "r");
    if(!setfileID) {fprintf(stderr,"Couldn't open set %s!\n",setfile); return retset;}    
    set[numset] = (char *) malloc(MAXFILENAME * sizeof(char));
    if(set[numset] == NULL) {fprintf(stderr,"Allocating set[%i] failed!\n",numset); return retset;}
    fgets(tmpstr, len, setfileID);
    if (tmpstr != NULL) {
	size_t last = strlen (tmpstr) - 1;
	if (tmpstr[last] == '\n') tmpstr[last] = '\0';
	//printf("set[%i] = %s\n", numset, set[numset]);
    }
    sprintf(set[numset],"%s/%s",setdir,tmpstr);
    
    while(!feof(setfileID)) {
	numset++;
	if(numset >= setarraysize) {
	    //printf("Preparing for setA element number %i, increasing setA element array from %i to %i\n", numset+1, setarraysize, 2*setarraysize);
	    tmpptr = realloc(set, (2 * setarraysize) * sizeof(char *));
	    if(tmpptr == NULL) {fprintf(stderr,"Realloc failed!\n"); return retset;}
	    else {set=tmpptr; setarraysize = 2*setarraysize;}
	}
	
	set[numset] = (char *) malloc(MAXFILENAME * sizeof(char));
	if(set[numset] == NULL) {fprintf(stderr,"Allocating set[%i] failed!\n",numset); return retset;}
	fgets(tmpstr, len, setfileID);
	if (tmpstr != NULL) {
            size_t last = strlen (tmpstr) - 1;
	    if (tmpstr[last] == '\n') tmpstr[last] = '\0';
	    //printf("set[%i] = %s\n", numset, set[numset]);
	}
	sprintf(set[numset],"%s/%s",setdir,tmpstr);
    }

    fclose(setfileID);

    retset.arr = set; 
    retset.row_count = numset;
    return retset;
}


int work_accept( struct work_queue_task * task, FILE* of)
{
    if (task->return_status != 0) return 0;
    fputs(task->output,of);
    fflush(of);
    total_done++;
    fprintf(stderr,"Completed task with command: %s\n",task->command_line);
    fprintf(stderr,"%i tasks done so far.\n",total_done);
    return 1;
}

void do_failure( struct work_queue_task * task)
{
    fprintf(stderr,"Task with command \"%s\" returned with return status: %i\n", task->command_line,task->return_status);
    // other applications may resubmit task or take other action, if desired
}

struct work_queue_task* work_create(char* e, struct ragged_array* a, struct ragged_array* b )
{

	char input_file_a[EXAMPLE_LINE_MAX];
	char input_file_b[EXAMPLE_LINE_MAX];
	char cmd[EXAMPLE_LINE_MAX];
	if(!(a->current < a->row_count && b->current < b->row_count)) return 0;

	sprintf(input_file_a,"afile.%i",a->current);
	sprintf(input_file_b,"bfile.%i",b->current);
	sprintf(cmd, "./exec.exe %s %s",input_file_a,input_file_b);
	struct work_queue_task* t = work_queue_task_create(cmd);
	fprintf(stderr,"Created task with command: %s\n",cmd);
	work_queue_task_specify_input_file(t,e,"exec.exe");
	work_queue_task_specify_input_file(t,a->arr[a->current],input_file_a);
	work_queue_task_specify_input_file(t,b->arr[b->current],input_file_b);

	b->current++;
	if(b->current >= b->row_count) {
		b->current%=b->row_count;
		a->current++;
	}
	
	return t;
}

int main(int argc, char** argv) { // or other primary control function.
    int i, sum=0;
    time_t short_timeout = 10;
    struct work_queue *q;
    struct work_queue_task *task;
    FILE* outfile;

    if(argc!=5) {
	fprintf(stderr,"Bad arguments, argc=%i.\n",argc);
	return 1;
    }

    char* executable = argv[1];
    struct ragged_array setA = getsetarray(argv[2]);
    setA.current = 0;
    struct ragged_array setB = getsetarray(argv[3]);
    setB.current = 0;
    outfile = fopen(argv[4],"w");
    if(!outfile) {
	    fprintf(stderr,"Could not open output file %s.\n",argv[4]);
	    return 1;
    }
    
    q = work_queue_create( 9068 , time(0)+60 ); // create a queue
    if(!q) // if it could not be created
    {
	    fprintf(stderr,"Could not create queue.\n");
	    return 1;
    }
    
    while(1) {
	while(work_queue_hungry(q)) { // while the work queue can support more tasks
	    task = work_create(executable,&setA,&setB);
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
	    if(work_accept(task,outfile)) {
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


