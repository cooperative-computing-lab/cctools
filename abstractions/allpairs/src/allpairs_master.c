/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <fcntl.h>
#include <signal.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>

#include "debug.h"
#include "work_queue.h"
#include "fast_popen.h"
#include "text_array.h"
#include "ragged_array.h"
#include "hash_table.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "macros.h"

#include "allpairs_util.h"

#define EXAMPLE_LINE_MAX 4096
#define MAX_FILENAME_LEN 1024
#define DEFAULT_PORT 9068
#define USING_INNER_FUNCTION 0
#define USING_OUTER_FUNCTION 1
#define NO_COMPARE_FUNCTION 2

int total_done = 0;

int topLeftX;
int topLeftY;
int usingInnerFunc = 0;

char *allpairs_multicore = "allpairs_multicore";
char *compare_function = NULL;

struct block {
	int x1;
	int y1;
	int x2;
	int y2;
	struct block *next;
};

struct ragged_array setA;
struct ragged_array setB;

struct block *pCurrentBlock = NULL;

struct ragged_array read_in_set(const char *setdir) {
    int numset=0;
    char *setfile;
    char *tmpstr;
	char buffer[MAX_FILENAME_LEN];
	char filepath[MAX_FILENAME_LEN];
    int setarraysize;

    struct ragged_array setarray;

	// get location of set.list file
    setfile = (char*) malloc((strlen(setdir)+1+strlen("set.list")+1)*sizeof(char));
    if(setfile == NULL) {
		fprintf(stderr,"Allocating set name failed!\n"); 
		goto FAIL;
	}
    sprintf(setfile,"%s/set.list",setdir);
	
	
	// allocate char * array to store a list file names
	setarraysize = file_line_count(setfile);
	if(setarraysize == -1) goto FAIL;
	setarray = ragged_array_initialize(setarraysize);

	// read in each line
	tmpstr = buffer;
    FILE *setfileID = fopen(setfile, "r");
    if(!setfileID) {
		fprintf(stderr,"Couldn't open set %s!\n",setfile); 
		goto FAIL;
	}    
	
    while(!feof(setfileID)) {
		tmpstr = fgets(tmpstr, MAX_FILENAME_LEN, setfileID);
		if (tmpstr != NULL) {
            size_t last = strlen (tmpstr) - 1;
	    	if (tmpstr[last] == '\n') tmpstr[last] = '\0';
		} else {
			continue;
		}

		sprintf(filepath,"%s/%s",setdir,tmpstr);
		if(ragged_array_add_line(&setarray, filepath) != 0) {
			fprintf(stderr,"Allocating set[%i] failed!\n",numset); 
			goto FAIL;
		}
		
		numset++;
    }
    fclose(setfileID);

	if(numset == 0) {
		fprintf(stderr, "Error: Set file - %s is empty!\n", setfile);
		goto FAIL;
	}

	if(numset != setarraysize) {
		fprintf(stderr,"Error counting number of items in %s.\n", setfile); 
		goto FAIL;
	}

	goto SUCCESS;

FAIL:
	setarray.arr = NULL;
	setarray.row_count = 0;
	setarray.array_size = 0;

SUCCESS:
    return setarray;
}

int divide_block(struct block *p) {
	struct block *q;
	int width, height;
	int i;

	if(!p) return 0;

	width = p->x2 - p->x1 + 1;
	height = p->y2 - p->y1 + 1;

	if(width < 4 && height < 4) return 0;

	q = (struct block *)malloc(sizeof(struct block));
	if(!q) {
		fprintf(stderr, "Allocating block failed!\n");
		return -1;
	}

	q->x1 = p->x1;
	q->y1 = p->y1;
	q->x2 = p->x2;
	q->y2 = p->y2;

	if(width > height) {
		i = (int)(width/2);
		p->x2 = p->x1 + i - 1;
		q->x1 = p->x2 + 1;
	} else {
		i = (int)(height/2);
		p->y2 = p->y1 + i - 1;
		q->y1 = p->y2 + 1;
	}

	q->next = p->next;
	p->next = q;

	return 1;
}
	

int init_worklist(int n, int x1, int y1, int x2, int y2) {

	struct block *p;
	struct block *head;

	int i;
	int ret;
	int total = 0;

	p = (struct block *)malloc(sizeof(struct block));
	if(!p) {
		fprintf(stderr, "Allocating block failed!\n");
		return -1;
	}

	p->x1 = x1;
	p->y1 = y1;
	p->x2 = x2;
	p->y2 = y2;
	p->next = NULL;
	total++;

	head = p;

	for(i = 0; i < n; i++) {
		while(p) {
			ret = divide_block(p);
			if(ret == 1) {
				p = (p->next)->next;
				total++;
			} else if (ret == 0) {
				p = p->next;
			} else {
				return -1;
			}
		}
		p = head;
	}

	pCurrentBlock = head;
	return total;
}

// TODO for measurement
//FILE *tasklog;

int work_accept(struct work_queue_task * task)
{
    if (task->return_status != 0) return 0;
    fputs(task->output,stdout);
    fflush(stdout);
    total_done++;
	
	/**
	// for measurement only
	// taskid, computation time(including transfer result back), bytes_transfered, transfer_time
	fprintf(tasklog, "%d\t%.02lfs\t%lld\t%.02lfs\t%s\n", task->taskid, (task->finish_time-task->start_time)/1000000.0, task->total_bytes_transfered, task->total_transfer_time/1000000.0, task->command_line);
*/
					
    fprintf(stderr,"Completed task with command: %s\n",task->command_line);
    fprintf(stderr,"%i tasks done so far.\n",total_done);
    return 1;
}

void do_failure( struct work_queue_task * task)
{
    fprintf(stderr,"Task with command \"%s\" returned with return status: %i\n", task->command_line,task->return_status);
    // other applications may resubmit task or take other action, if desired
}

struct work_queue_task* work_create(struct block *q, char *setAdir, char *setBdir)
{
	int i;
	char input_file[EXAMPLE_LINE_MAX];
	char cmd[EXAMPLE_LINE_MAX];

	char *setAfile;
	char *setBfile;

	if(!q) return 0;
	
	// get location of set.list file
    setAfile = (char*) malloc((strlen(setAdir)+1+strlen("set.list")+1)*sizeof(char));
    if(setAfile == NULL) {
		fprintf(stderr,"Allocating set name failed!\n"); 
		return 0;
	}
    sprintf(setAfile,"%s/set.list",setAdir);


	// get location of set.list file
    setBfile = (char*) malloc((strlen(setBdir)+1+strlen("set.list")+1)*sizeof(char));
    if(setBfile == NULL) {
		fprintf(stderr,"Allocating set name failed!\n"); 
		return 0;
	}
    sprintf(setBfile,"%s/set.list",setBdir);



	sprintf(cmd, "./allpairs_multicore -i %d -j %d -k %d -l %d -X %d -Y %d -r setA.set.list setB.set.list %s", q->x1, q->y1, q->x2, q->y2, topLeftX, topLeftY, compare_function);
	struct work_queue_task* t = work_queue_task_create(cmd);
	fprintf(stderr,"Created task with command: %s\n",cmd);
	work_queue_task_specify_input_file(t, allpairs_multicore, "allpairs_multicore");
	if(usingInnerFunc != 1) {
		work_queue_task_specify_input_file(t, compare_function, compare_function);
	}
	work_queue_task_specify_input_file(t, setAfile, "setA.set.list");
	work_queue_task_specify_input_file(t, setBfile, "setB.set.list");

	for(i = q->x1; i <= q->x2; i++) {
		sprintf(input_file,"setA.%s", strrchr(setA.arr[i], '/')+1);
		debug(D_DEBUG, "specified %s as %s\n", setA.arr[i], input_file);
		work_queue_task_specify_input_file(t,setA.arr[i], input_file);
	}
	for(i = q->y1; i <= q->y2; i++) {
		sprintf(input_file,"setB.%s", strrchr(setB.arr[i], '/')+1);
		debug(D_DEBUG, "specified %s as %s\n", setB.arr[i], input_file);
		work_queue_task_specify_input_file(t,setB.arr[i], input_file);
	}
	
	free(setAfile);
	free(setBfile);
	pCurrentBlock = q->next;
	return t;
}

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help( const char *cmd )
{
	printf("Usage: %s [options] <set A> <set B> <compare function>\n",cmd);
	printf("The most common options are:\n");
	printf(" -d <string>	Enable debugging for this subsystem.\n");
	printf(" -v         	Show program version.\n");
	printf(" -h         	Display this message.\n");
	printf(" -p <integer>	The port that the Master will be listening on.\n");
	printf(" -i <integer>  	x coordinate of the start point of computation in the matrix. \n");
	printf(" -j <integer>  	y coordinate of the start point of computation in the matrix. \n");
	printf(" -k <integer>  	x coordinate of the end point of computation in the matrix. \n");
	printf(" -l <integer>  	y coordinate of the end point of computation in the matrix. \n");
	printf("\n");
	printf("Less common options are:\n");
	printf(" -x <integer>	Block width.  (default is chosen according to hardware conditions)\n");
	printf(" -y <integer>	Block height. (default is chosen according to hardware conditions)\n");
	printf(" -X <integer> 	x coordinate of starting point in a distributed context.\n");
	printf(" -Y <integer>  	y coordinate of starting point in a distributed context.\n");
	printf(" -c <integer>	Number of workers to be used.\n");
	printf(" -f 			Indicate that workqueue uses an inner compare function embedded in allpairs_multicore.\n");
}

// Test function starts below

void display_ragged_array(struct ragged_array *t) {
	int i;

	if(!t) return;

	printf("Array size: %d; Elements are as follow:\n", t->array_size);
	for(i = 0; i < t->array_size; i++) {
		printf("\t%s\n", t->arr[i]);
	}
	printf("\n");
}

void display_work_list(){
	struct block *p;

	p = pCurrentBlock;

	while(p) {
		debug(D_DEBUG, "[%d, %d]\t[%d, %d]\n", p->x1, p->y1, p->x2, p->y2);
		p = p->next;
	}
}

int main(int argc, char** argv) { // or other primary control function.
    int i, sum=0;
	int ret;
	char c;
    time_t short_timeout = 10;
    struct work_queue *q;
    struct work_queue_task *task;

	int setAindex, setBindex, funcindex;
	
	char *setAfile;
	char *setBfile;
	int x1, y1, x2, y2;
	int numOfStableElements;
	int numOfMovingElements;
	int numOfWorkers = 0;
	int port = DEFAULT_PORT;

	x1 = y1 = x2 = y2 = -1;
	
    while((c=getopt(argc,argv,"d:vhx:p:y:i:j:k:l:X:Y:c:"))!=(char)-1) {
			switch(c) {
				case 'd':
					debug_flags_set(optarg);
					break;
				case 'v':
					show_version(argv[0]);
					exit(0);
					break;
				case 'h':
					show_help(argv[0]);
					exit(0);
					break;
				case 'x':
					numOfStableElements = atoi(optarg);
					break;
				case 'y':
					numOfMovingElements = atoi(optarg);
					break;
				case 'i':
					x1 = atoi(optarg);
					break;
				case 'j':
					y1 = atoi(optarg);
					break;
				case 'k':
					x2 = atoi(optarg);
					break;
				case 'l':
					y2 = atoi(optarg);
					break;
				case 'X':
					topLeftX = atoi(optarg);
					break;
				case 'Y':
					topLeftY = atoi(optarg);
            		break;
        		case 'c':
            		numOfWorkers = atoi(optarg);
            		break;
        		case 'p':
            		port = atoi(optarg);
            		break;
			}
    }
	
    if( (argc-optind)<3 ) {
		fprintf(stderr, "allpairs_multicore: after all options, you must have: setA setB function\n");
		exit(1);
    }
	
	// TODO measurement only
	//tasklog = fopen("task_stats.log", "w");

    setAindex = optind;
    setBindex = optind+1;
    funcindex = optind+2;

    setA = read_in_set(argv[setAindex]);
    setB = read_in_set(argv[setBindex]);
	compare_function = argv[funcindex];

	// Check if the compare function exists. 
	FILE *tmpresult;
	int function_flag;
	char cmdrun[256];
	sprintf(cmdrun, "allpairs_multicore -f setA.set.list setB.set.list %s", compare_function);
	if((tmpresult = fast_popen(cmdrun)) == NULL){
		fprintf(stderr, "allpairs_master: Cannot execute allpairs_multicore. : %s\n", strerror(errno));
		exit(1);
	}
	fscanf(tmpresult, "%d", &function_flag);
	fast_pclose(tmpresult);

    if(function_flag == USING_INNER_FUNCTION) {
		usingInnerFunc = 1;
		debug(D_DEBUG, "Using inner function.\n");
	} else if(function_flag == USING_OUTER_FUNCTION) {
		usingInnerFunc = 0;
		debug(D_DEBUG, "Using outer function.\n");
	} else {
		// function_flag == NO_COMPARE_FUNCTION
		fprintf(stderr, "allpairs_master: no compare function is found, either internal or external.\n");
		exit(1);
	}


	// get location of set.list file
    setAfile = (char*) malloc((strlen(argv[setAindex])+1+strlen("set.list")+1)*sizeof(char));
    if(setAfile == NULL) {
		fprintf(stderr,"Allocating set name failed!\n"); 
		return -1;
	}
    sprintf(setAfile,"%s/set.list",argv[setBindex]);


	// get location of set.list file
    setBfile = (char*) malloc((strlen(argv[setBindex])+1+strlen("set.list")+1)*sizeof(char));
    if(setBfile == NULL) {
		fprintf(stderr,"Allocating set name failed!\n"); 
		return -1;
	}
    sprintf(setBfile,"%s/set.list",argv[setBindex]);


	validate_coordinates(setAfile, setBfile, &x1, &y1, &x2, &y2);
	debug(D_DEBUG, "validated coords: [%d, %d]\t[%d, %d]\n", x1, y1, x2, y2);

	free(setAfile);
	free(setBfile);

    ret = init_worklist(numOfWorkers, x1, y1, x2, y2);
	debug(D_DEBUG, "Number of tasks: %d. They are:\n", ret);
	//display_work_list();
	
    q = work_queue_create( port, time(0)+60 ); // create a queue
    if(!q) {	// if it could not be created
	    fprintf(stderr,"Could not create queue.\n");
	    return 1;
    }
	
	//work_queue_activate_fast_abort(q, 2);
    
    while(1) {
		while(work_queue_hungry(q)) { // while the work queue can support more tasks
	    	task = work_create(pCurrentBlock, argv[setAindex], argv[setBindex]);
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

	//fclose(tasklog);
    return 0;
}
