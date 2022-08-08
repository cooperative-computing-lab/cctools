#include "ds_gpus.h"
#include "ds_resources.h"
#include "buffer.h"
#include "debug.h"

extern struct ds_resources *total_resources;

/* Array tracks which task is assigned to each GPU. */
static int *gpu_to_task = 0;

/*
Initialize the GPU tracking state.
Note that this may be called many times,
but should only initialized once.
*/

void ds_gpus_init( int ngpus )
{
	if(!gpu_to_task) gpu_to_task = calloc(ngpus,sizeof(int));
}

/*
Display the GPUs associated with each task.
*/

void ds_gpus_debug()
{
	buffer_t b;
	buffer_init(&b);
	buffer_putfstring(&b,"GPUs Assigned to Tasks: [ ");
	int i;
	for(i=0;i<total_resources->gpus.total;i++) {
		buffer_putfstring(&b,"%d ",gpu_to_task[i]);
	}
	buffer_putfstring(&b," ]");
	debug(D_WQ,"%s",buffer_tostring(&b));
	buffer_free(&b);
}

/*
Free all of the GPUs associated with this taskid.
*/

void ds_gpus_free( int taskid )
{
	int i;
	for(i=0;i<total_resources->gpus.total;i++) {
		if(gpu_to_task[i]==taskid) {
			gpu_to_task[i] = 0;
		}
	}
}

/*
Allocate n specific GPUs to the given task.
This assumes the total number of GPUs has been
accurately tracked: this function will fatal()
if not enough are available.
*/

void ds_gpus_allocate( int n, int task )
{
	int i;
	for(i=0;i<total_resources->gpus.total && n>0;i++) {
		if(gpu_to_task[i]==0) {
			gpu_to_task[i] = task;
			n--;
		}
	}

	if(n>0) fatal("ds_gpus_allocate: accounting error: ran out of gpus to assign!");

	ds_gpus_debug();
}

/*
Return a string representing the GPUs allocated to taskid.
For example, if GPUs 1 and 3 are allocated, return "1,3"
This string must be freed after use.
*/

char *ds_gpus_to_string( int taskid )
{
	int i;
	int first = 1;
	buffer_t b;
	buffer_init(&b);
	for(i=0;i<total_resources->gpus.total;i++) {
		if(gpu_to_task[i]==taskid) {
			if(first) {
				first = 0;
			} else {
				buffer_putfstring(&b,",");
			}
			buffer_putfstring(&b,"%d",i);
		}
	}
	char *str = strdup(buffer_tostring(&b));
	buffer_free(&b);
	return str;
}

