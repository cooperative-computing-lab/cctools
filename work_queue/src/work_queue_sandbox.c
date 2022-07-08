#include "work_queue_sandbox.h"
#include "work_queue_internal.h"

#include "stringtools.h"
#include "debug.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "link_recursive.h"

#include <unistd.h>
#include <errno.h>
#include <stdlib.h>

extern int symlinks_enabled;


/*
Transfer a single input file from a url to a local filename by using /usr/bin/curl.
XXX add time and performance here.
*/

static int transfer_input_url( struct work_queue_process *p, const char *url, const char *filename )
{
	char * command = string_format("curl -f -o \"%s\" \"%s\"",filename,url);
	debug(D_WQ,"transfer %s to %s using %s",url,filename,command);
	int result = system(command);
	free(command);
	if(result==0) {
		debug(D_WQ,"transfer %s success",url);
		return 1;
	} else {
		debug(D_WQ,"transfer %s failed",url);
		return 0;
	}
}

/*
If a string begins with one or more instances of ./
return the beginning of the string with those removed.
*/

static const char *skip_dotslash( const char *s )
{
	while(!strncmp(s,"./",2)) s+=2;
	return s;
}

/*
Transfer a single file into the sandbox.
Each type of input binding has a different method.
Ordinary files are linked from the cache directory,
remote urls are loaded over the network, etc.

XXX Later improvement: transferred files should be moved
into the sandbox directory where they can be shared.
*/

static int transfer_input_file( struct work_queue_process *p, struct work_queue_file *f )
{
	char *sandbox_name = string_format("%s/%s",skip_dotslash(p->sandbox),skip_dotslash(f->remote_name));
	create_dir_parents(sandbox_name,0777);
	int result = 0;
	
	switch(f->type) {
		case WORK_QUEUE_FILE:
		case WORK_QUEUE_FILE_PIECE:
		case WORK_QUEUE_BUFFER:
			debug(D_WQ,"input: file %s -> %s",f->payload,sandbox_name);
		  	result = link_recursive(skip_dotslash(f->payload),skip_dotslash(sandbox_name),symlinks_enabled);
			if(!result) {
				if(errno==EEXIST) {
					// XXX silently ignore the case where the target file exists.
					// This happens when managers apps map the same input file twice, or to the same name.
					// Would be better to reject this at the manager instead.
					result = 1;
				} else {
					debug(D_WQ,"couldn't link %s into sandbox as %s: %s",f->payload,sandbox_name,strerror(errno));
				}
			}
			break;
		case WORK_QUEUE_REMOTECMD:
			// XXX execute remote command here
			break;
		case WORK_QUEUE_DIRECTORY:
			debug(D_WQ,"input: dir %s",sandbox_name);
			result = create_dir(sandbox_name, 0700);
			if(!result) debug(D_WQ,"couldn't create directory %s: %s", sandbox_name, strerror(errno));
			break;
		case WORK_QUEUE_URL:
			debug(D_WQ,"input: url %s -> %s",f->payload,sandbox_name);
			result = transfer_input_url(p,f->payload,f->remote_name);
			break;
	}

	free(sandbox_name);
	return result;
}

/*
For each input file specified by the process,
transfer it into the sandbox directory.
*/

static int transfer_input_files( struct work_queue_process *p )
{
	struct work_queue_task *t = p->task;
	struct work_queue_file *f;
	int result=1;
	
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			result = transfer_input_file(p,f);
			if(!result) break;
		}
	}

	return result;
}

int work_queue_sandbox_stagein( struct work_queue_process *p )
{
	if(!transfer_input_files(p)) return 0;
	
	return 1;
}

static int transfer_output_file( struct work_queue_process *p, struct work_queue_file *f )
{
	char *sandbox_name = string_format("%s/%s",p->sandbox,f->remote_name);
	int result = 0;
	
	switch(f->type) {
		case WORK_QUEUE_FILE:
			debug(D_WQ,"moving output file from %s to %s",sandbox_name,f->payload);
			/* First we try a cheap rename. It that does not work, we try to copy the file. */
			if(rename(sandbox_name,f->payload)<0) {
				debug(D_WQ, "could not rename output file %s to %s: %s",sandbox_name,f->payload,strerror(errno));
				if(copy_file_to_file(sandbox_name, f->payload)  == -1) {
					debug(D_WQ, "could not copy output file %s to %s: %s",sandbox_name,f->payload,strerror(errno));
					result = 0;
				} else {
					result = 1;
				}
			} else {
				result = 1;
			}
			break;
			
		case WORK_QUEUE_FILE_PIECE:
		case WORK_QUEUE_BUFFER:
		case WORK_QUEUE_REMOTECMD:
		case WORK_QUEUE_DIRECTORY:
		case WORK_QUEUE_URL:
			debug(D_WQ,"unsupported output file transfer method %d\n",f->type);
			result = 0;
			break;
	}
	
	free(sandbox_name);
	return result;
}

/*
Move all output files of a completed process back into the proper cache location.
This function deliberately does not fail.  If any of the desired outputs was not
created, we still want the task to be marked as completed and sent back to the
manager.  The manager will handle the consequences of missing output files.
*/

static int transfer_output_files( struct work_queue_process *p )
{
	struct work_queue_file *f;
	list_first_item(p->task->output_files);
	while((f = list_next_item(p->task->output_files))) {
		transfer_output_file(p,f);
	}

	return 1;
}


int work_queue_sandbox_stageout( struct work_queue_process *p )
{
	transfer_output_files(p);
	// delete of sandbox dir happens in work_queue_process_delete
	return 1;
}

