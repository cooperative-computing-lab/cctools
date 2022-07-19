#include "work_queue_sandbox.h"
#include "work_queue_cache.h"
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

extern struct work_queue_cache *thecache;

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
Ensure that a given input file/dir/object is present in the cache,
(which may result in a transfer)
and then link it into the sandbox at the desired location.
*/

static int ensure_input_file( struct work_queue_process *p, struct work_queue_file *f )
{
	char *cache_name = string_format("%s/%s",p->cache_dir,skip_dotslash(f->cached_name));
	char *sandbox_name = string_format("%s/%s",p->sandbox,skip_dotslash(f->remote_name));

	int result = 0;

	if(f->type==WORK_QUEUE_DIRECTORY) {
		/* Special case: empty directories are not cached objects, just create in sandbox */
		result = create_dir(sandbox_name, 0700);
		if(!result) debug(D_WQ,"couldn't create directory %s: %s", sandbox_name, strerror(errno));
	} else if(work_queue_cache_ensure(thecache,f->cached_name)) {
		/* All other types, link the cached object into the sandbox */
	    	create_dir_parents(sandbox_name,0777);
		debug(D_WQ,"input: link %s -> %s",cache_name,sandbox_name);
		result = link_recursive(cache_name,sandbox_name,symlinks_enabled);
		if(!result) debug(D_WQ,"couldn't link %s into sandbox as %s: %s",cache_name,sandbox_name,strerror(errno));
	}
	
	free(cache_name);
	free(sandbox_name);
	
	return result;
}

/*
For each input file specified by the process,
transfer it into the sandbox directory.
*/

int work_queue_sandbox_stagein( struct work_queue_process *p )
{
	struct work_queue_task *t = p->task;
	struct work_queue_file *f;
	int result=1;
	
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			result = ensure_input_file(p,f);
			if(!result) break;
		}
	}

	return result;
}

/*
Move a given output file back to the target cache location.
First attempt a cheap rename.
If that does not work (perhaps due to crossing filesystems)
then attempt a recursive copy.
*/

static int transfer_output_file( struct work_queue_process *p, struct work_queue_file *f )
{
	char *cache_name = string_format("%s/%s",p->cache_dir,skip_dotslash(f->cached_name));
	char *sandbox_name = string_format("%s/%s",p->sandbox,skip_dotslash(f->remote_name));

	int result = 0;
	
	debug(D_WQ,"output: moving %s to %s",sandbox_name,cache_name);
	if(rename(sandbox_name,cache_name)<0) {
		debug(D_WQ, "output: move failed, attempting copy of %s to %s: %s",sandbox_name,cache_name,strerror(errno));
		if(copy_file_to_file(sandbox_name, cache_name)  == -1) {
			debug(D_WQ, "could not move or copy output file %s to %s: %s",sandbox_name,cache_name,strerror(errno));
			result = 0;
		} else {
			result = 1;
		}
	} else {
		result = 1;
	}
	
	free(sandbox_name);
	free(cache_name);
	
	return result;
}

/*
Move all output files of a completed process back into the proper cache location.
This function deliberately does not fail.  If any of the desired outputs was not
created, we still want the task to be marked as completed and sent back to the
manager.  The manager will handle the consequences of missing output files.
*/

int work_queue_sandbox_stageout( struct work_queue_process *p )
{
	struct work_queue_file *f;
	list_first_item(p->task->output_files);
	while((f = list_next_item(p->task->output_files))) {
		transfer_output_file(p,f);
	}

	return 1;
}
