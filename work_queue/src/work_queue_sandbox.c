#include "work_queue_sandbox.h"
#include "work_queue_cache.h"
#include "work_queue_internal.h"

#include "stringtools.h"
#include "debug.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "file_link_recursive.h"

#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/stat.h>

extern int symlinks_enabled;

char * work_queue_sandbox_full_path( struct work_queue_process *p, const char *sandbox_name )
{
	return string_format("%s/%s",p->sandbox,sandbox_name);
}

/*
Ensure that a given input file/dir/object is present in the cache,
(which may result in a transfer)
and then link it into the sandbox at the desired location.
*/

static int ensure_input_file( struct work_queue_process *p, struct work_queue_file *f, struct work_queue_cache *cache, struct link *manager )
{
	char *cache_path = work_queue_cache_full_path(cache,f->cached_name);
	char *sandbox_path = work_queue_sandbox_full_path(p,f->remote_name);
	
	int result = 0;

	if(f->type==WORK_QUEUE_DIRECTORY) {
		/* Special case: empty directories are not cached objects, just create in sandbox */
		result = create_dir(sandbox_path, 0700);
		if(!result) debug(D_WQ,"couldn't create directory %s: %s", sandbox_path, strerror(errno));

	} else if(work_queue_cache_ensure(cache,f->cached_name,manager)) {
		/* All other types, link the cached object into the sandbox */
	    	create_dir_parents(sandbox_path,0777);
		debug(D_WQ,"input: link %s -> %s",cache_path,sandbox_path);
		result = file_link_recursive(cache_path,sandbox_path,symlinks_enabled);
		if(!result) debug(D_WQ,"couldn't link %s into sandbox as %s: %s",cache_path,sandbox_path,strerror(errno));
	}
	
	free(cache_path);
	free(sandbox_path);
	
	return result;
}

/*
For each input file specified by the process,
transfer it into the sandbox directory.
*/

int work_queue_sandbox_stagein( struct work_queue_process *p, struct work_queue_cache *cache, struct link *manager )
{
	struct work_queue_task *t = p->task;
	struct work_queue_file *f;
	int result=1;
	
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			result = ensure_input_file(p,f,cache,manager);
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
Inform the cache of the added file.
*/

static int transfer_output_file( struct work_queue_process *p, struct work_queue_file *f, struct work_queue_cache *cache )
{
	char *cache_path = work_queue_cache_full_path(cache,f->cached_name);
	char *sandbox_path = work_queue_sandbox_full_path(p,f->remote_name);

	int result = 0;
	
	debug(D_WQ,"output: moving %s to %s",sandbox_path,cache_path);
	if(rename(sandbox_path,cache_path)<0) {
		debug(D_WQ, "output: move failed, attempting copy of %s to %s: %s",sandbox_path,cache_path,strerror(errno));
		if(copy_file_to_file(sandbox_path, cache_path)  == -1) {
			debug(D_WQ, "could not move or copy output file %s to %s: %s",sandbox_path,cache_path,strerror(errno));
			result = 0;
		} else {
			result = 1;
		}
	} else {
		result = 1;
	}

	if(result) {
		struct stat info;
		if(stat(cache_path,&info)==0) {
			work_queue_cache_addfile(cache,info.st_size,f->cached_name);
		} else {
			// This seems implausible given that the rename/copy succeded, but we still have to check...
			debug(D_WQ,"output: failed to stat %s: %s",cache_path,strerror(errno));
			result = 0;
		}
	}
	
	free(sandbox_path);
	free(cache_path);
	
	return result;
}

/*
Move all output files of a completed process back into the proper cache location.
This function deliberately does not fail.  If any of the desired outputs was not
created, we still want the task to be marked as completed and sent back to the
manager.  The manager will handle the consequences of missing output files.
*/

int work_queue_sandbox_stageout( struct work_queue_process *p, struct work_queue_cache *cache )
{
	struct work_queue_file *f;
	list_first_item(p->task->output_files);
	while((f = list_next_item(p->task->output_files))) {
		transfer_output_file(p,f,cache);
	}

	return 1;
}
