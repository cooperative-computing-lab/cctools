/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_cache.h"
#include "vine_process.h"

#include "vine_transfer.h"
#include "xxmalloc.h"
#include "hash_table.h"
#include "debug.h"
#include "stringtools.h"
#include "trash.h"
#include "link.h"
#include "timestamp.h"
#include "copy_stream.h"
#include "path_disk_size_info.h"

#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

struct vine_cache {
	struct hash_table *table;
	char *cache_dir;
};

struct cache_file {
	vine_cache_type_t type;
	char *source;
	int64_t actual_size;
	int mode;
	int complete;
	struct vine_task *mini_task;
};

struct cache_file * cache_file_create( vine_cache_type_t type, const char *source, int64_t actual_size, int mode, struct vine_task *mini_task )
{
	struct cache_file *f = malloc(sizeof(*f));
	f->type = type;
	f->source = xxstrdup(source);
	f->actual_size = actual_size;
	f->mode = mode;
	f->complete = 0;
	f->mini_task = mini_task;
	return f;
}

void cache_file_delete( struct cache_file *f )
{
	free(f->source);
	free(f);
}

/*
Create the cache manager structure for a given cache directory.
*/

struct vine_cache * vine_cache_create( const char *cache_dir )
{
	struct vine_cache *c = malloc(sizeof(*c));
	c->cache_dir = strdup(cache_dir);
	c->table = hash_table_create(0,0);
	return c;
}

/*
Delete the cache manager structure, though not the underlying files.
*/

void vine_cache_delete( struct vine_cache *c )
{
	hash_table_clear(c->table,(void*)cache_file_delete);
	hash_table_delete(c->table);
	free(c->cache_dir);
	free(c);
}

/*
Get the full path to a file name within the cache.
This result must be freed.
*/

char * vine_cache_full_path( struct vine_cache *c, const char *cachename )
{
	return string_format("%s/%s",c->cache_dir,cachename);
}
	

/*
Add a file to the cache manager (already created in the proper place) and note its size.
It may still be necessary to perform post-transfer processing of this file.
*/

int vine_cache_addfile( struct vine_cache *c, int64_t size, int mode, const char *cachename )
{
	struct cache_file *f = cache_file_create(VINE_CACHE_FILE,"manager",size,mode,0);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

/*
Return true if the cache contains the requested item.
*/

int vine_cache_contains( struct vine_cache *c, const char *cachename )
{
	return hash_table_lookup(c->table,cachename)!=0;
}

/*
Queue a remote file transfer to produce a file.
This entry will be materialized later in vine_cache_ensure.
*/

int vine_cache_queue_transfer( struct vine_cache *c, const char *source, const char *cachename, int64_t size, int mode, vine_file_flags_t flags )
{
	struct cache_file *f = cache_file_create(VINE_CACHE_TRANSFER,source,size,mode,0);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

/*
Queue a mini-task to produce a file.
This entry will be materialized later in vine_cache_ensure.
*/

int vine_cache_queue_command( struct vine_cache *c, struct vine_task *mini_task, const char *cachename, int64_t size, int mode, vine_file_flags_t flags )
{
	struct cache_file *f = cache_file_create(VINE_CACHE_MINI_TASK,"task",size,mode,mini_task);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

/*
Remove a named item from the cache, regardless of its type.
*/

int vine_cache_remove( struct vine_cache *c, const char *cachename )
{
	struct cache_file *f = hash_table_remove(c->table,cachename);
	if(!f) return 0;
	
	char *cache_path = vine_cache_full_path(c,cachename);
	trash_file(cache_path);
	free(cache_path);

	cache_file_delete(f);
	
	return 1;

}

/*
Execute a shell command via popen and capture its output.
On success, return true.
On failure, return false with the string error_message filled in.
*/


static int do_internal_command( struct vine_cache *c, const char *command, char **error_message )
{
	int result = 0;
	*error_message = 0;
	
	debug(D_VINE,"executing: %s",command);
		
	FILE *stream = popen(command,"r");
	if(stream) {
		copy_stream_to_buffer(stream,error_message,0);
	  	int exit_status = pclose(stream);
		if(exit_status==0) {
			if(*error_message) {
				free(*error_message);
				*error_message = 0;
			}
			result = 1;
		} else {
			debug(D_VINE,"command failed with output: %s",*error_message);
			result = 0;
		}
	} else {
		*error_message = string_format("couldn't execute \"%s\": %s",command,strerror(errno));
		result = 0;
	}

	return result;
}

/*
Transfer a single input file from a url to a local filename by using /usr/bin/curl.
-s Do not show progress bar.  (Also disables errors.)
-S Show errors.
-L Follow redirects as needed.
--stderr Send errors to /dev/stdout so that they are observed by popen.
*/

static int do_transfer( struct vine_cache *c, const char *source_url, const char *cache_path, char **error_message )
{
	char * command = string_format("curl -sSL --stderr /dev/stdout -o \"%s\" \"%s\"",cache_path,source_url);
	int result = do_internal_command(c,command,error_message);
	free(command);
	return result;
}

/*
Create a file by executing a mini_task, which should produce the desired cachename.
The mini_task uses all the normal machinery to run a task synchronously,
which should result in the desired file being placed into the cache.
This will be double-checked below.
*/

static int do_command( struct vine_cache *c, struct vine_task *mini_task, struct link *manager, char **error_message )
{
	if(vine_process_execute_and_wait(mini_task,c,manager)) {
		*error_message = 0;
		return 1;
	} else {
		const char *str = vine_task_get_stdout(mini_task);
		if(str) {
			*error_message = xxstrdup(str);
		} else {
			*error_message = 0;
		}
		return 0;
	}
}

/*
Transfer a single input file from a worker url to a local file name. 

*/
static int do_worker_transfer( struct vine_cache *c, const char *source_url, const char *cache_path, char **error_message)
{	
	int port_num;
	char addr[99], path[4096];
	int stoptime;	
	struct link *worker_link;

	// expect the form: worker://addr:port/path/to/file
	sscanf(source_url, "worker://%99[^:]:%d/%s", addr, &port_num, path);

	stoptime = time(0) + 15;
	worker_link = link_connect(addr, port_num, stoptime);

	if(worker_link == NULL)
	{
		*error_message = string_format("Could not establish connection with worker at: %s:%d", addr, port_num);
		return 0;
	}

	if(!vine_transfer_get_any(worker_link, c, path, time(0) + 120))
	{
		*error_message = string_format("Could not transfer file %s from worker %s:%d", path, addr, port_num);
		return 0;
	}

	// rename file to our expected cache name (probably not the best way to do this)
	char *received_filename = string_format("%s/%s", c->cache_dir, path);
	rename(received_filename, cache_path);
	free(received_filename);
	
	return 1;
}

/*
For a given file that has been transferred into transfer_name,
either unpack it into cache_name, or just rename it into place,
depending on the flags of the file.
Returns true on success, false otherwise.
*/

int unpack_or_rename_target( struct cache_file *f, const char *transfer_path, const char *cache_path, vine_file_flags_t flags )
{
	int unix_result;
	char *command;

	if(flags & VINE_UNPACK) {
		if(string_suffix_is(f->source,".tar")) {
			mkdir(cache_path,0700);
			command = string_format("tar xf %s -C %s",transfer_path,cache_path);
		} else if(string_suffix_is(f->source,".tar.gz") || string_suffix_is(f->source,".tgz")) {
			mkdir(cache_path,0700);
			command = string_format("tar xzf %s -C %s",transfer_path,cache_path);
		} else if(string_suffix_is(f->source,".gz")) {
			command = string_format("gunzip <%s >%s",transfer_path,cache_path);
		} else if(string_suffix_is(f->source,".zip")) {
			mkdir(cache_path,0700);
			command = string_format("unzip %s -d %s",transfer_path,cache_path);
		} else {
			command = strdup("false");
		}
		debug(D_VINE,"unpacking %s to %s via command %s",transfer_path,cache_path,command);
		unix_result = system(command);
		free(command);
	} else if(flags & VINE_PONCHO_UNPACK){
		command = string_format("poncho_package_run -u %s -e %s", cache_path, transfer_path);
		debug(D_VINE,"unpacking %s to %s via command %s", transfer_path, cache_path, command);
		unix_result = system(command);
		free(command);
	} else {
		debug(D_VINE,"renaming %s to %s",transfer_path,cache_path);
		unix_result = rename(transfer_path,cache_path);
	}

	if(unix_result==0) {
		return 1;
	} else {
		debug(D_VINE,"command failed: %s",strerror(errno));
		return 0;
	}
}

/*
Ensure that a given cached entry is fully materialized in the cache,
downloading files or executing commands as needed.  If complete, return
true, otherwise return false.

It is a little odd that the manager link is passed as an argument here,
but it is needed in order to send back the necessary update/invalid messages.

XXX There is a subtle problem here.  File flags like UNPACK are associated
with the task definition, rather than the file definition.  If two or more
tasks specify the same input file but with different flags, unexpected things
will happen.  We need to better separate flags that affect files vs flags that
affect the binding to files.
*/

int send_cache_update( struct link *manager, const char *cachename, int64_t size, timestamp_t transfer_time );
int send_cache_invalid( struct link *manager, const char *cachename, const char *message );

int vine_cache_ensure( struct vine_cache *c, const char *cachename, struct link *manager, vine_file_flags_t flags )
{
	if(!strcmp(cachename,"0")) return 1;

	struct cache_file *f = hash_table_lookup(c->table,cachename);
	if(!f) {
		debug(D_VINE,"cache: %s is unknown, perhaps it failed to transfer earlier?",cachename);
		return 0;
	}

	if(f->complete) {
		debug(D_VINE,"cache: %s is already present.",cachename);
		return 1;
	}

	char *error_message = 0;
	char *cache_path = vine_cache_full_path(c,cachename);
	char *transfer_path = string_format("%s.transfer",cache_path);

	int result = 0;

	timestamp_t transfer_start = timestamp_get();
	
	switch(f->type) {
		case VINE_CACHE_FILE:
			debug(D_VINE,"cache: manager already delivered %s",cachename);
			/*
			This odd little rename here is to make manager-delivered files
			look like transfer/command files, which arrive into .transfer files,
			and then have the opportunity to be unpacked below.
			*/
			result = (rename(cache_path,transfer_path)==0);
			if(result) {
				result = unpack_or_rename_target(f,transfer_path,cache_path,flags);
			}
			break;
		  
		case VINE_CACHE_TRANSFER:
			debug(D_VINE,"cache: transferring %s to %s",f->source,cachename);
			if(strncmp(f->source, "worker://", 9) == 0){
				result = do_worker_transfer(c, f->source, transfer_path, &error_message);
			}else{ 
				result = do_transfer(c,f->source,transfer_path,&error_message);
			}
			if(result) {
				result = unpack_or_rename_target(f,transfer_path,cache_path,flags);
			}
			break;

		case VINE_CACHE_MINI_TASK:
			debug(D_VINE,"cache: creating %s via mini task",cachename);
			result = do_command(c,f->mini_task,manager,&error_message);
			break;
	}

	chmod(cache_path,f->mode);

	// Set the permissions as originally indicated.	

	timestamp_t transfer_end = timestamp_get();
	timestamp_t transfer_time = transfer_end - transfer_start;
	
	/*
	Although the prior command may have succeeded, check the actual desired
	file in the cache to make sure that it is complete.
	*/
	
	if(result) {
		int64_t nbytes, nfiles;
		if(path_disk_size_info_get(cache_path,&nbytes,&nfiles)==0) {
			f->actual_size = nbytes;
			f->complete = 1;
			debug(D_VINE,"cache: created %s with size %lld in %lld usec",cachename,(long long)f->actual_size,(long long)transfer_time);
			send_cache_update(manager,cachename,f->actual_size,transfer_time);
			result = 1;
		} else {
			debug(D_VINE,"cache: command succeeded but did not create %s",cachename);
			result = 0;
		}
	} else {
		debug(D_VINE,"cache: unable to create %s",cachename);
		result = 0;
	}

	/*
	If we failed to create the cached file for any reason,
	then destroy any partial remaining file, and inform
	the manager that the cached object is invalid.
	This task will fail in the sandbox setup stage.
	*/
	
	if(!result) {
		trash_file(transfer_path);
		if(!error_message) error_message = strdup("unknown");
		send_cache_invalid(manager,cachename,error_message);
		vine_cache_remove(c,cachename);
	}
	
	if(error_message) free(error_message);
	free(cache_path);
	free(transfer_path);
	return result;
}

