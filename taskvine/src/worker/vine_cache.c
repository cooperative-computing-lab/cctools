/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_cache.h"
#include "vine_process.h"

#include "vine_transfer.h"
#include "vine_protocol.h"
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
#include <dirent.h>
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
Load existing cache directory into cache structure.
*/
void vine_cache_load(struct vine_cache *c)
{
	DIR *dir = opendir(c->cache_dir);
	if(dir){
		debug(D_VINE, "loading cache at: %s", c->cache_dir);
		struct dirent *d;
		while((d=readdir(dir))){
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;
			debug(D_VINE, "found %s in cache at: %s",d->d_name, c->cache_dir);
			struct stat info;
			int64_t nbytes, nfiles;
			char *cache_path = vine_cache_full_path(c,d->d_name);
			if(stat(cache_path, &info)==0){
				if(S_ISREG(info.st_mode)){
					vine_cache_addfile(c, info.st_size, info.st_mode, d->d_name);
					debug(D_VINE,"loaded: %s into cache at: %s", d->d_name, c->cache_dir);
				}
				else if(S_ISDIR(info.st_mode)){
                			path_disk_size_info_get(cache_path,&nbytes,&nfiles); 
					vine_cache_addfile(c, nbytes, info.st_mode, d->d_name);
					debug(D_VINE,"loaded: %s into cache at: %s", d->d_name, c->cache_dir);
				}
			}	
			else{
				debug(D_VINE,"could not stat: %s in cache: %s error %s", d->d_name, c->cache_dir, strerror(errno));
			}
			free(cache_path);	
		}
	}
	closedir(dir);
}

int send_cache_update( struct link *manager, const char *cachename, int64_t size, timestamp_t transfer_time );
/*
send cache updates to manager from existing cache_directory 
*/
void vine_init_update(struct vine_cache *c, struct link *manager)
{
	struct cache_file *f;
	char * cachename;
	HASH_TABLE_ITERATE(c->table, cachename, f){
		debug(D_VINE,"sending cache update to manager cachename: %s source %s", cachename, f->source);
		timestamp_t transfer_time = timestamp_get();
		send_cache_update(manager,cachename,f->actual_size,transfer_time);
	}
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
*/

int vine_cache_addfile( struct vine_cache *c, int64_t size, int mode, const char *cachename )
{
	struct cache_file *f = cache_file_create(VINE_CACHE_FILE,"manager",size,mode,0);
	hash_table_insert(c->table,cachename,f);
	f->complete = 1;
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

static int do_curl_transfer( struct vine_cache *c, const char *source_url, const char *cache_path, char **error_message )
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

static int do_mini_task( struct vine_cache *c, struct vine_task *mini_task, struct link *manager, char **error_message )
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
	char addr[VINE_LINE_MAX], path[VINE_LINE_MAX];
	int stoptime;	
	struct link *worker_link;
	
	// expect the form: worker://addr:port/path/to/file
	sscanf(source_url, "worker://%99[^:]:%d/%s", addr, &port_num, path);
	debug(D_VINE, "Setting up worker transfer file %s",source_url);

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
		link_close(worker_link);
		return 0;
	}
		
	
	link_close(worker_link);

	return 1;
}

/*
Transfer a single obejct into the cache,
whether by worker or via curl.
Use a temporary transfer path while downloading,
and then rename it into the proper place.
*/

static int do_transfer( struct vine_cache *c, const char *source_url, const char *cache_path, char **error_message)
{
	char *transfer_path = string_format("%s.transfer",cache_path);
	int result = 0;
	
	if(strncmp(source_url, "worker://", 9) == 0){
		result = do_worker_transfer(c,source_url,transfer_path,error_message);
		if(result){
			debug(D_VINE, "received file from worker");
			rename(cache_path, transfer_path);
		}
	} else { 
		result = do_curl_transfer(c,source_url,transfer_path,error_message);
	}

	if(result) {
		if(rename(transfer_path,cache_path)==0) {
			debug(D_VINE,"cache: renamed %s to %s",transfer_path,cache_path);
		} else {
			debug(D_VINE,"cache: failed to rename %s to %s: %s",transfer_path,cache_path,strerror(errno));
			result = 0;
		}
	}

	if(!result) trash_file(transfer_path);
	
	free(transfer_path);

	return result;
}


/*
Ensure that a given cached entry is fully materialized in the cache,
downloading files or executing commands as needed.  If complete, return
true, otherwise return false.

It is a little odd that the manager link is passed as an argument here,
but it is needed in order to send back the necessary update/invalid messages.
*/

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

	int result = 0;

	timestamp_t transfer_start = timestamp_get();
	
	switch(f->type) {
		case VINE_CACHE_FILE:
			debug(D_VINE,"cache: manager already delivered %s",cachename);
			result = 1;
			break;
		  
		case VINE_CACHE_TRANSFER:
			debug(D_VINE,"cache: transferring %s to %s",f->source,cachename);
			result = do_transfer(c,f->source,cache_path,&error_message);
			break;

		case VINE_CACHE_MINI_TASK:
			debug(D_VINE,"cache: creating %s via mini task",cachename);
			result = do_mini_task(c,f->mini_task,manager,&error_message);
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
		if(!error_message) error_message = strdup("unknown");
		send_cache_invalid(manager,cachename,error_message);
		vine_cache_remove(c,cachename);
	}
	
	if(error_message) free(error_message);
	free(cache_path);
	return result;
}

