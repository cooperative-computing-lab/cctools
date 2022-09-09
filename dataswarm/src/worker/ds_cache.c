/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ds_cache.h"

#include "xxmalloc.h"
#include "hash_table.h"
#include "debug.h"
#include "stringtools.h"
#include "trash.h"
#include "link.h"
#include "timestamp.h"
#include "copy_stream.h"

#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

struct ds_cache {
	struct hash_table *table;
	char *cache_dir;
};

struct cache_file {
	ds_cache_type_t type;
	char *source;
	int64_t expected_size;
	int64_t actual_size;
	int mode;
	int present;
};

struct cache_file * cache_file_create( ds_cache_type_t type, const char *source, int64_t expected_size, int64_t actual_size, int mode, int present )
{
	struct cache_file *f = malloc(sizeof(*f));
	f->type = type;
	f->source = xxstrdup(source);
	f->expected_size = expected_size;
	f->actual_size = actual_size;
	f->mode = mode;
	f->present = present;
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

struct ds_cache * ds_cache_create( const char *cache_dir )
{
	struct ds_cache *c = malloc(sizeof(*c));
	c->cache_dir = strdup(cache_dir);
	c->table = hash_table_create(0,0);
	return c;
}

/*
Delete the cache manager structure, though not the underlying files.
*/

void ds_cache_delete( struct ds_cache *c )
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

char * ds_cache_full_path( struct ds_cache *c, const char *cachename )
{
	return string_format("%s/%s",c->cache_dir,cachename);
}
	

/*
Add a file to the cache manager (already created in the proper place) and note its size.
*/

int ds_cache_addfile( struct ds_cache *c, int64_t size, const char *cachename )
{
	struct cache_file *f = cache_file_create(DS_CACHE_FILE,"manager",size,size,0777,1);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

/*
Queue a remote file transfer or command execution to produce a file.
This entry will be materialized later in ds_cache_ensure.
*/

int ds_cache_queue( struct ds_cache *c, ds_cache_type_t type, const char *source, const char *cachename, int64_t size, int mode )
{
	struct cache_file *f = cache_file_create(type,source,size,0,mode,0);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

/*
Remove a named item from the cache, regardless of its type.
*/

int ds_cache_remove( struct ds_cache *c, const char *cachename )
{
	struct cache_file *f = hash_table_remove(c->table,cachename);
	if(!f) return 0;
	
	char *cache_path = ds_cache_full_path(c,cachename);
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


static int do_internal_command( struct ds_cache *c, const char *command, char **error_message )
{
	int result = 0;
	*error_message = 0;
	
	debug(D_DS,"executing: %s",command);
		
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
			debug(D_DS,"command failed with output: %s",*error_message);
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

static int do_transfer( struct ds_cache *c, const char *source_url, const char *cache_path, char **error_message )
{
	char * command = string_format("curl -sSL --stderr /dev/stdout -o \"%s\" \"%s\"",cache_path,source_url);
	int result = do_internal_command(c,command,error_message);
	free(command);
	return result;
}

/*
Create a file by executing a shell command.
The command should contain %% which indicates the path of the cache file to be created.
*/

static int do_command( struct ds_cache *c, const char *command, const char *cache_path, char **error_message )
{
	char *full_command = string_replace_percents(command,cache_path);
	int result = do_internal_command(c,full_command,error_message);
	free(full_command);
	return result;
}

/*
Ensure that a given cached entry is fully materialized in the cache,
downloading files or executing commands as needed.  If present, return
true, otherwise return false.

It is a little odd that the manager link is passed as an argument here,
but it is needed in order to send back the necessary update/invalid messages.
*/

int send_cache_update( struct link *manager, const char *cachename, int64_t size, timestamp_t transfer_time );
int send_cache_invalid( struct link *manager, const char *cachename, const char *message );

int ds_cache_ensure( struct ds_cache *c, const char *cachename, struct link *manager )
{
	struct cache_file *f = hash_table_lookup(c->table,cachename);
	if(!f) {
		debug(D_DS,"cache: %s is unknown, perhaps it failed to transfer earlier?",cachename);
		return 0;
	}

	if(f->present) {
		debug(D_DS,"cache: %s is already present.",cachename);
		return 1;
	}
	
	char *cache_path = ds_cache_full_path(c,cachename);
	char *error_message = 0;

	int result = 0;

	timestamp_t transfer_start = timestamp_get();
	
	switch(f->type) {
		case DS_CACHE_FILE:
			debug(D_DS,"error: file %s should already be present!",cachename);
			result = 0;
			break;
		  
		case DS_CACHE_TRANSFER:
			debug(D_DS,"cache: transferring %s to %s",f->source,cachename);
			result = do_transfer(c,f->source,cache_path,&error_message);
			break;

		case DS_CACHE_COMMAND:
			debug(D_DS,"cache: creating %s via shell command",cachename);
			result = do_command(c,f->source,cache_path,&error_message);
			break;
	}

	// Set the permissions as originally indicated.	
	chmod(cache_path,f->mode);

	timestamp_t transfer_end = timestamp_get();
	timestamp_t transfer_time = transfer_end - transfer_start;
	
	/*
	Although the prior command may have succeeded, check the actual desired
	file in the cache to make sure that it is present.
	*/
	
	if(result) {
		struct stat info;
		if(stat(cache_path,&info)==0) {
			f->actual_size = info.st_size;
			f->expected_size = f->actual_size;
			f->present = 1;
			debug(D_DS,"cache: created %s with size %lld in %lld usec",cachename,(long long)f->actual_size,(long long)transfer_time);
			send_cache_update(manager,cachename,f->actual_size,transfer_time);
			result = 1;
		} else {
			debug(D_DS,"cache: command succeeded but did not create %s",cachename);
			result = 0;
		}
	} else {
		debug(D_DS,"cache: unable to create %s",cachename);
		result = 0;
	}

	/*
	If we failed to create the cached file for any reason,
	then destroy any partial remaining file, and inform
	the manager that the cached object is invalid.
	This task will fail in the sandbox setup stage.
	*/
	
	if(!result) {
		trash_file(cache_path);
		send_cache_invalid(manager,cachename,error_message);
	}
	
	if(error_message) free(error_message);
	free(cache_path);
	return result;
}



