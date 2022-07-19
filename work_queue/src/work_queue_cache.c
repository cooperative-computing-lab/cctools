
#include "work_queue_cache.h"

#include "xxmalloc.h"
#include "hash_table.h"
#include "debug.h"
#include "stringtools.h"
#include "trash.h"

#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>

struct work_queue_cache {
	struct hash_table *table;
	char *cache_dir;
};

struct cache_file {
	work_queue_cache_type_t type;
	char *source;
	int64_t size;
	int present;
};

struct cache_file * cache_file_create( work_queue_cache_type_t type, const char *source, int64_t size, int present )
{
	struct cache_file *f = malloc(sizeof(*f));
	f->type = type;
	f->source = xxstrdup(source);
	f->size = size;
	f->present = present;
	return f;
}

void cache_file_delete( struct cache_file *f )
{
	free(f->source);
	free(f);
}

struct work_queue_cache * work_queue_cache_create( const char *cache_dir )
{
	struct work_queue_cache *c = malloc(sizeof(*c));
	c->cache_dir = strdup(cache_dir);
	c->table = hash_table_create(0,0);
	return c;
}

void work_queue_cache_delete( struct work_queue_cache *c )
{
	hash_table_clear(c->table,(void*)cache_file_delete);
	hash_table_delete(c->table);
	free(c->cache_dir);
	free(c);
}

int work_queue_cache_addfile( struct work_queue_cache *c, int64_t size, const char *cachename )
{
	struct cache_file *f = cache_file_create(WORK_QUEUE_CACHE_FILE,"manager",size,1);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

int work_queue_cache_queue( struct work_queue_cache *c, work_queue_cache_type_t type, const char *source, const char *cachename )
{
	struct cache_file *f = cache_file_create(type,source,0,0);
	hash_table_insert(c->table,cachename,f);
	return 1;
}

int work_queue_cache_remove( struct work_queue_cache *c, const char *cachename )
{
	struct cache_file *f = hash_table_remove(c->table,cachename);
	if(!f) return 0;
	
	char *cache_path = string_format("%s/%s",c->cache_dir,cachename);
	trash_file(cache_path);
	free(cache_path);

	cache_file_delete(f);
	
	return 1;
}

/*
Transfer a single input file from a url to a local filename by using /usr/bin/curl.
XXX add time and performance here.
*/

static int work_queue_cache_do_transfer( struct work_queue_cache *c, const char *source_url, const char *cache_path )
{
	char * command = string_format("curl -f -o \"%s\" \"%s\"",cache_path,source_url);
	debug(D_WQ,"executing: %s",command);
	int result = system(command);
	free(command);
	// convert result from unix convention to boolean
	return (result==0);
}

/*
Create a file by executing a shell command.
The command should contain %% which indicates the path
of the cache file to be created.
*/

static int work_queue_cache_do_command( struct work_queue_cache *c, const char *command, const char *cache_path )
{
	char *full_command = string_replace_percents(command,cache_path);
	debug(D_WQ,"executing: %s",full_command);
	int result = system(full_command);
	free(full_command);
	// convert result from unix convention to boolean
	return (result=0);
}

int work_queue_cache_ensure( struct work_queue_cache *c, const char *cachename )
{
	struct cache_file *f = hash_table_lookup(c->table,cachename);
	if(!f) return 0;

	if(f->present) {
		  debug(D_WQ,"cache: %s is already present.",cachename);
		  return 1;
	}
	
	char *cache_path = string_format("%s/%s",c->cache_dir,cachename);

	int result = 0;

	switch(f->type) {
		case WORK_QUEUE_CACHE_FILE:
			debug(D_WQ,"error: file %s should already be present!",cachename);
			result = 0;
			break;
		  
		case WORK_QUEUE_CACHE_TRANSFER:
			debug(D_WQ,"cache: transferring %s to %s",f->source,cachename);
			result = work_queue_cache_do_transfer(c,f->source,cache_path);
			break;

		case WORK_QUEUE_CACHE_COMMAND:
			debug(D_WQ,"cache: creating %s via shell command",cachename);
			result = work_queue_cache_do_command(c,f->source,cache_path);
			break;
	}

	if(result) {
		struct stat info;
		if(stat(cache_path,&info)==0) {
			f->size = info.st_size;
			f->present = 1;
			debug(D_WQ,"cache: created %s with size %lld",cachename,(long long)f->size);
			// XXX send back to manager size of created file
		} else {
			debug(D_WQ,"cache: command succeeded but did not create %s",cachename);
		}
	} else {
		debug(D_WQ,"cache: unable to create %s",cachename);
		// In case the command created a partial file, trash it.
		trash_file(cache_path);
	}
	
	free(cache_path);
	return result;
}



