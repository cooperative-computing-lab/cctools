/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file.h"
#include "vine_task.h"

#include "debug.h"
#include "xxmalloc.h"
#include "md5.h"
#include "url_encode.h"
#include "stringtools.h"
#include "path.h"

#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#include <dirent.h>
#include <sys/stat.h>

/* Internal use: when the worker uses the client library, do not recompute cached names. */
int vine_hack_do_not_compute_cached_name = 0;

/*
For a given task and file, generate the name under which the file
should be stored in the remote cache directory.

The basic strategy is to construct a name that is unique to the
namespace from where the file is drawn, so that tasks sharing
the same input file can share the same copy.

In the common case of files, the cached name is based on the
hash of the local path, with the basename of the local path
included simply to assist with debugging.

In each of the other file types, a similar approach is taken,
including a hash and a name where one is known, or another
unique identifier where no name is available.
*/


/* creates a hash for a single file or directory */

const char * merkle(char * src){
	struct stat info;
	if(stat(src, &info) != 0) return 0; // couldn't stat file
	if(S_ISDIR(info.st_mode)){
		struct dirent *d;	
		DIR *dir = opendir(src);
		if(!dir) return 0;
		unsigned char digest[MD5_DIGEST_LENGTH];
		char * path;
		const char * hash = 0;
		int init = 0;
		while((d=readdir(dir))){
			if(!strcmp(d->d_name,".")) continue;
            		if(!strcmp(d->d_name,"..")) continue;
			path = path_concat(src, d->d_name);
			if(init){
				char * str = string_format("%s%s", hash, merkle(path));
				md5_buffer((str), strlen(str), digest);
				hash = md5_string(digest);
			}
			else{
				init = 1;
				hash = merkle(path);	
			}
		}
		return hash; // return hash of hashes
	}
	else if(S_ISREG(info.st_mode)){
		unsigned char digest[MD5_DIGEST_LENGTH];
		const char * hash;
		md5_file(src, digest);
		hash = md5_string(digest);
		return hash;
		
	}
	else{
		return 0;
	}
}

char *make_cached_name( const struct vine_file *f )
{
	static unsigned int file_count = 0;
	file_count++;

	unsigned char digest[MD5_DIGEST_LENGTH];
	char source_enc[PATH_MAX];
	const char * merkle_hash;

	if(f->type == VINE_BUFFER) {
		if(f->data) {
			md5_buffer(f->data, f->length, digest);
		} else {
			md5_buffer("buffer", 6, digest );
		}
	} else if(f->type == VINE_FILE) {
		/* XXX check for hashing options*/
		merkle_hash = merkle(f->source);
		//md5_buffer(f->source,strlen(f->source),digest);
		url_encode(path_basename(f->source), source_enc, PATH_MAX);
	} else if(f->type == VINE_URL){
		/* XXX curl header of file and cycle though options(eTag, md5, modified date?)*/
		md5_buffer(f->source,strlen(f->source),digest);
		url_encode(path_basename(f->source), source_enc, PATH_MAX);
	} else if(f->type == VINE_MINI_TASK){
		md5_buffer(f->source,strlen(f->source),digest);
		url_encode(path_basename(f->source), source_enc, PATH_MAX);
	} else {
		md5_buffer(f->source,strlen(f->source),digest);
		url_encode(path_basename(f->source), source_enc, PATH_MAX);
	}

	/* 0 for cache files, file_count for non-cache files. With this, non-cache
	 * files cannot be shared among tasks, and can be safely deleted once a
	 * task finishes. */
	unsigned int cache_file_id = 0;
	if(!(f->flags & VINE_CACHE)) {
		cache_file_id = file_count;
	}

	/* XXX hack to force caching for the moment */
	cache_file_id = 0;
	
	switch(f->type) {
		case VINE_FILE:
		case VINE_EMPTY_DIR:
			return string_format("file-%d-%s-%s", cache_file_id, merkle_hash, source_enc);
			break;
		case VINE_MINI_TASK:
			/* XXX This should be computed from the constituents of the mini task */
			return string_format("task-%d-%s", cache_file_id, md5_string(digest));
			break;
	       	case VINE_URL:
			return string_format("url-%d-%s", cache_file_id, md5_string(digest));
			break;
		case VINE_BUFFER:
		default:
			return string_format("buffer-%d-%s", cache_file_id, md5_string(digest));
			break;
	}
}

/* Create a new file object with the given properties. */

struct vine_file *vine_file_create(const char *source, const char *remote_name, const char *data, int length, vine_file_t type, vine_file_flags_t flags, struct vine_task *mini_task )
{
	struct vine_file *f;

	f = xxmalloc(sizeof(*f));

	memset(f, 0, sizeof(*f));

	f->source = xxstrdup(source);
	if(remote_name) {
		f->remote_name = xxstrdup(remote_name);
	} else {
		f->remote_name = 0;
	}
	f->type = type;
	f->flags = flags;
	f->length = length;
	f->mini_task = mini_task;
	
	if(data) {
		f->data = malloc(length);
		memcpy(f->data,data,length);
	} else {
		f->data = 0;
	}

	if(vine_hack_do_not_compute_cached_name) {
  		f->cached_name = xxstrdup(f->source);
	} else {
		f->cached_name = make_cached_name(f);
	}

	return f;
}

/* Make a deep copy of a file object to be used independently. */

struct vine_file *vine_file_clone(const struct vine_file *f )
{
	if(!f) return 0;
	return vine_file_create(f->source,f->remote_name,f->data,f->length,f->type,f->flags,vine_task_clone(f->mini_task));
}

/* Delete a file object */

void vine_file_delete(struct vine_file *f)
{
	if(!f) return;
	vine_task_delete(f->mini_task);
	free(f->source);
	free(f->remote_name);
	free(f->cached_name);
	free(f->data);
	free(f);
}

struct vine_file * vine_file_local( const char *source )
{
	return vine_file_create(source,0,0,0,VINE_FILE,0,0);
}

struct vine_file * vine_file_url( const char *source )
{
	return vine_file_create(source,0,0,0,VINE_URL,0,0);
}

struct vine_file * vine_file_buffer( const char *buffer_name,const char *data, int length )
{
	return vine_file_create(buffer_name,0,data,length,VINE_BUFFER,0,0);
}

struct vine_file * vine_file_empty_dir()
{
	return vine_file_create("unnamed",0,0,0,VINE_EMPTY_DIR,0,0);
}

struct vine_file * vine_file_mini_task( struct vine_task *t )
{
	return vine_file_create(t->command_line,0,0,0,VINE_MINI_TASK,0,t);
}

struct vine_file * vine_file_untar( struct vine_file *f )
{
	struct vine_task *t = vine_task_create("mkdir output && tar xf input.tar -C output");
	vine_task_add_input(t,f,"input.tar",VINE_CACHE);
	vine_task_add_output(t,vine_file_local("output"),"output",VINE_CACHE);
	return vine_file_mini_task(t);
}

struct vine_file * vine_file_untgz( struct vine_file *f )
{
	struct vine_task *t = vine_task_create("mkdir output && tar xzf input.tgz -C output");
	vine_task_add_input(t,f,"input.tgz",VINE_CACHE);
	vine_task_add_output(t,vine_file_local("output"),"output",VINE_CACHE);
	return vine_file_mini_task(t);
}

struct vine_file * vine_file_unponcho( struct vine_file *f)
{

	struct vine_task *t  = vine_task_create(string_format("./poncho_package_run --unpack-to output -e package.tar.gz"));
	char * poncho_path = path_which("poncho_package_run");
	vine_task_add_input(t, vine_file_local(poncho_path), "poncho_package_run", VINE_CACHE);
	vine_task_add_input(t, f, "package.tar.gz", VINE_CACHE);
	vine_task_add_output(t, vine_file_local("output"), "output", VINE_CACHE);
	return vine_file_mini_task(t);

}

