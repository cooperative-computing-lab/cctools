/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_file.h"
#include "vine_task.h"
#include "vine_cached_name.h"

#include "debug.h"
#include "xxmalloc.h"
#include "stringtools.h"
#include "path.h"

#include <stdlib.h>
#include <unistd.h>
#include <limits.h>

/* Internal use: when the worker uses the client library, do not recompute cached names. */
int vine_hack_do_not_compute_cached_name = 0;

/* Create a new file object with the given properties. */

struct vine_file *vine_file_create(const char *source, const char *cached_name, const char *data, int length, vine_file_t type, struct vine_task *mini_task )
{
	struct vine_file *f;

	f = xxmalloc(sizeof(*f));

	memset(f, 0, sizeof(*f));

	f->source = xxstrdup(source);
	f->type = type;
	f->length = length;
	f->mini_task = mini_task;

	if(data) {
		f->data = malloc(length);
		memcpy(f->data,data,length);
	} else {
		f->data = 0;
	}

  	if(vine_hack_do_not_compute_cached_name) {
		/* On the worker, the source (name on disk) is already the cached name. */
		f->cached_name = xxstrdup(f->source);
	} else if(cached_name) {
		/* If the cached name is provided, just use it.  (Likely a cloned object.) */
		f->cached_name = xxstrdup(cached_name);
	} else {
		/* Otherwise we need to figure it out ourselves from the content. */
		/* This may give us the actual size of the object along the way. */
		ssize_t totalsize = 0;
		f->cached_name = vine_cached_name(f,&totalsize);
		if(length==0) f->length = totalsize;
	}

	f->refcount = 1;

	return f;
}

/* Make a reference counted copy of a file object. */

struct vine_file *vine_file_clone( struct vine_file *f )
{
	if(!f) return 0;
	f->refcount++;
	return f;
}

/*
Request to delete a file object.
Decrement the reference count and delete if zero.
*/

void vine_file_delete(struct vine_file *f)
{
	if(!f) return;

	f->refcount--;
	if(f->refcount>0) return;

	vine_task_delete(f->mini_task);
	free(f->source);
	free(f->cached_name);
	free(f->data);
	free(f);
}

struct vine_file * vine_file_local( const char *source )
{
	return vine_file_create(source,0,0,0,VINE_FILE,0);
}

struct vine_file * vine_file_url( const char *source )
{
	return vine_file_create(source,0,0,0,VINE_URL,0);
}

struct vine_file * vine_file_substitute_url( struct vine_file *f, const char *source )
{
	return vine_file_create(source,f->cached_name,0,f->length,VINE_URL,0);
}

struct vine_file * vine_file_temp()
{
	return vine_file_create("temp",0,0,0,VINE_TEMP,0);
}

struct vine_file * vine_file_buffer( const char *buffer_name,const char *data, int length )
{
	return vine_file_create(buffer_name,0,data,length,VINE_BUFFER,0);
}

struct vine_file * vine_file_empty_dir()
{
	return vine_file_create("unnamed",0,0,0,VINE_EMPTY_DIR,0);
}

struct vine_file * vine_file_mini_task( struct vine_task *t )
{
	return vine_file_create(t->command_line,0,0,0,VINE_MINI_TASK,t);
}

struct vine_file * vine_file_untar( struct vine_file *f )
{
	struct vine_task *t = vine_task_create("mkdir output && tar xf input -C output");
	vine_task_add_input(t,f,"input",VINE_CACHE);
	vine_task_add_output(t,vine_file_local("output"),"output",VINE_CACHE);
	return vine_file_mini_task(t);
}

struct vine_file * vine_file_unponcho( struct vine_file *f)
{
	struct vine_task *t = vine_task_create("./poncho_package_run --unpack-to output -e package.tar.gz");
	char * poncho_path = path_which("poncho_package_run");
	vine_task_add_input(t, vine_file_local(poncho_path), "poncho_package_run", VINE_CACHE);
	vine_task_add_input(t, f, "package.tar.gz", VINE_CACHE);
	vine_task_add_output(t, vine_file_local("output"), "output", VINE_CACHE);
	return vine_file_mini_task(t);
}

struct vine_file * vine_file_unstarch( struct vine_file *f )
{
	struct vine_task *t = vine_task_create("SFX_DIR=output SFX_EXTRACT_ONLY=1 ./package.sfx");
	vine_task_add_input(t,f,"package.sfx",VINE_CACHE);
	vine_task_add_output(t,vine_file_local("output"),"output",VINE_CACHE);
	return vine_file_mini_task(t);
}


static char * find_x509_proxy()
{
	const char *from_env = getenv("X509_USER_PROXY");

	if(from_env) {
		return xxstrdup(from_env);
	} else {
		uid_t uid = getuid();
		const char *tmpdir = getenv("TMPDIR");
		if(!tmpdir) {
			tmpdir = "/tmp";
		}

		char *from_uid = string_format("%s/x509up_u%u", tmpdir, uid);
		if(!access(from_uid, R_OK)) {
			return from_uid;
		}
	}

	return NULL;
}

struct vine_file * vine_file_xrootd( const char *source, struct vine_file *proxy )
{
	if(!proxy) {
		char *proxy_filename = find_x509_proxy();
		if(proxy_filename) {
			proxy = vine_file_local(proxy_filename);
			free(proxy_filename);
		}
	}

	char *command = string_format("xrdcp %s output.root", source);
	struct vine_task *t = vine_task_create(command);

	vine_task_add_output(t,vine_file_local("output.root"),"output.root",VINE_CACHE);

	if(proxy) {
		vine_task_set_env_var(t, "X509_USER_PROXY", "proxy509");
		vine_task_add_input(t,proxy,"proxy509.pem",VINE_CACHE);
	}

	free(command);

	return vine_file_mini_task(t);
}


/* vim: set noexpandtab tabstop=4: */
