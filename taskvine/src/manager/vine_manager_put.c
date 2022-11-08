/*
Copyright (C) 2022- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "vine_manager_put.h"
#include "vine_worker_info.h"
#include "vine_task.h"
#include "vine_file.h"
#include "vine_protocol.h"
#include "vine_remote_file_info.h"
#include "vine_txn_log.h"
#include "vine_current_transfers.h"

#include "debug.h"
#include "timestamp.h"
#include "link.h"
#include "url_encode.h"
#include "create_dir.h"
#include "path.h"
#include "stringtools.h"
#include "rmsummary.h"
#include "host_disk_info.h"
#include "xxmalloc.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

char *vine_monitor_wrap(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct rmsummary *limits);

/*
Send a symbolic link to the remote worker.
Note that the target of the link is sent
as the "body" of the link, following the
message header.
*/

static int vine_manager_put_symlink( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, const char *localname, const char *remotename, int64_t *total_bytes )
{
	char target[VINE_LINE_MAX];

	int length = readlink(localname,target,sizeof(target));
	if(length<0) return VINE_APP_FAILURE;

	char remotename_encoded[VINE_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	vine_manager_send(q,w,"symlink %s %d\n",remotename_encoded,length);

	link_write(w->link,target,length,time(0)+q->long_timeout);

	*total_bytes += length;

	return VINE_SUCCESS;
}

/*
Send a single file to the remote worker.
The transfer time is controlled by the size of the file.
If the transfer takes too long, then cancel it.
*/

static int vine_manager_put_file( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, const char *localname, const char *remotename, struct stat info, int64_t *total_bytes )
{
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	int64_t actual = 0;

	/* normalize the mode so as not to set up invalid permissions */
	int mode = ( info.st_mode | 0x600 ) & 0777;

	int64_t length = info.st_size;

	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s: %s", localname, strerror(errno));
		return VINE_APP_FAILURE;
	}

	if(q->bandwidth_limit) {
		effective_stoptime = (length/q->bandwidth_limit)*1000000 + timestamp_get();
	}

	/* filenames are url-encoded to avoid problems with spaces, etc */
	char remotename_encoded[VINE_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	stoptime = time(0) + vine_manager_transfer_time(q, w, t, length);
	vine_manager_send(q,w, "file %s %"PRId64" 0%o\n",remotename_encoded, length, mode );
	actual = link_stream_from_fd(w->link, fd, length, stoptime);
	close(fd);

	*total_bytes += actual;

	if(actual != length) return VINE_WORKER_FAILURE;

	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return VINE_SUCCESS;
}

/* Need prototype here to address mutually recursive code. */

static vine_result_code_t vine_manager_put_item( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, const char *name, const char *remotename, int64_t * total_bytes, int follow_links );

/*
Send a directory and all of its contents using the new streaming protocol.
Do this by sending a "dir" prefix, then all of the directory contents,
and then an "end" marker.
*/

static vine_result_code_t vine_manager_put_directory( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, const char *localname, const char *remotename, int64_t * total_bytes )
{
	DIR *dir = opendir(localname);
	if(!dir) {
		debug(D_NOTICE, "Cannot open dir %s: %s", localname, strerror(errno));
		return VINE_APP_FAILURE;
	}

	vine_result_code_t result = VINE_SUCCESS;

	char remotename_encoded[VINE_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	vine_manager_send(q,w,"dir %s\n",remotename_encoded);

	struct dirent *d;
	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..")) continue;

		char *localpath = string_format("%s/%s",localname,d->d_name);

		result = vine_manager_put_item( q, w, t, localpath, d->d_name, total_bytes, 0 );

		free(localpath);

		if(result != VINE_SUCCESS) break;
	}

	vine_manager_send(q,w,"end\n");

	closedir(dir);
	return result;
}

/*
Send a single item, whether it is a directory, symlink, or file.

Note 1: We call stat/lstat here a single time, and then pass it
to the underlying object so as not to minimize syscall work.

Note 2: This function is invoked at the top level with follow_links=1,
since it is common for the user to to pass in a top-level symbolic
link to a file or directory which they want transferred.
However, in recursive calls, follow_links is set to zero,
and internal links are not followed, they are sent natively.
*/


static vine_result_code_t vine_manager_put_item( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, const char *localpath, const char *remotepath, int64_t * total_bytes, int follow_links )
{
	struct stat info;
	int result = VINE_SUCCESS;

	if(follow_links) {
		result = stat(localpath,&info);
	} else {
		result = lstat(localpath,&info);
	}

	if(result>=0) {
		if(S_ISDIR(info.st_mode))  {
			result = vine_manager_put_directory( q, w, t, localpath, remotepath, total_bytes );
		} else if(S_ISLNK(info.st_mode)) {
			result = vine_manager_put_symlink( q, w, t, localpath, remotepath, total_bytes );
		} else if(S_ISREG(info.st_mode)) {
			result = vine_manager_put_file( q, w, t, localpath, remotepath, info, total_bytes );
		} else {
			debug(D_NOTICE,"skipping unusual file: %s",strerror(errno));
		}
	} else {
		debug(D_NOTICE, "cannot stat file %s: %s", localpath, strerror(errno));
		result = VINE_APP_FAILURE;
	}

	return result;
}

/*
Send an item to a remote worker, if it is not already cached.
The local file name should already have been expanded by the caller.
If it is in the worker, but a new version is available, warn and return.
We do not want to rewrite the file while some other task may be using it.
Otherwise, send it to the worker.
*/

static vine_result_code_t vine_manager_put_item_if_not_cached( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_file *tf, const char *expanded_local_name, int64_t * total_bytes)
{
	struct stat local_info;
	if(lstat(expanded_local_name, &local_info) < 0) {
		debug(D_NOTICE, "Cannot stat file %s: %s", expanded_local_name, strerror(errno));
		return VINE_APP_FAILURE;
	}

	struct vine_remote_file_info *remote_info = hash_table_lookup(w->current_files, tf->cached_name);

	if(remote_info && (remote_info->mtime != local_info.st_mtime || remote_info->size != local_info.st_size)) {
		debug(D_NOTICE|D_VINE, "File %s changed locally. Task %d will be executed with an older version.", expanded_local_name, t->task_id);
		return VINE_SUCCESS;
	} else if(!remote_info) {

		debug(D_VINE, "%s (%s) needs file %s as '%s'", w->hostname, w->addrport, expanded_local_name, tf->cached_name);

		vine_result_code_t result;
		result = vine_manager_put_item(q, w, t, expanded_local_name, tf->cached_name, total_bytes, 1 );

		if(result == VINE_SUCCESS && tf->flags & VINE_CACHE) {
			remote_info = vine_remote_file_info_create(tf->type,local_info.st_size,local_info.st_mtime);
			hash_table_insert(w->current_files, tf->cached_name, remote_info);
		}

		return result;
	} else {
		/* Up-to-date file on the worker, we do nothing. */
		return VINE_SUCCESS;
	}
}

/*
This function expands environment variables such as
$OS, $ARCH, that are specified in the definition of taskvine
input files. It expands these variables based on the info reported
by each connected worker.
Will always return a non-empty string. That is if no match is found
for any of the environment variables, it will return the input string
as is.
*/

static char *expand_envnames(struct vine_worker_info *w, const char *source)
{
	char *expanded_name;
	char *str, *curr_pos;
	char *delimtr = "$";
	char *token;

	// Shortcut: If no dollars anywhere, duplicate the whole string.
	if(!strchr(source,'$')) return strdup(source);

	str = xxstrdup(source);

	expanded_name = (char *) malloc(strlen(source) + (50 * sizeof(char)));
	if(expanded_name == NULL) {
		debug(D_NOTICE, "Cannot allocate memory for filename %s.\n", source);
		return NULL;
	} else {
		//Initialize to null byte so it works correctly with strcat.
		*expanded_name = '\0';
	}

	token = strtok(str, delimtr);
	while(token) {
		if((curr_pos = strstr(token, "ARCH"))) {
			if((curr_pos - token) == 0) {
				strcat(expanded_name, w->arch);
				strcat(expanded_name, token + 4);
			} else {
				//No match. So put back '$' and rest of the string.
				strcat(expanded_name, "$");
				strcat(expanded_name, token);
			}
		} else if((curr_pos = strstr(token, "OS"))) {
			if((curr_pos - token) == 0) {
				//Cygwin oddly reports OS name in all caps and includes version info.
				if(strstr(w->os, "CYGWIN")) {
					strcat(expanded_name, "Cygwin");
				} else {
					strcat(expanded_name, w->os);
				}
				strcat(expanded_name, token + 2);
			} else {
				strcat(expanded_name, "$");
				strcat(expanded_name, token);
			}
		} else {
			//If token and str don't point to same location, then $ sign was before token and needs to be put back.
			if((token - str) > 0) {
				strcat(expanded_name, "$");
			}
			strcat(expanded_name, token);
		}
		token = strtok(NULL, delimtr);
	}

	free(str);

	debug(D_VINE, "File name %s expanded to %s for %s (%s).", source, expanded_name, w->hostname, w->addrport);

	return expanded_name;
}

/*
Send a url to generate a cached file,
if it has not already been cached there.  Note that the length
may be an estimate at this point and will be updated by return
message once the object is actually loaded into the cache.
*/

static vine_result_code_t vine_manager_put_url_if_not_cached( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_file *tf )
{
	if(hash_table_lookup(w->current_files,tf->cached_name)) return VINE_SUCCESS;

	char source_encoded[VINE_LINE_MAX];
	char cached_name_encoded[VINE_LINE_MAX];

	url_encode(tf->source,source_encoded,sizeof(source_encoded));
	url_encode(tf->cached_name,cached_name_encoded,sizeof(cached_name_encoded));
									
	char *transfer_id = vine_current_transfers_add(q, w, strdup(tf->source));

	vine_manager_send(q,w,"puturl %s %s %d %o %d\n",source_encoded, cached_name_encoded, tf->length, 0777,tf->flags );

	if(tf->flags & VINE_CACHE) {
		struct vine_remote_file_info *remote_info = vine_remote_file_info_create(tf->type,tf->length,time(0));
		hash_table_insert(w->current_files,tf->cached_name,remote_info);
	}

	return VINE_SUCCESS;
}

/*
Send a mini_task that will be used to generate the target file,
if it has not already been sent.
*/

static vine_result_code_t vine_manager_put_mini_task_if_not_cached( struct vine_manager *q, struct vine_worker_info *w, struct vine_file *tf )
{
	if(hash_table_lookup(w->current_files,tf->cached_name)) return VINE_SUCCESS;

	vine_manager_put_task(q,w,tf->mini_task,0,0,tf);

	if(tf->flags & VINE_CACHE) {
		struct vine_remote_file_info *remote_info = vine_remote_file_info_create(tf->type,tf->length,time(0));
		hash_table_insert(w->current_files,tf->cached_name,remote_info);
	}

	return VINE_SUCCESS;
}

/*
Send a single input file of any type to the given worker, and record the performance.
If the file has a chained dependency, send that first.
*/

static vine_result_code_t vine_manager_put_input_file(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, struct vine_file *f)
{
	int64_t total_bytes = 0;
	int64_t actual = 0;
	vine_result_code_t result = VINE_SUCCESS; //return success unless something fails below
	
	if(f->flags & VINE_PONCHO_UNPACK){
		char * poncho_path = path_which("poncho_package_run");
		struct vine_file *p = vine_file_create(poncho_path , "poncho_package_run", 0, 0, VINE_FILE, VINE_CACHE,0);
		vine_manager_put_input_file(q, w, t, p);
	}

	timestamp_t open_time = timestamp_get();

	switch (f->type) {
	case VINE_BUFFER:
		debug(D_VINE, "%s (%s) needs literal as %s", w->hostname, w->addrport, f->remote_name);
		time_t stoptime = time(0) + vine_manager_transfer_time(q, w, t, f->length);
		vine_manager_send(q,w, "file %s %d %o\n",f->cached_name, f->length, 0777 );
		actual = link_putlstring(w->link, f->data, f->length, stoptime);
		if(actual!=f->length) {
			result = VINE_WORKER_FAILURE;
		}
		total_bytes = actual;
		break;

	case VINE_MINI_TASK:
		debug(D_VINE, "%s (%s) will produce %s via mini task %d", w->hostname, w->addrport, f->remote_name, f->mini_task->task_id);
		result = vine_manager_put_mini_task_if_not_cached(q,w,f);
		break;

	case VINE_URL:
		debug(D_VINE, "%s (%s) will get %s from url %s", w->hostname, w->addrport, f->remote_name, f->source);
		result = vine_manager_put_url_if_not_cached(q,w,t,f);
		break;

	case VINE_EMPTY_DIR:
		debug(D_VINE, "%s (%s) will create directory %s", w->hostname, w->addrport, f->remote_name);
  		// Do nothing.  Empty directories are handled by the task specification, while recursive directories are implemented as VINE_FILEs
		break;

	case VINE_FILE: {
		char *expanded_source = expand_envnames(w, f->source);
		if(expanded_source) {
			result = vine_manager_put_item_if_not_cached(q,w,t,f,expanded_source,&total_bytes);
			free(expanded_source);
		} else {
			result = VINE_APP_FAILURE; //signal app-level failure.
		}
		break;
		}
	}

	if(result == VINE_SUCCESS) {
		timestamp_t close_time = timestamp_get();
		timestamp_t elapsed_time = close_time-open_time;

		t->bytes_sent        += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time     += elapsed_time;

		q->stats->bytes_sent += total_bytes;

		// Write to the transaction log.
		vine_txn_log_write_transfer(q, w, t, f, total_bytes, elapsed_time, 1);

		// Avoid division by zero below.
		if(elapsed_time==0) elapsed_time = 1;

		if(total_bytes > 0) {
			debug(D_VINE, "%s (%s) received %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s",
				w->hostname,
				w->addrport,
				total_bytes / 1000000.0,
				elapsed_time / 1000000.0,
				(double) total_bytes / elapsed_time,
				(double) w->total_bytes_transferred / w->total_transfer_time
			);
		}
	} else {
		debug(D_VINE, "%s (%s) failed to send %s (%" PRId64 " bytes sent).",
			w->hostname,
			w->addrport,
			f->type == VINE_BUFFER ? "literal data" : f->source,
			total_bytes);

		if(result == VINE_APP_FAILURE) {
			vine_task_set_result(t, VINE_RESULT_INPUT_MISSING);
		}
	}

	return result;
}

static char *vine_manager_can_any_transfer( struct vine_manager *q, struct vine_worker_info *w, struct vine_file *f)
{
		char *id;
		struct vine_worker_info *peer;
		struct vine_remote_file_info *remote_info;

	
		vine_current_transfers_print_table(q);

		// check original source first
		if(vine_current_transfers_source_in_use(q, f->source) < VINE_FILE_SOURCE_MAX_TRANSFERS)
		{
			return f->source;
		}
		
		// if busy, check workers
		HASH_TABLE_ITERATE(q->worker_table, id, peer){
			if((remote_info = hash_table_lookup(peer->current_files, f->cached_name)))
			{
				char *peer_source =  string_format("worker://%s:%d/%s", peer->transfer_addr, peer->transfer_port, f->cached_name);
				if(vine_current_transfers_source_in_use(q, peer_source) < VINE_FILE_SOURCE_MAX_TRANSFERS)
				{
					if(remote_info->in_cache)
					{
						debug(D_VINE, "This file is to be requested from source: %s:%d", peer->transfer_addr, peer->transfer_port);
						return peer_source;
					}
				}else{
					debug(D_VINE, "Vine transfer source busy: %s:%d", peer->transfer_addr, peer->transfer_port);
				}
			}
		}
		return NULL;
}

/* Send all input files needed by a task to the given worker. */

vine_result_code_t vine_manager_put_input_files( struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t )
{
	struct vine_file *f;
	struct stat s;

	// Check for existence of each input file first.
	// If any one fails to exist, set the failure condition and return failure.
	if(t->input_files) {
		LIST_ITERATE(t->input_files,f) {
			if(f->type == VINE_FILE) {
				char * expanded_source = expand_envnames(w, f->source);
				if(!expanded_source) {
					vine_task_set_result(t, VINE_RESULT_INPUT_MISSING);
					return VINE_APP_FAILURE;
				}
				if(stat(expanded_source, &s) != 0) {
					debug(D_VINE,"Could not stat %s: %s\n", expanded_source, strerror(errno));
					free(expanded_source);
					vine_task_set_result(t, VINE_RESULT_INPUT_MISSING);
					return VINE_APP_FAILURE;
				}
				free(expanded_source);
			}
		}
	}

	// Send each of the input files.
	// If any one fails to be sent, return failure.
	if(t->input_files) {
		LIST_ITERATE(t->input_files,f) {
			vine_result_code_t result;

			// look for the best source for this file
			char *source;
			if((source = vine_manager_can_any_transfer(q, w, f))) { 
				struct vine_file *worker_file = vine_file_create(source, f->remote_name, f->data, f->length, VINE_URL, f->flags);
				free(worker_file->cached_name);
				worker_file->cached_name = strdup(f->cached_name);

				result = vine_manager_put_input_file(q, w, t, worker_file);
				free(source);
				vine_file_delete(worker_file);
			}else{
				return 1;
			}

			if(result != VINE_SUCCESS)
			{
				return result;
			}
			
		}
	}
	return VINE_SUCCESS;
}

/*
Send the details of one task to a worker.
Note that this function just performs serialization of the task definition.
It does not perform any resource management.
This allows it to be used for both regular tasks and mini tasks.
*/

vine_result_code_t vine_manager_put_task(struct vine_manager *q, struct vine_worker_info *w, struct vine_task *t, const char *command_line, struct rmsummary *limits, struct vine_file *target )
{
	vine_result_code_t result = vine_manager_put_input_files(q, w, t);
	if(result!=VINE_SUCCESS) return result;

	if(target) {
		vine_manager_send(q,w, "mini_task %lld %s %d %o %d\n",(long long)target->mini_task->task_id,target->cached_name,target->length,0777,target->flags);
	} else {
		vine_manager_send(q,w, "task %lld\n",(long long)t->task_id);
	}
	
	if(!command_line) {
		command_line = t->command_line;
	}
	
	long long cmd_len = strlen(command_line);
	vine_manager_send(q,w, "cmd %lld\n", (long long) cmd_len);
	link_putlstring(w->link, command_line, cmd_len, time(0) + q->short_timeout);
	debug(D_VINE, "%s\n", command_line);

	if(t->coprocess) {
		cmd_len = strlen(t->coprocess);
		vine_manager_send(q,w, "coprocess %lld\n", (long long) cmd_len);
		link_putlstring(w->link, t->coprocess, cmd_len, /* stoptime */ time(0) + q->short_timeout);
	}

	vine_manager_send(q,w, "category %s\n", t->category);

	if(limits) {
		vine_manager_send(q,w, "cores %s\n",  rmsummary_resource_to_str("cores", limits->cores, 0));
		vine_manager_send(q,w, "gpus %s\n",   rmsummary_resource_to_str("gpus", limits->gpus, 0));
		vine_manager_send(q,w, "memory %s\n", rmsummary_resource_to_str("memory", limits->memory, 0));
		vine_manager_send(q,w, "disk %s\n",   rmsummary_resource_to_str("disk", limits->disk, 0));
	
		/* Do not set end, wall_time if running the resource monitor. We let the monitor police these resources. */
		if(q->monitor_mode == VINE_MON_DISABLED) {
			if(limits->end > 0) {
				vine_manager_send(q,w, "end_time %s\n",  rmsummary_resource_to_str("end", limits->end, 0));
			}
			if(limits->wall_time > 0) {
				vine_manager_send(q,w, "wall_time %s\n", rmsummary_resource_to_str("wall_time", limits->wall_time, 0));
			}
		}
	}

	/* Note that even when environment variables after resources, values for
	 * CORES, MEMORY, etc. will be set at the worker to the values of
	 * set_*, if used. */
	char *var;
	LIST_ITERATE(t->env_list,var) {
		vine_manager_send(q, w,"env %zu\n%s\n", strlen(var), var);
	}

	if(t->input_files) {
		struct vine_file *tf;
		LIST_ITERATE(t->input_files,tf) {
			if(tf->type == VINE_EMPTY_DIR) {
				vine_manager_send(q,w, "dir %s\n", tf->remote_name);
			} else {
				char remote_name_encoded[PATH_MAX];
				url_encode(tf->remote_name, remote_name_encoded, PATH_MAX);
				vine_manager_send(q,w, "infile %s %s %d\n", tf->cached_name, remote_name_encoded, tf->flags);
			}
		}
	}

	if(t->output_files) {
		struct vine_file *tf;
		LIST_ITERATE(t->output_files,tf) {
			char remote_name_encoded[PATH_MAX];
			url_encode(tf->remote_name, remote_name_encoded, PATH_MAX);
			vine_manager_send(q,w, "outfile %s %s %d\n", tf->cached_name, remote_name_encoded, tf->flags);
		}
	}

	// vine_manager_send returns the number of bytes sent, or a number less than
	// zero to indicate errors. We are lazy here, we only check the last
	// message we sent to the worker (other messages may have failed above).

	int r = vine_manager_send(q,w,"end\n");
	if(r>=0) {
		return VINE_SUCCESS;
	} else {
		return VINE_WORKER_FAILURE;
	}	
}
