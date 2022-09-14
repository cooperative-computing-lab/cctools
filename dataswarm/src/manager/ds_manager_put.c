#include "ds_manager_put.h"
#include "ds_worker_info.h"
#include "ds_task.h"
#include "ds_file.h"
#include "ds_protocol.h"
#include "ds_remote_file_info.h"
#include "ds_transaction.h"

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

/*
Send a symbolic link to the remote worker.
Note that the target of the link is sent
as the "body" of the link, following the
message header.
*/

static int ds_manager_put_symlink( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *localname, const char *remotename, int64_t *total_bytes )
{
	char target[DS_LINE_MAX];

	int length = readlink(localname,target,sizeof(target));
	if(length<0) return DS_APP_FAILURE;

	char remotename_encoded[DS_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	ds_manager_send(q,w,"symlink %s %d\n",remotename_encoded,length);

	link_write(w->link,target,length,time(0)+q->long_timeout);

	*total_bytes += length;

	return DS_SUCCESS;
}

/*
Send a single file (or a piece of a file) to the remote worker.
The transfer time is controlled by the size of the file.
If the transfer takes too long, then abort.
*/

static int ds_manager_put_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *localname, const char *remotename, off_t offset, int64_t length, struct stat info, int64_t *total_bytes )
{
	time_t stoptime;
	timestamp_t effective_stoptime = 0;
	int64_t actual = 0;

	/* normalize the mode so as not to set up invalid permissions */
	int mode = ( info.st_mode | 0x600 ) & 0777;

	if(!length) {
		length = info.st_size;
	}

	int fd = open(localname, O_RDONLY, 0);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s: %s", localname, strerror(errno));
		return DS_APP_FAILURE;
	}

	/* If we are sending only a piece of the file, seek there first. */

	if (offset >= 0 && (offset+length) <= info.st_size) {
		if(lseek(fd, offset, SEEK_SET) == -1) {
			debug(D_NOTICE, "Cannot seek file %s to offset %lld: %s", localname, (long long) offset, strerror(errno));
			close(fd);
			return DS_APP_FAILURE;
		}
	} else {
		debug(D_NOTICE, "File specification %s (%lld:%lld) is invalid", localname, (long long) offset, (long long) offset+length);
		close(fd);
		return DS_APP_FAILURE;
	}

	if(q->bandwidth_limit) {
		effective_stoptime = (length/q->bandwidth_limit)*1000000 + timestamp_get();
	}

	/* filenames are url-encoded to avoid problems with spaces, etc */
	char remotename_encoded[DS_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	stoptime = time(0) + ds_manager_transfer_wait_time(q, w, t, length);
	ds_manager_send(q,w, "file %s %"PRId64" 0%o\n",remotename_encoded, length, mode );
	actual = link_stream_from_fd(w->link, fd, length, stoptime);
	close(fd);

	*total_bytes += actual;

	if(actual != length) return DS_WORKER_FAILURE;

	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return DS_SUCCESS;
}

/* Need prototype here to address mutually recursive code. */

static ds_result_code_t ds_manager_put_item( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *name, const char *remotename, int64_t offset, int64_t length, int64_t * total_bytes, int follow_links );

/*
Send a directory and all of its contents using the new streaming protocol.
Do this by sending a "dir" prefix, then all of the directory contents,
and then an "end" marker.
*/

static ds_result_code_t ds_manager_put_directory( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *localname, const char *remotename, int64_t * total_bytes )
{
	DIR *dir = opendir(localname);
	if(!dir) {
		debug(D_NOTICE, "Cannot open dir %s: %s", localname, strerror(errno));
		return DS_APP_FAILURE;
	}

	ds_result_code_t result = DS_SUCCESS;

	char remotename_encoded[DS_LINE_MAX];
	url_encode(remotename,remotename_encoded,sizeof(remotename_encoded));

	ds_manager_send(q,w,"dir %s\n",remotename_encoded);

	struct dirent *d;
	while((d = readdir(dir))) {
		if(!strcmp(d->d_name, ".") || !strcmp(d->d_name, "..")) continue;

		char *localpath = string_format("%s/%s",localname,d->d_name);

		result = ds_manager_put_item( q, w, t, localpath, d->d_name, 0, 0, total_bytes, 0 );

		free(localpath);

		if(result != DS_SUCCESS) break;
	}

	ds_manager_send(q,w,"end\n");

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


static ds_result_code_t ds_manager_put_item( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *localpath, const char *remotepath, int64_t offset, int64_t length, int64_t * total_bytes, int follow_links )
{
	struct stat info;
	int result = DS_SUCCESS;

	if(follow_links) {
		result = stat(localpath,&info);
	} else {
		result = lstat(localpath,&info);
	}

	if(result>=0) {
		if(S_ISDIR(info.st_mode))  {
			result = ds_manager_put_directory( q, w, t, localpath, remotepath, total_bytes );
		} else if(S_ISLNK(info.st_mode)) {
			result = ds_manager_put_symlink( q, w, t, localpath, remotepath, total_bytes );
		} else if(S_ISREG(info.st_mode)) {
			result = ds_manager_put_file( q, w, t, localpath, remotepath, offset, length, info, total_bytes );
		} else {
			debug(D_NOTICE,"skipping unusual file: %s",strerror(errno));
		}
	} else {
		debug(D_NOTICE, "cannot stat file %s: %s", localpath, strerror(errno));
		result = DS_APP_FAILURE;
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

static ds_result_code_t ds_manager_put_item_if_not_cached( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *tf, const char *expanded_local_name, int64_t * total_bytes)
{
	struct stat local_info;
	if(lstat(expanded_local_name, &local_info) < 0) {
		debug(D_NOTICE, "Cannot stat file %s: %s", expanded_local_name, strerror(errno));
		return DS_APP_FAILURE;
	}

	struct ds_remote_file_info *remote_info = hash_table_lookup(w->current_files, tf->cached_name);

	if(remote_info && (remote_info->mtime != local_info.st_mtime || remote_info->size != local_info.st_size)) {
		debug(D_NOTICE|D_DS, "File %s changed locally. Task %d will be executed with an older version.", expanded_local_name, t->taskid);
		return DS_SUCCESS;
	} else if(!remote_info) {

		if(tf->offset==0 && tf->length==0) {
			debug(D_DS, "%s (%s) needs file %s as '%s'", w->hostname, w->addrport, expanded_local_name, tf->cached_name);
		} else {
			debug(D_DS, "%s (%s) needs file %s (offset %lld length %lld) as '%s'", w->hostname, w->addrport, expanded_local_name, (long long) tf->offset, (long long) tf->length, tf->cached_name );
		}

		ds_result_code_t result;
		result = ds_manager_put_item(q, w, t, expanded_local_name, tf->cached_name, tf->offset, tf->piece_length, total_bytes, 1 );

		if(result == DS_SUCCESS && tf->flags & DS_CACHE) {
			remote_info = ds_remote_file_info_create(tf->type,local_info.st_size,local_info.st_mtime);
			hash_table_insert(w->current_files, tf->cached_name, remote_info);
		}

		return result;
	} else {
		/* Up-to-date file on the worker, we do nothing. */
		return DS_SUCCESS;
	}
}

/**
 *	This function expands Data Swarm environment variables such as
 * 	$OS, $ARCH, that are specified in the definition of Data Swarm
 * 	input files. It expands these variables based on the info reported
 *	by each connected worker.
 *	Will always return a non-empty string. That is if no match is found
 *	for any of the environment variables, it will return the input string
 *	as is.
 * 	*/
static char *expand_envnames(struct ds_worker_info *w, const char *source)
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

	debug(D_DS, "File name %s expanded to %s for %s (%s).", source, expanded_name, w->hostname, w->addrport);

	return expanded_name;
}

/*
Send a url or remote command used to generate a cached file,
if it has not already been cached there.  Note that the length
may be an estimate at this point and will be updated by return
message once the object is actually loaded into the cache.
*/

static ds_result_code_t ds_manager_put_special_if_not_cached( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *tf, const char *typestring )
{
	if(hash_table_lookup(w->current_files,tf->cached_name)) return DS_SUCCESS;

	char source_encoded[DS_LINE_MAX];
	char cached_name_encoded[DS_LINE_MAX];

	url_encode(tf->source,source_encoded,sizeof(source_encoded));
	url_encode(tf->cached_name,cached_name_encoded,sizeof(cached_name_encoded));

	ds_manager_send(q,w,"%s %s %s %d %o\n",typestring, source_encoded, cached_name_encoded, tf->length, 0777);

	if(tf->flags & DS_CACHE) {
		struct ds_remote_file_info *remote_info = ds_remote_file_info_create(tf->type,tf->length,time(0));
		hash_table_insert(w->current_files,tf->cached_name,remote_info);
	}

	return DS_SUCCESS;
}

static ds_result_code_t ds_manager_put_input_file(struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f)
{

	int64_t total_bytes = 0;
	int64_t actual = 0;
	ds_result_code_t result = DS_SUCCESS; //return success unless something fails below

	timestamp_t open_time = timestamp_get();

	switch (f->type) {

	case DS_BUFFER:
		debug(D_DS, "%s (%s) needs literal as %s", w->hostname, w->addrport, f->remote_name);
		time_t stoptime = time(0) + ds_manager_transfer_wait_time(q, w, t, f->length);
		ds_manager_send(q,w, "file %s %d %o\n",f->cached_name, f->length, 0777 );
		actual = link_putlstring(w->link, f->source, f->length, stoptime);
		if(actual!=f->length) {
			result = DS_WORKER_FAILURE;
		}
		total_bytes = actual;
		break;

	case DS_REMOTECMD:
		debug(D_DS, "%s (%s) will get %s via remote command \"%s\"", w->hostname, w->addrport, f->remote_name, f->source);
		result = ds_manager_put_special_if_not_cached(q,w,t,f,"putcmd");
		break;

	case DS_URL:
		debug(D_DS, "%s (%s) will get %s from url %s", w->hostname, w->addrport, f->remote_name, f->source);
		result = ds_manager_put_special_if_not_cached(q,w,t,f,"puturl");
		break;

	case DS_DIRECTORY:
		debug(D_DS, "%s (%s) will create directory %s", w->hostname, w->addrport, f->remote_name);
  		// Do nothing.  Empty directories are handled by the task specification, while recursive directories are implemented as DS_FILEs
		break;

	case DS_FILE:
	case DS_FILE_PIECE: {
		char *expanded_source = expand_envnames(w, f->source);
		if(expanded_source) {
			result = ds_manager_put_item_if_not_cached(q,w,t,f,expanded_source,&total_bytes);
			free(expanded_source);
		} else {
			result = DS_APP_FAILURE; //signal app-level failure.
		}
		break;
		}
	}

	if(result == DS_SUCCESS) {
		timestamp_t close_time = timestamp_get();
		timestamp_t elapsed_time = close_time-open_time;

		t->bytes_sent        += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time     += elapsed_time;

		q->stats->bytes_sent += total_bytes;

		// Write to the transaction log.
		ds_transaction_write_transfer(q, w, t, f, total_bytes, elapsed_time, DS_INPUT);

		// Avoid division by zero below.
		if(elapsed_time==0) elapsed_time = 1;

		if(total_bytes > 0) {
			debug(D_DS, "%s (%s) received %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s",
				w->hostname,
				w->addrport,
				total_bytes / 1000000.0,
				elapsed_time / 1000000.0,
				(double) total_bytes / elapsed_time,
				(double) w->total_bytes_transferred / w->total_transfer_time
			);
		}
	} else {
		debug(D_DS, "%s (%s) failed to send %s (%" PRId64 " bytes sent).",
			w->hostname,
			w->addrport,
			f->type == DS_BUFFER ? "literal data" : f->source,
			total_bytes);

		if(result == DS_APP_FAILURE) {
			ds_task_update_result(t, DS_RESULT_INPUT_MISSING);
		}
	}

	return result;
}

ds_result_code_t ds_manager_put_input_files( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t )
{
	struct ds_file *f;
	struct stat s;

	// Check for existence of each input file first.
	// If any one fails to exist, set the failure condition and return failure.
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			if(f->type == DS_FILE || f->type == DS_FILE_PIECE) {
				char * expanded_source = expand_envnames(w, f->source);
				if(!expanded_source) {
					ds_task_update_result(t, DS_RESULT_INPUT_MISSING);
					return DS_APP_FAILURE;
				}
				if(stat(expanded_source, &s) != 0) {
					debug(D_DS,"Could not stat %s: %s\n", expanded_source, strerror(errno));
					free(expanded_source);
					ds_task_update_result(t, DS_RESULT_INPUT_MISSING);
					return DS_APP_FAILURE;
				}
				free(expanded_source);
			}
		}
	}

	// Send each of the input files.
	// If any one fails to be sent, return failure.
	if(t->input_files) {
		list_first_item(t->input_files);
		while((f = list_next_item(t->input_files))) {
			ds_result_code_t result = ds_manager_put_input_file(q,w,t,f);
			if(result != DS_SUCCESS) {
				return result;
			}
		}
	}

	return DS_SUCCESS;
}

