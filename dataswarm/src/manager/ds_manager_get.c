
#include "ds_manager_get.h"
#include "ds_worker_info.h"
#include "ds_task.h"
#include "ds_file.h"
#include "ds_protocol.h"
#include "ds_remote_file_info.h"
#include "ds_txn_log.h"

#include "debug.h"
#include "timestamp.h"
#include "link.h"
#include "url_encode.h"
#include "create_dir.h"
#include "path.h"
#include "stringtools.h"
#include "rmsummary.h"
#include "host_disk_info.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

/*
Receive the contents of a single file from a worker.
The "file" header has already been received, just
bring back the streaming data within various constraints.
*/

static ds_result_code_t ds_manager_get_file_contents( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *local_name, int64_t length, int mode )
{
	// If a bandwidth limit is in effect, choose the effective stoptime.
	timestamp_t effective_stoptime = 0;
	if(q->bandwidth_limit) {
		effective_stoptime = (length/q->bandwidth_limit)*1000000 + timestamp_get();
	}

	// Choose the actual stoptime.
	time_t stoptime = time(0) + ds_manager_transfer_wait_time(q, w, t, length);

	// If necessary, create parent directories of the file.
	char dirname[DS_LINE_MAX];
	path_dirname(local_name,dirname);
	if(strchr(local_name,'/')) {
		if(!create_dir(dirname, 0777)) {
			debug(D_DS, "Could not create directory - %s (%s)", dirname, strerror(errno));
			link_soak(w->link, length, stoptime);
			return DS_MGR_FAILURE;
		}
	}

	// Create the local file.
	debug(D_DS, "Receiving file %s (size: %"PRId64" bytes) from %s (%s) ...", local_name, length, w->addrport, w->hostname);

	// Check if there is space for incoming file at manager
	if(!check_disk_space_for_filesize(dirname, length, q->disk_avail_threshold)) {
		debug(D_DS, "Could not receive file %s, not enough disk space (%"PRId64" bytes needed)\n", local_name, length);
		return DS_MGR_FAILURE;
	}

	int fd = open(local_name, O_WRONLY | O_TRUNC | O_CREAT, 0777);
	if(fd < 0) {
		debug(D_NOTICE, "Cannot open file %s for writing: %s", local_name, strerror(errno));
		link_soak(w->link, length, stoptime);
		return DS_MGR_FAILURE;
	}

	// Write the data on the link to file.
	int64_t actual = link_stream_to_fd(w->link, fd, length, stoptime);

	fchmod(fd,mode);

	if(close(fd) < 0) {
		warn(D_DS, "Could not write file %s: %s\n", local_name, strerror(errno));
		unlink(local_name);
		return DS_MGR_FAILURE;
	}

	if(actual != length) {
		debug(D_DS, "Received item size (%"PRId64") does not match the expected size - %"PRId64" bytes.", actual, length);
		unlink(local_name);
		return DS_WORKER_FAILURE;
	}

	// If the transfer was too fast, slow things down.
	timestamp_t current_time = timestamp_get();
	if(effective_stoptime && effective_stoptime > current_time) {
		usleep(effective_stoptime - current_time);
	}

	return DS_SUCCESS;
}

/*
Get the contents of a symlink back from the worker,
after the "symlink" header has already been received.
*/


static ds_result_code_t ds_manager_get_symlink_contents( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *filename, int length )
{
        char *target = malloc(length);

        int actual = link_read(w->link,target,length,time(0)+q->short_timeout);
        if(actual!=length) {
                free(target);
                return DS_WORKER_FAILURE;
        }

        int result = symlink(target,filename);
        if(result<0) {
                debug(D_DS,"could not create symlink %s: %s",filename,strerror(errno));
                free(target);
                return DS_MGR_FAILURE;
        }

        free(target);

        return DS_SUCCESS;
}


static ds_result_code_t ds_manager_get_dir_contents( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *dirname, int64_t *totalsize );

/*
Get a single item (file, dir, symlink, etc) back
from the worker by observing the header and then
pulling the appropriate data on the stream.
Note that if forced_name is non-null, then the item
is stored under that filename.  Otherwise, it is placed
in the directory dirname with the filename given by the
worker.  This allows this function to handle both the
top-level case of renamed files as well as interior files
within a directory.
*/

static ds_result_code_t ds_manager_get_any( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *dirname, const char *forced_name, int64_t *totalsize )
{
	char line[DS_LINE_MAX];
	char name_encoded[DS_LINE_MAX];
	char name[DS_LINE_MAX];
	int64_t size;
	int mode;
	int errornum;

	ds_result_code_t r = DS_WORKER_FAILURE;

	ds_msg_code_t mcode = ds_manager_recv_retry(q, w, line, sizeof(line));
	if(mcode!=DS_MSG_NOT_PROCESSED) return DS_WORKER_FAILURE;

	if(sscanf(line,"file %s %" SCNd64 " 0%o",name_encoded,&size,&mode)==3) {

		url_decode(name_encoded,name,sizeof(name));

		char *subname;
		if(forced_name) {
			subname = strdup(forced_name);
		} else {
			subname = string_format("%s/%s",dirname,name);
		}
		r = ds_manager_get_file_contents(q,w,t,subname,size,mode);
		free(subname);

		if(r==DS_SUCCESS) *totalsize += size;

	} else if(sscanf(line,"symlink %s %" SCNd64,name_encoded,&size)==2) {

		url_decode(name_encoded,name,sizeof(name));

		char *subname;
		if(forced_name) {
			subname = strdup(forced_name);
		} else {
			subname = string_format("%s/%s",dirname,name);
		}
		r = ds_manager_get_symlink_contents(q,w,t,subname,size);
		free(subname);

		if(r==DS_SUCCESS) *totalsize += size;

	} else if(sscanf(line,"dir %s",name_encoded)==1) {

		url_decode(name_encoded,name,sizeof(name));

		char *subname;
		if(forced_name) {
			subname = strdup(forced_name);
		} else {
			subname = string_format("%s/%s",dirname,name);
		}
		r = ds_manager_get_dir_contents(q,w,t,subname,totalsize);
		free(subname);

	} else if(sscanf(line,"missing %s %d",name_encoded,&errornum)==2) {

		// If the output file is missing, we make a note of that in the task result,
		// but we continue and consider the transfer a 'success' so that other
		// outputs are transferred and the task is given back to the caller.
		url_decode(name_encoded,name,sizeof(name));
		debug(D_DS, "%s (%s): could not access requested file %s (%s)",w->hostname,w->addrport,name,strerror(errornum));
		ds_task_update_result(t, DS_RESULT_OUTPUT_MISSING);

		r = DS_SUCCESS;

	} else if(!strcmp(line,"end")) {
		r = DS_END_OF_LIST;

	} else {
		debug(D_DS, "%s (%s): sent invalid response to get: %s",w->hostname,w->addrport,line);
		r = DS_WORKER_FAILURE;
	}

	return r;
}

/*
Retrieve the contents of a directory by creating the local
dir, then receiving each item in the directory until an "end"
header is received.
*/

static ds_result_code_t ds_manager_get_dir_contents( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, const char *dirname, int64_t *totalsize )
{
	int result = mkdir(dirname,0777);
	if(result<0) {
		debug(D_DS,"unable to create %s: %s",dirname,strerror(errno));
		return DS_APP_FAILURE;
	}

	while(1) {
		int r = ds_manager_get_any(q,w,t,dirname,0,totalsize);
		if(r==DS_SUCCESS) {
			// Successfully received one item. 	
			continue;
		} else if(r==DS_END_OF_LIST) {
			// Sucessfully got end of sequence. 
			return DS_SUCCESS;
		} else {
			// Failed to receive item.
			return r;
		}
	}
}

/*
Get a single output file, located at the worker under 'cached_name'.
*/
ds_result_code_t ds_manager_get_output_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t, struct ds_file *f )
{
	int64_t total_bytes = 0;
	ds_result_code_t result = DS_SUCCESS; //return success unless something fails below.

	timestamp_t open_time = timestamp_get();

	debug(D_DS, "%s (%s) sending back %s to %s", w->hostname, w->addrport, f->cached_name, f->source);
	ds_manager_send(q,w, "get %s\n",f->cached_name);

	result = ds_manager_get_any(q, w, t, 0, f->source, &total_bytes);

	timestamp_t close_time = timestamp_get();
	timestamp_t sum_time = close_time - open_time;

	if(total_bytes>0) {
		q->stats->bytes_received += total_bytes;

		t->bytes_received    += total_bytes;
		t->bytes_transferred += total_bytes;

		w->total_bytes_transferred += total_bytes;
		w->total_transfer_time += sum_time;

		debug(D_DS, "%s (%s) sent %.2lf MB in %.02lfs (%.02lfs MB/s) average %.02lfs MB/s", w->hostname, w->addrport, total_bytes / 1000000.0, sum_time / 1000000.0, (double) total_bytes / sum_time, (double) w->total_bytes_transferred / w->total_transfer_time);

		ds_txn_log_write_transfer(q, w, t, f, total_bytes, sum_time, DS_OUTPUT);
	}

	// If we failed to *transfer* the output file, then that is a hard
	// failure which causes this function to return failure and the task
	// to be returned to the queue to be attempted elsewhere.
	// But if we failed to *store* the file, that is a manager failure.

	if(result!=DS_SUCCESS) {
		debug(D_DS, "%s (%s) failed to return output %s to %s", w->addrport, w->hostname, f->cached_name, f->source );

		if(result == DS_APP_FAILURE) {
			ds_task_update_result(t, DS_RESULT_OUTPUT_MISSING);
		} else if(result == DS_MGR_FAILURE) {
			ds_task_update_result(t, DS_RESULT_OUTPUT_TRANSFER_ERROR);
		}
	}

	// If the transfer was successful, make a record of it in the cache.
	if(result == DS_SUCCESS && f->flags & DS_CACHE) {
		struct stat local_info;
		if (stat(f->source,&local_info) == 0) {
			struct ds_remote_file_info *remote_info = ds_remote_file_info_create(f->type,local_info.st_size,local_info.st_mtime);
			hash_table_insert(w->current_files, f->cached_name, remote_info);
		} else {
			debug(D_NOTICE, "Cannot stat file %s: %s", f->source, strerror(errno));
		}
	}

	return result;
}

/* Get all output files produced by a given task on this worker. */

ds_result_code_t ds_manager_get_output_files( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t )
{
	struct ds_file *f;
	ds_result_code_t result = DS_SUCCESS;

	if(t->output_files) {
		list_first_item(t->output_files);
		while((f = list_next_item(t->output_files))) {
			// non-file objects are handled by the worker.
			if(f->type!=DS_FILE) continue;
		     
			int task_succeeded = (t->result==DS_RESULT_SUCCESS && t->exit_code==0);

			// skip failure-only files on success 
			if(f->flags&DS_FAILURE_ONLY && task_succeeded) continue;

 			// skip success-only files on failure
			if(f->flags&DS_SUCCESS_ONLY && !task_succeeded) continue;

			// otherwise, get the file.
			result = ds_manager_get_output_file(q,w,t,f);

			//if success or app-level failure, continue to get other files.
			//if worker failure, return.
			if(result == DS_WORKER_FAILURE) {
				break;
			}
		}
	}

	// tell the worker you no longer need that task's output directory.
	ds_manager_send(q,w, "kill %d\n",t->taskid);

	return result;
}

/*
Get only the resource monitor output file for a given task,
usually because the task has failed, and we want to know why.
*/

ds_result_code_t ds_manager_get_monitor_output_file( struct ds_manager *q, struct ds_worker_info *w, struct ds_task *t )
{
	struct ds_file *f;
	ds_result_code_t result = DS_SUCCESS;

	const char *summary_name = RESOURCE_MONITOR_REMOTE_NAME ".summary";

	if(t->output_files) {
		list_first_item(t->output_files);
		while((f = list_next_item(t->output_files))) {
			if(!strcmp(summary_name, f->remote_name)) {
				result = ds_manager_get_output_file(q,w,t,f);
				break;
			}
		}
	}

	// tell the worker you no longer need that task's output directory.
	ds_manager_send(q,w, "kill %d\n",t->taskid);

	return result;
}

