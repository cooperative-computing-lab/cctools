
#include "dataswarm_blob.h"
#include "dataswarm_message.h"

#include "stringtools.h"
#include "debug.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "jx.h"
#include "delete_dir.h"
#include "create_dir.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

dataswarm_result_t dataswarm_blob_create(struct dataswarm_worker *w, const char *blobid, jx_int_t size, struct jx *meta )
{
	if(!blobid || size < 1) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}
	// XXX should here check for available space

	char *blob_dir = string_format("%s/blob/rw/%s", w->workspace, blobid);
	char *blob_meta = string_format("%s/meta", blob_dir);

	if(mkdir(blob_dir, 0777)<0) {
		debug(D_DATASWARM, "couldn't mkdir %s: %s", blob_dir, strerror(errno));
		free(blob_dir);
		free(blob_meta);
		return DS_RESULT_UNABLE;
	}

	if(meta) {
		FILE *file = fopen(blob_meta, "w");
		if(!file) {
			debug(D_DATASWARM, "couldn't open %s: %s", blob_meta, strerror(errno));
			free(blob_dir);
			free(blob_meta);
			return DS_RESULT_UNABLE;
		}
		jx_print_stream(meta, file);
		fclose(file);
	}

	free(blob_dir);
	free(blob_meta);

	return DS_RESULT_SUCCESS;
}


dataswarm_result_t dataswarm_blob_put(struct dataswarm_worker *w, const char *blobid, struct link *l)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *blob_data = string_format("%s/blob/rw/%s/data", w->workspace, blobid);

	char line[32];

	// XXX should set timeout more appropriately
	time_t stoptime = time(0) + 3600;

	if(!link_readline(l, line, sizeof(line), stoptime)) {
		debug(D_DATASWARM, "couldn't read file length: %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	int64_t length = atoll(line);
	// XXX should here check for available space
	// return dataswarm_message_state_response("internal-failure", "no space available");
	//
	// XXX should handle directory transfers.

	FILE *file = fopen(blob_data, "w");
	if(!file) {
		debug(D_DATASWARM, "couldn't open %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	int bytes_transfered = link_stream_to_file(l, file, length, stoptime);
	fclose(file);

	if(bytes_transfered != length) {
		debug(D_DATASWARM, "couldn't stream to %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	free(blob_data);
	return DS_RESULT_SUCCESS;
}


dataswarm_result_t dataswarm_blob_get(struct dataswarm_worker *w, const char *blobid, struct link *l)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *blob_data = string_format("%s/blob/rw/%s/data", w->workspace, blobid);

	struct stat info;
	int status = stat(blob_data, &info);
	if(status<0) {
		debug(D_DATASWARM, "couldn't stat blob: %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	FILE *file = fopen(blob_data, "r");
	if(!file) {
		debug(D_DATASWARM, "couldn't open %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	int64_t length = info.st_size;
	char *line = string_format("%lld\n", (long long) length);

	// XXX should set timeout more appropriately
	time_t stoptime = time(0) + 3600;
	link_write(l, line, strlen(line), stoptime);
	free(line);

	// XXX should handle directory transfers.

	int bytes_transfered = link_stream_from_file(l, file, length, stoptime);
	fclose(file);

	if(bytes_transfered != length) {
		debug(D_DATASWARM, "couldn't stream from %s: %s", blob_data, strerror(errno));
		return DS_RESULT_UNABLE;
	}

	free(blob_data);
	return DS_RESULT_SUCCESS;
}


/*
dataswarm_blob_commit converts a read-write blob into
a read-only blob, fixing its size and properties for all time,
allowing the object to be duplicated to other nodes.
*/

dataswarm_result_t dataswarm_blob_commit(struct dataswarm_worker *w, const char *blobid)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *ro_name = string_format("%s/blob/ro/%s", w->workspace, blobid);
	char *rw_name = string_format("%s/blob/rw/%s", w->workspace, blobid);

	int status = rename(rw_name, ro_name);
	free(ro_name);
	free(rw_name);

	if(status == 0) {
		return DS_RESULT_SUCCESS;
	} else {
		debug(D_DATASWARM, "couldn't commit %s: %s", blobid, strerror(errno));
		return DS_RESULT_UNABLE;
	}
}

/*
dataswarm_blob_delete moves the blob to the deleting
dir, and then also deletes the object synchronously.  This ensures
that the delete (logically) occurs atomically, so that if the delete
fails or the worker crashes, all deleted blobs can be cleaned up on restart.
*/


dataswarm_result_t dataswarm_blob_delete(struct dataswarm_worker *w, const char *blobid)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *ro_name = string_format("%s/blob/ro/%s", w->workspace, blobid);
	char *rw_name = string_format("%s/blob/rw/%s", w->workspace, blobid);
	char *deleting_name = string_format("%s/blob/deleting/%s", w->workspace,blobid);

	dataswarm_result_t result = DS_RESULT_SUCCESS;

	int status = rename(ro_name, deleting_name);
	if(status!=0) {
		status = rename(rw_name,deleting_name);
		if(status!=0) {
			if(errno==ENOENT) {
				// Never existed, so just fall through.
				result = DS_RESULT_SUCCESS;
			} else {
				debug(D_DATASWARM, "couldn't delete blob %s: %s", blobid, strerror(errno));
				result = DS_RESULT_UNABLE;
			}
		}
	}

	delete_dir(deleting_name);

	free(ro_name);
	free(rw_name);
	free(deleting_name);

	return result;
}


/*
dataswarm_blob_copy message requests a blob to be duplicated. The new copy is
read-write with a new blob-id.
*/

dataswarm_result_t dataswarm_blob_copy(struct dataswarm_worker *w, const char *blobid, const char *blobid_src)
{
	if(!blobid || !blobid_src) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	/* XXX do the copying */

	return DS_RESULT_SUCCESS;
}

/*
Delete all the stale objects currently in the deleting directory.
*/

void dataswarm_blob_purge( struct dataswarm_worker *w )
{
	char *dirname = string_format("%s/blob/deleting",w->workspace);

	debug(D_DATASWARM,"checking %s for stale blobs to delete:",dirname);

	DIR *dir = opendir(dirname);
	if(dir) {
		struct dirent *d;
		while((d=readdir(dir))) {
			if(!strcmp(d->d_name,".")) continue;
			if(!strcmp(d->d_name,"..")) continue;
			char *blobname = string_format("%s/%s",dirname,blobname);
			debug(D_DATASWARM,"deleting blob: %s",blobname);
			delete_dir(blobname);
			free(blobname);
		}
	}

	free(dirname);
}
