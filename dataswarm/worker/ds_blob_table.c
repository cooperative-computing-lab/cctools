#include "common/ds_blob.h"
#include "common/ds_message.h"
#include "ds_blob_table.h"

#include "stringtools.h"
#include "debug.h"
#include "jx.h"
#include "delete_dir.h"
#include "create_dir.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

ds_result_t ds_blob_table_create(struct ds_worker *w, const char *blobid, jx_int_t size, struct jx *meta)
{
	if(!blobid || size < 1) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}
	// XXX should here check for available space

	if(hash_table_lookup(w->blob_table,blobid)) {
		return DS_RESULT_BLOBID_EXISTS;
	}

	char *blob_dir = ds_worker_blob_dir(w,blobid);
	char *blob_meta = ds_worker_blob_meta(w,blobid);

	ds_result_t result = DS_RESULT_SUCCESS;

	struct ds_blob *b = ds_blob_create(blobid, size, meta);

	if(mkdir(blob_dir, 0777) == 0) {
		if(ds_blob_to_file(b, blob_meta)) {
			hash_table_insert(w->blob_table,blobid,b);
			result = DS_RESULT_SUCCESS;
		} else {
			debug(D_DATASWARM, "couldn't write %s: %s", blob_meta, strerror(errno));
			result = DS_RESULT_UNABLE;
			ds_blob_delete(b);
		}
	} else {
		debug(D_DATASWARM, "couldn't mkdir %s: %s", blob_dir, strerror(errno));
		result = DS_RESULT_UNABLE;
		ds_blob_delete(b);
	}

	free(blob_dir);
	free(blob_meta);

	return result;
}


ds_result_t ds_blob_table_put(struct ds_worker * w, const char *blobid)
{
	if(!blobid) {
		return DS_RESULT_BAD_PARAMS;
	}

	struct ds_blob *b = hash_table_lookup(w->blob_table,blobid);
	if(!b) {
		return DS_RESULT_BAD_ID;
	} else if(b->state!=DS_BLOB_RW) {
		return DS_RESULT_BAD_STATE;
	}

	char *blob_data = ds_worker_blob_data(w,blobid);

	char line[32];

	// XXX should set timeout more appropriately
	time_t stoptime = time(0) + 3600;

	if(!link_readline(w->manager_link, line, sizeof(line), stoptime)) {
		debug(D_DATASWARM, "couldn't read file length: %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	int64_t length = atoll(line);
	// XXX should here check for available space
	// return ds_message_state_response("internal-failure", "no space available");
	//
	// XXX should handle directory transfers.

	FILE *file = fopen(blob_data, "w");
	if(!file) {
		debug(D_DATASWARM, "couldn't open %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	int bytes_transfered = link_stream_to_file(w->manager_link, file, length, stoptime);
	fclose(file);

	if(bytes_transfered != length) {
		debug(D_DATASWARM, "couldn't stream to %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	debug(D_DATASWARM, "finished putting %" PRId64 " bytes into %s", length, blob_data);

	free(blob_data);

	return DS_RESULT_SUCCESS;
}



ds_result_t ds_blob_table_get(struct ds_worker * w, const char *blobid, jx_int_t msgid, int *should_respond)
{

	*should_respond = 1;
	if(!blobid) {
		return DS_RESULT_BAD_PARAMS;
	}

	struct ds_blob *b = hash_table_lookup(w->blob_table,blobid);
	if(!b) {
		return DS_RESULT_BAD_ID;
	} else if(b->state!=DS_BLOB_RW && b->state!=DS_BLOB_RO) {
		return DS_RESULT_BAD_STATE;
	}

	char *blob_data = ds_worker_blob_data(w,blobid);

	struct stat info;
	int status = stat(blob_data, &info);
	if(status < 0) {
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
	//Here we construct the response and then send the file.
	*should_respond = 0;
	struct jx *response = ds_message_standard_response(msgid, DS_RESULT_SUCCESS, NULL);
	ds_json_send(w->manager_link, response, w->long_timeout);
	jx_delete(response);

	int64_t length = info.st_size;
	char *line = string_format("%lld\n", (long long) length);

	// XXX should set timeout more appropriately
	time_t stoptime = time(0) + 3600;
	link_write(w->manager_link, line, strlen(line), stoptime);
	free(line);

	// XXX should handle directory transfers.

	int bytes_transfered = link_stream_from_file(w->manager_link, file, length, stoptime);
	fclose(file);

	if(bytes_transfered != length) {
		debug(D_DATASWARM, "couldn't stream from %s: %s", blob_data, strerror(errno));
	} else {
		debug(D_DATASWARM, "finished reading %" PRId64 " bytes from %s", length, blob_data);
	}

	free(blob_data);
	return DS_RESULT_SUCCESS;
}


/*
ds_blob_table_commit converts a read-write blob into
a read-only blob, fixing its size and properties for all time,
allowing the object to be duplicated to other nodes.
*/

ds_result_t ds_blob_table_commit(struct ds_worker * w, const char *blobid)
{
	if(!blobid) {
		return DS_RESULT_BAD_PARAMS;
	}

	ds_result_t result = DS_RESULT_UNABLE;

	struct ds_blob *b = hash_table_lookup(w->blob_table,blobid);
	if(b) {
		if(b->state == DS_BLOB_RW) {
			b->state = DS_BLOB_RO;
			// XXX need to measure,checksum,update here
			char *blob_meta = ds_worker_blob_meta(w,blobid);
			if(ds_blob_to_file(b, blob_meta)) {
				result = DS_RESULT_SUCCESS;
			} else {
				debug(D_DATASWARM, "couldn't write %s: %s", blob_meta, strerror(errno));
				result = DS_RESULT_UNABLE;
			}
			free(blob_meta);
		} else if(b->state == DS_BLOB_RO) {
			// Already committed, not an error.
			result = DS_RESULT_SUCCESS;
		} else {
			debug(D_DATASWARM, "couldn't commit blobid %s because it is in state %d", blobid, b->state);
			result = DS_RESULT_BAD_STATE;
		}
	} else {
		result = DS_RESULT_BAD_ID;
	}

	return result;
}

/*
ds_blob_table_delete updates the metadata to the 'deleting'
state, and then starts to delete the actual data.  Note that
this could potentially take a long time, so if the worker
dies before completing the delete, the state is known and
the blob will be deleted in ds_blob_table_recover.
*/

ds_result_t ds_blob_table_delete(struct ds_worker * w, const char *blobid)
{
	if(!blobid) return DS_RESULT_BAD_PARAMS;

	struct ds_blob *b = hash_table_lookup(w->blob_table,blobid);
	if(!b) return DS_RESULT_BAD_ID;

	char *blob_dir = ds_worker_blob_dir(w,blobid);
	char *blob_meta = ds_worker_blob_meta(w,blobid);
	char *blob_data = ds_worker_blob_data(w,blobid);

	// Record the deleting state in the metadata
	b->state = DS_BLOB_DELETING;
	ds_blob_to_file(b,blob_meta);

	// First delete the data which may take some time.
	delete_dir(blob_data);

	// Then delete the containing directory, which should be quick
	delete_dir(blob_dir);

	// Now free up the data structures.
	hash_table_remove(w->blob_table,blobid);
	ds_blob_delete(b);

	free(blob_dir);
	free(blob_meta);
	free(blob_data);

	return DS_RESULT_SUCCESS;
}


/*
ds_blob_table_copy message requests a blob to be duplicated. The new copy is
read-write with a new blob-id.
*/

ds_result_t ds_blob_table_copy(struct ds_worker * w, const char *blobid, const char *blobid_src)
{
	if(!blobid || !blobid_src) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	/* XXX do the copying */

	return DS_RESULT_UNABLE;
}

ds_result_t ds_blob_table_list( struct ds_worker *w, struct jx **result )
{
	struct ds_blob *blob;
	char *blobid;

	*result = jx_object(0);

	hash_table_firstkey(w->blob_table);
	while(hash_table_nextkey(w->blob_table,&blobid,(void**)&blob)) {
		jx_insert(*result,jx_string(blobid),ds_blob_to_jx(blob));
	}

	return DS_RESULT_SUCCESS;	
}

/*
After a restart, scan the blobs on disk to recover the table,
then delete any blobs that didn't successfully delete before.
Note that we are not connected to the master at this point,
so do not send any message.  A complete set uf updates gets sent
when we reconnect.
*/

void ds_blob_table_recover( struct ds_worker *w )
{
	char * blob_dir = string_format("%s/blob",w->workspace);

	DIR *dir;
	struct dirent *d;

	debug(D_DATASWARM,"checking %s for blobs to recover...",blob_dir);

	dir = opendir(blob_dir);
	if(!dir) return;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;
		if(!strcmp(d->d_name,"deleting")) continue;

		char *blob_meta;
		struct ds_blob *b;

		debug(D_DATASWARM,"recovering blob %s",d->d_name);

		blob_meta = ds_worker_blob_meta(w,d->d_name);

		b = ds_blob_create_from_file(blob_meta);
		if(b) {
			hash_table_insert(w->blob_table,b->blobid,b);
			if(b->state==DS_BLOB_DELETING) {
				debug(D_DATASWARM, "deleting blob %s",b->blobid);
				ds_blob_table_delete(w,b->blobid);
			}
		}
		free(blob_meta);

	}

	debug(D_DATASWARM,"done recovering blobs");
	closedir(dir);
	free(blob_dir);
}

