#include "ds_blob.h"
#include "ds_message.h"
#include "ds_blob_table.h"
#include "ds_measure.h"

#include "stringtools.h"
#include "debug.h"
#include "jx.h"
#include "unlink_recursive.h"
#include "create_dir.h"
#include "host_disk_info.h"
#include "host_memory_info.h"
#include "load_average.h"
#include "macros.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <string.h>

static void update_blob_state( struct ds_worker *w, struct ds_blob *blob, ds_blob_state_t state, int send_update_message )
{
	debug(D_DATASWARM,"blob %s %s -> %s",
	      blob->blobid,
	      ds_blob_state_string(blob->state),
	      ds_blob_state_string(state));

	blob->state = state;

	char *blob_meta = ds_worker_blob_meta(w,blob->blobid);
	ds_blob_to_file(blob,blob_meta);
	free(blob_meta);

	if(send_update_message) {
		struct jx *msg = ds_message_blob_update( blob->blobid, state );
		ds_json_send(w->manager_connection,msg);
		free(msg);
	}
}

void ds_blob_table_advance( struct ds_worker *w )
{
	struct ds_blob *blob;
	char *blobid;

	hash_table_firstkey(w->blob_table);
	while(hash_table_nextkey(w->blob_table,&blobid,(void**)&blob)) {
        if(blob->state == DS_BLOB_DELETING) {
            ds_blob_state_t result;
            if((ds_blob_table_delete(w, blobid) == DS_RESULT_SUCCESS)) {
                result = DS_BLOB_DELETED;
            } else {
                result = DS_BLOB_ERROR;
            }

            update_blob_state(w,blob,result,1);
            hash_table_remove(w->blob_table,blobid);
            ds_blob_delete(blob);
        }
    }
}


ds_result_t ds_blob_table_create(struct ds_worker *w, const char *blobid, jx_int_t size, struct jx *meta)
{
	if(!blobid || size<0) {
		return DS_RESULT_BAD_PARAMS;
	}

	if(!ds_worker_disk_avail(w,size)) {
		return DS_RESULT_TOO_FULL;
	}

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
			/* space is accounted for on creation before data arrives */
			ds_worker_disk_alloc(w,size);
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
		return DS_RESULT_NO_SUCH_BLOBID;
	} else if(b->state!=DS_BLOB_RW) {
        debug(D_DATASWARM, "blob %s expected state %s, but got %s", blobid, ds_blob_state_string(b->state), ds_blob_state_string(DS_BLOB_RW));
		return DS_RESULT_BAD_STATE;
	}

	// XXX reject a put if the data stream is larger than the allocated size

	char *blob_data = ds_worker_blob_data(w,blobid);

	// XXX should here check for available space
	// return ds_message_state_response("internal-failure", "no space available");

	// XXX should handle directory transfers.

	int file = open(blob_data, O_WRONLY|O_CREAT|O_EXCL, 0777);
	if(file < 0) {
		debug(D_DATASWARM, "couldn't open %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	mq_store_fd(w->manager_connection, file, 0);
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
		return DS_RESULT_NO_SUCH_BLOBID;
	} else if(b->state!=DS_BLOB_RW && b->state!=DS_BLOB_RO) {
        debug(D_DATASWARM, "cannot get blob %s in state %s", blobid, ds_blob_state_string(b->state));
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

	int file = open(blob_data, O_RDONLY);
	if(file < 0) {
		debug(D_DATASWARM, "couldn't open %s: %s", blob_data, strerror(errno));
		free(blob_data);
		return DS_RESULT_UNABLE;
	}

	//Here we construct the response and then send the file.
	*should_respond = 0;
	struct jx *response = ds_message_response(msgid, DS_RESULT_SUCCESS, NULL);
	ds_json_send(w->manager_connection, response);

	jx_delete(response);

	// XXX should handle directory transfers.

	ds_fd_send(w->manager_connection, file, 0);

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

			char *blob_data = ds_worker_blob_data(w,blobid);

			// Measure the actual size of the committed object.  (Could be slow)
			int64_t newsize = ds_measure(blob_data);
			int64_t difference = newsize-b->size;
			debug(D_DATASWARM,"blob %s measured %lld MB (change of %lld MB)",blobid,(long long)newsize/MEGA,(long long)difference/MEGA);

			// Update the storage allocation based on actual size
			ds_worker_disk_alloc(w,difference);
			b->size = newsize;

			// Now store the new metadata in the filesystem.
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
			debug(D_DATASWARM, "couldn't commit blob-id %s because it is in state %d", blobid, b->state);
			result = DS_RESULT_BAD_STATE;
		}
	} else {
		result = DS_RESULT_NO_SUCH_BLOBID;
	}

	return result;
}


/* ds_blob_table_deleting updates the metadata to the 'deleting'
state, and then starts to delete the actual data.  Since deleting actual data
may take this could potentially take a long time, if the worker dies before
completing the delete, the state is known and the blob will be deleted in
ds_blob_table_recover. */

ds_result_t ds_blob_table_deleting(struct ds_worker *w, const char *blobid)
{
	if(!blobid) return DS_RESULT_BAD_PARAMS;

	struct ds_blob *b = hash_table_lookup(w->blob_table,blobid);
	char *blob_meta = ds_worker_blob_meta(w,blobid);

	if(!b) return DS_RESULT_NO_SUCH_BLOBID;

	// Record the deleting state in the metadata
	b->state = DS_BLOB_DELETING;
	ds_blob_to_file(b,blob_meta);

    return DS_RESULT_SUCCESS;
}

ds_result_t ds_blob_table_delete(struct ds_worker *w, const char *blobid)
{
	if(!blobid) return DS_RESULT_BAD_PARAMS;

	struct ds_blob *b = hash_table_lookup(w->blob_table,blobid);
	if(!b) return DS_RESULT_NO_SUCH_BLOBID;

    if(b->state != DS_BLOB_DELETING) {
        ds_blob_table_deleting(w, blobid);
    }

	char *blob_dir = ds_worker_blob_dir(w,blobid);
	char *blob_meta = ds_worker_blob_meta(w,blobid);
	char *blob_data = ds_worker_blob_data(w,blobid);

	// First delete the data which may take some time.
	unlink_recursive(blob_data);

	// Then delete the containing directory, which should be quick
	unlink_recursive(blob_dir);

	// Account for space only after the whole object is deleted
	ds_worker_disk_free(w,b->size);

	free(blob_dir);
	free(blob_meta);
	free(blob_data);

    //Freeing of blob structure is done on a succesful delete in ds_blob_table_advance
	// Now free up the data structures.

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

	/* XXX account for for duplicate storage use. */

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
	int64_t total_blob_size = 0;

	debug(D_DATASWARM,"checking %s for blobs to recover...",blob_dir);

	dir = opendir(blob_dir);
	if(!dir) return;

	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;

		char *blob_meta;
		struct ds_blob *b;

		debug(D_DATASWARM,"recovering blob %s",d->d_name);

		blob_meta = ds_worker_blob_meta(w,d->d_name);

		b = ds_blob_create_from_file(blob_meta);
		if(b) {
			total_blob_size += b->size;
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

	/*
	This is a little strange because the initial disk available measurement
	captures the available space, but doesn't know about the space already
	consumed, so add that into the total.
	*/

	w->resources_total->disk += total_blob_size;
	w->resources_inuse->disk = total_blob_size;

	debug(D_DATASWARM,"%d blobs, %lld MB inuse, %lld MB avail, %lld MB total",
		hash_table_size(w->blob_table),
		(long long) w->resources_inuse->disk/MEGA,
		(long long) (w->resources_total->disk-w->resources_inuse->disk)/MEGA,
		(long long) w->resources_total->disk/MEGA
	);

}
