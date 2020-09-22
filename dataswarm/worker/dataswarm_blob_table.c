
#include "dataswarm_blob_table.h"
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

typedef enum {
	DATASWARM_BLOB_RW,
	DATASWARM_BLOB_RO,
	DATASWARM_BLOB_DELETING,
	DATASWARM_BLOB_DELETED
} dataswarm_blob_state_t;

struct dataswarm_blob {
	char *blobid;
	dataswarm_blob_state_t state;
	int64_t size;
	struct jx *meta;
};

struct dataswarm_blob * dataswarm_blob_create( const char *blobid, jx_int_t size, struct jx *meta)
{
	struct dataswarm_blob *b = malloc(sizeof(*b));
	memset(b,0,sizeof(*b));
	b->blobid = strdup(blobid);
	b->state = DATASWARM_BLOB_RW;
	b->size = size;
	b->meta = meta;
	return b;
}

void dataswarm_blob_delete( struct dataswarm_blob *b )
{
	if(!b) return;
	if(b->meta) jx_delete(b->meta);
	if(b->blobid) free(b->blobid);
	free(b);
}

struct dataswarm_blob * dataswarm_blob_create_from_jx( struct jx *jblob )
{
	struct dataswarm_blob *b = malloc(sizeof(*b));
	memset(b,0,sizeof(*b));
	b->blobid = jx_lookup_string_dup(jblob,"blobid");
	b->state = jx_lookup_integer(jblob,"state");
	b->size = jx_lookup_integer(jblob,"size");
	b->meta = jx_lookup(jblob,"meta");
	if(b->meta) b->meta = jx_copy(b->meta);
	return b;
}

struct dataswarm_blob * dataswarm_blob_create_from_file( const char *filename )
{
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct jx *jblob = jx_parse_stream(file);
	if(!jblob) {
		fclose(file);
		return 0;
	}

	struct dataswarm_blob *b = dataswarm_blob_create_from_jx(jblob);

	jx_delete(jblob);
	fclose(file);

	return b;
}


struct jx * dataswarm_blob_to_jx( struct dataswarm_blob *b )
{
	struct jx *jblob = jx_object(0);
	jx_insert_string(jblob,"blobid",b->blobid);
	jx_insert_integer(jblob,"state",b->state);
	jx_insert_integer(jblob,"size",b->size);
	if(b->meta) jx_insert(jblob,jx_string("meta"),jx_copy(b->meta));
	return jblob;
}

int dataswarm_blob_to_file( struct dataswarm_blob *b, const char *filename )
{
	struct jx *jblob = dataswarm_blob_to_jx(b);
	if(!jblob) return 0;

	FILE *file = fopen(filename,"w");
	if(!file) {
		jx_delete(jblob);
		return 0;
	}

	jx_print_stream(jblob,file);

	jx_delete(jblob);
	fclose(file);

	return 1;
}

dataswarm_result_t dataswarm_blob_table_create(struct dataswarm_worker *w, const char *blobid, jx_int_t size, struct jx *meta )
{
	if(!blobid || size < 1) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}
	// XXX should here check for available space

	char *blob_dir = string_format("%s/blob/%s", w->workspace, blobid);
	char *blob_meta = string_format("%s/meta", blob_dir);

	dataswarm_result_t result = DS_RESULT_SUCCESS;

	struct dataswarm_blob *b = dataswarm_blob_create(blobid,size,meta);

	if(mkdir(blob_dir, 0777)==0) {
		if(dataswarm_blob_to_file(b,blob_meta)) {
			result = DS_RESULT_SUCCESS;
		} else {
			debug(D_DATASWARM, "couldn't write %s: %s", blob_meta, strerror(errno));
			result = DS_RESULT_UNABLE;
		}
	} else {
		debug(D_DATASWARM, "couldn't mkdir %s: %s", blob_dir, strerror(errno));
		result = DS_RESULT_UNABLE;
	}

	dataswarm_blob_delete(b);

	free(blob_dir);
	free(blob_meta);

	return result;
}


dataswarm_result_t dataswarm_blob_table_put(struct dataswarm_worker *w, const char *blobid, struct link *l)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *blob_data = string_format("%s/blob/%s/data", w->workspace, blobid);

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

    debug(D_DATASWARM, "finished putting %" PRId64 " bytes into %s", length, blob_data);

	return DS_RESULT_SUCCESS;
}


dataswarm_result_t dataswarm_blob_table_get(struct dataswarm_worker *w, const char *blobid, struct link *l)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *blob_data = string_format("%s/blob/%s/data", w->workspace, blobid);

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
	} else {
        debug(D_DATASWARM, "finished reading %" PRId64 " bytes from %s", length, blob_data);
    }

	free(blob_data);
	return DS_RESULT_SUCCESS;
}


/*
dataswarm_blob_table_commit converts a read-write blob into
a read-only blob, fixing its size and properties for all time,
allowing the object to be duplicated to other nodes.
*/

dataswarm_result_t dataswarm_blob_table_commit(struct dataswarm_worker *w, const char *blobid)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *blob_meta = string_format("%s/blob/%s/meta", w->workspace, blobid);
	dataswarm_result_t result = DS_RESULT_UNABLE;

	struct dataswarm_blob *b = dataswarm_blob_create_from_file(blob_meta);
	if(b) {
		if(b->state==DATASWARM_BLOB_RW) {
			b->state = DATASWARM_BLOB_RO;
			// XXX need to measure,checksum,update here
			if(dataswarm_blob_to_file(b,blob_meta)) {
				result = DS_RESULT_SUCCESS;
			} else {
				debug(D_DATASWARM,"couldn't write %s: %s",blob_meta,strerror(errno));
				result = DS_RESULT_UNABLE;
			}
		} else if(b->state==DATASWARM_BLOB_RO) {
			// Already committed, not an error.
			result = DS_RESULT_SUCCESS;
		} else {
			debug(D_DATASWARM,"couldn't commit blobid %s because it is in state %d",blobid,b->state);
			result = DS_RESULT_UNABLE;
		}

		dataswarm_blob_delete(b);
	} else {
		debug(D_DATASWARM,"couldn't read %s: %s",blob_meta,strerror(errno));
		result = DS_RESULT_UNABLE;
	}

	return result;
}

/*
dataswarm_blob_table_delete moves the blob to the deleting
dir, and then also deletes the object synchronously.  This ensures
that the delete (logically) occurs atomically, so that if the delete
fails or the worker crashes, all deleted blobs can be cleaned up on restart.
*/

dataswarm_result_t dataswarm_blob_table_delete(struct dataswarm_worker *w, const char *blobid)
{
	if(!blobid) {
		// XXX return obj with incorrect parameters
		return DS_RESULT_BAD_PARAMS;
	}

	char *blob_dir = string_format("%s/blob/%s", w->workspace, blobid);
	char *deleting_name = string_format("%s/blob/deleting/%s", w->workspace,blobid);

	dataswarm_result_t result = DS_RESULT_SUCCESS;

	int status = rename(blob_dir,deleting_name);
	if(status!=0) {
		if(errno==ENOENT || errno==EEXIST) {
			// If it was never there, that's a success.
			// Also if already moved to deleted, that's ok too.
			result = DS_RESULT_SUCCESS;
		} else {
			debug(D_DATASWARM, "couldn't delete blob %s: %s", blobid, strerror(errno));
			result = DS_RESULT_UNABLE;
		}
	}

	delete_dir(deleting_name);

	free(blob_dir);
	free(deleting_name);

	return result;
}


/*
dataswarm_blob_table_copy message requests a blob to be duplicated. The new copy is
read-write with a new blob-id.
*/

dataswarm_result_t dataswarm_blob_table_copy(struct dataswarm_worker *w, const char *blobid, const char *blobid_src)
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

void dataswarm_blob_table_purge( struct dataswarm_worker *w )
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
