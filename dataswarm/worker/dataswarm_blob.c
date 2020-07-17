
#include "dataswarm_blob.h"

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

int dataswarm_blob_create( const char *blobid, struct jx *metadata )
{
	int result = 0;

	char *blob_dir = string_format("rw/%s",blobid);
	char *blob_meta = string_format("rw/%s/meta",blob_dir);

	if(mkdir(blob_dir,0777)==0) {
		FILE *file = fopen(blob_meta,"w");
		if(file) {
			jx_print_stream(metadata,file);
			fclose(file);
			result = 1;
		} else {
			debug(D_DATASWARM,"couldn't open %s: %s",blob_meta,strerror(errno));
		}
	} else {
		debug(D_DATASWARM,"couldn't mkdir %s: %s",blob_dir,strerror(errno));
	}

	free(blob_dir);
	free(blob_meta);

	return result;
}

int dataswarm_blob_upload( const char *blobid, struct link *l )
{
	int result = 0;

	char *blob_dir = string_format("rw/%s",blobid);
	char *blob_data = string_format("rw/%s/data",blob_dir);

	char line[32];

	// XXX should set timeout more appropriately
	time_t stoptime = time(0) + 3600;

	if(link_readline(l,line,sizeof(line),stoptime)) {
		int64_t length = atoll(line);

		// XXX should here check for available space

		// XXX should handle directory transfers.

		FILE *file = fopen(blob_data,"w");
		if(file) {
			if(link_stream_to_file(l,file,length,stoptime)==length) {
				result = 1;
			} else {
				debug(D_DATASWARM,"couldn't stream to %s: %s",blob_data,strerror(errno));
			}
			fclose(file);
		} else {
			debug(D_DATASWARM,"couldn't open %s: %s",blob_data,strerror(errno));
		}
	} else {
		debug(D_DATASWARM,"couldn't read file length: %s: %s",blob_dir,strerror(errno));
	}

	free(blob_dir);
	free(blob_data);

	return result;
}

int dataswarm_blob_download( const char *blobid, struct link *l )
{
	int result = 0;

	char *blob_dir = string_format("rw/%s",blobid);
	char *blob_data = string_format("rw/%s/data",blob_dir);

	struct stat info;
	if(stat(blob_data,&info)==0) {

		int64_t length = info.st_size;
		char *line = string_format("%lld\n",(long long)length);

		// XXX should set timeout more appropriately
		time_t stoptime = time(0) + 3600;

		link_write(l,line,strlen(line),stoptime);

		// XXX should handle directory transfers.

		FILE *file = fopen(blob_data,"r");
		if(file) {
			if(link_stream_from_file(l,file,length,stoptime)==length) {
				result = 1;
			} else {
				debug(D_DATASWARM,"couldn't stream from %s: %s",blob_data,strerror(errno));
			}
			fclose(file);
		} else {
			debug(D_DATASWARM,"couldn't open %s: %s",blob_data,strerror(errno));
		}

		free(line);
	} else {
		debug(D_DATASWARM,"couldn't stat blob: %s: %s",blob_data,strerror(errno));
	}

	free(blob_dir);
	free(blob_data);

	return result;
}

/*
dataswarm_blob_commit converts a read-write blob into
a read-only blob, fixing its size and properties for all time,
allowing the object to be duplicated to other nodes.
*/

int dataswarm_blob_commit( const char *blobid )
{
	int result;

	char *ro_name = string_format("ro/%s",blobid);
	char *rw_name = string_format("rw/%s",blobid);

	if(rename(ro_name,rw_name)==0) {
		result = 1;
	} else {
		debug(D_DATASWARM,"couldn't commit %s: %s",blobid,strerror(errno));
	}

	// XXX need to measure the actual size of the object and add to metadata.

	free(ro_name);
	free(rw_name);

	return result;
}

/*
dataswarm_blob_delete moves the blob to the deleting
dir, and then also deletes the object synchronously.  This ensures
that the delete (logically) occurs atomically, so that if the delete
fails or the worker crashes, all deleted blobs can be cleaned up on restart.
*/

int dataswarm_blob_delete( const char *blobid )
{
	int result;

	char *ro_name = string_format("ro/%s",blobid);
	char *rw_name = string_format("rw/%s",blobid);
	char *deleting_name = string_format("deleting/%s",blobid);

	if(rename(ro_name,deleting_name)==0 || rename(rw_name,deleting_name)==0) {
		result = 1;
		delete_dir(deleting_name);
	} else {
		debug(D_DATASWARM,"couldn't delete %s: %s",blobid,strerror(errno));
	}

	free(ro_name);
	free(rw_name);
	free(deleting_name);

	return result;
}


