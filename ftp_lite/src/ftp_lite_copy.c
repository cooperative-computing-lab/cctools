/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "ftp_lite.h"

#include "copy_stream.h"
#include "debug.h"
#include "getopt.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

void show_use(char *name)
{
	fprintf(stderr,"use: %s [options]\n",name);
	fprintf(stderr,"where options are:\n");
	fprintf(stderr," -S <host>  Source host. (default=localhost)\n");
	fprintf(stderr," -s <file>  Source file. (default=stdin)\n");
	fprintf(stderr," -T <host>  Target host. (default=localhost)\n");
	fprintf(stderr," -t <file>  Target file. (default=stdout)\n");
	fprintf(stderr," -P <port>  Source port.\n");
	fprintf(stderr," -p <port>  Target port.\n");
	fprintf(stderr," -d         Debug mode.\n");
	fprintf(stderr," -h         Show help.\n");
	fprintf(stderr,"\n");
}


int main( int argc, char *argv[] )
{
	struct ftp_lite_server *source=0, *target=0;
	char *source_host=0, *source_file=0;
	char *target_host=0, *target_file=0;
	FILE *source_fp=0, *target_fp=0, *data=0;
	signed char c;
	int source_port=0, target_port=0;

	debug_config(argv[0]);

	while((c=getopt(argc,argv,"S:s:T:t:P:p:dh")) > -1) {
		switch(c) {
			case 'S':
				source_host = optarg;
				break;
			case 's':
				source_file = optarg;
				break;
			case 'T':
				target_host = optarg;
				break;
			case 't':
				target_file = optarg;
				break;
			case 'P':
				source_port = atoi(optarg);
				break;
			case 'p':
				target_port = atoi(optarg);
				break;
			case 'h':
				show_use(argv[0]);
				return 0;
			case 'd':
				debug_flags_set("ftp");
				break;
		}
	}

	if(source_host&&!source_file) {
		fprintf(stderr," -S requires -s \n");
		show_use(argv[0]);
		return 1;
	}

	if(target_host&&!target_file) {
		fprintf(stderr," -T requires -t \n");
		show_use(argv[0]);
		return 1;
	}

	if(!source_host) {
		if(source_file) {
			source_fp = fopen(source_file,"rb");
			if(!source_fp) {
				fprintf(stderr,"%s: couldn't open %s: %s\n",argv[0],source_file,strerror(errno));
				return 1;
			}
		} else {
			source_fp = stdin;
		}
	}

	if(!target_host) {
		if(target_file) {
			target_fp = fopen(target_file,"rb+");
			if(!target_fp) {
				fprintf(stderr,"%s: couldn't open %s: %s\n",argv[0],target_file,strerror(errno));
				return 1;
			}
		} else {
			target_fp = stdout;
		}
	}

	if(source_host) {
		source = ftp_lite_open_and_auth(source_host,source_port);
		if(!source) {
			fprintf(stderr,"%s: couldn't connect to %s: %s\n",argv[0],source_host,strerror(errno));
			return 1;
		}
	}

	if(target_host) {
		target = ftp_lite_open_and_auth(target_host,target_port);
		if(!target) {
			fprintf(stderr,"%s: couldn't log in to %s: %s\n",argv[0],target_host,strerror(errno));
			return 1;
		}
	}

	if(source&&target) {
		if(!ftp_lite_third_party_transfer(source,source_file,target,target_file)) {
			fprintf(stderr,"%s: transfer failed: %s\n",argv[0],strerror(errno));
			return 1;
		}
	} else if(source) {
		data = ftp_lite_get(source,source_file,0);
		if(!data) {
			fprintf(stderr,"%s: couldn't open %s: %s\n",argv[0],source_file,strerror(errno));
			return 1;
		}

		if(copy_stream_to_stream(data,target_fp)<0) {
			fprintf(stderr,"%s: couldn't copy data: %s\n",argv[0],strerror(errno));
			return 1;
		}
		fclose(data);
		ftp_lite_done(source);
	} else if(target) {
		data = ftp_lite_put(target,target_file,0,FTP_LITE_WHOLE_FILE);
		if(!data) {
			fprintf(stderr,"%s: couldn't open %s: %s\n",argv[0],target_file,strerror(errno));
			return 1;
		}

		if(copy_stream_to_stream(source_fp,data)<0) {
			fprintf(stderr,"%s: couldn't copy data: %s\n",argv[0],strerror(errno));
			return 1;
		}

		fclose(data);
		ftp_lite_done(target);
	} else {
		if(copy_stream_to_stream(source_fp,target_fp)<0) {
			fprintf(stderr,"%s: couldn't copy data: %s\n",argv[0],strerror(errno));
			return 1;
		}
	}

	if(source) ftp_lite_close(source);
	if(target) ftp_lite_close(target);

	if( source_fp && source_file ) fclose(source_fp);
	if( target_fp && target_file ) fclose(target_fp);

	return 0;
}

/* vim: set noexpandtab tabstop=8: */
