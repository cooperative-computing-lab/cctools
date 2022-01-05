/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_stream.h"

#include "jx.h"
#include "jx_parse.h"
#include "nvpair.h"
#include "nvpair_jx.h"

#include <string.h>
#include <stdio.h>

#define LOG_LINE_MAX 65536

static void corrupt_data( FILE *stream )
{
	fprintf(stderr,"deltadb: corrupt data in stream: ");
	int c;
	do {
		c = fgetc(stream);
		fprintf(stderr,"%c",c);
	} while( c!=EOF && c!='\n');
}

int deltadb_process_stream( struct deltadb_query *query, struct deltadb_event_handlers *handlers, FILE *stream, time_t starttime, time_t stoptime )
{
	char value[LOG_LINE_MAX];
	char name[LOG_LINE_MAX];
	char key[LOG_LINE_MAX];
	int n;
	struct jx *jvalue;

	long long current = 0;

	while(1) {
		int command = fgetc(stream);
		if(command=='C') {
			n = fscanf(stream,"%s %[^\n]\n",key,value);
			if(n==1) {
				/* backwards compatibility with old log format */
				struct nvpair *nv = nvpair_create();
				nvpair_parse_stream(nv,stream);
				jvalue = nvpair_to_jx(nv);
				nvpair_delete(nv);
			} else if(n==2) {
				jvalue = jx_parse_string(value);
				if(!jvalue) jvalue = jx_string(value);
			} else {
				corrupt_data(stream);
				continue;
			}

			if(!handlers->deltadb_create_event(query,key,jvalue)) break;

		} else if(command=='D') {
			n = fscanf(stream,"%s\n",key);
			if(n!=1) {
				corrupt_data(stream);
				continue;
			}

			if(!handlers->deltadb_delete_event(query,key)) break;

		} else if(command=='M') {
			n = fscanf(stream,"M %s %[^\n]\n",key,value);
			if(n==2) {
				jvalue = jx_parse_string(value);
				if(!jvalue) {
					corrupt_data(stream);
					continue;
				}
			} else {
				corrupt_data(stream);
				continue;
			}

			if(!handlers->deltadb_merge_event(query,key,jvalue)) break;

		} else if(command=='U') {
			n=fscanf(stream,"%s %s %[^\n]\n",key,name,value);
			if(n!=3) {
				corrupt_data(stream);
				continue;
			}

			jvalue = jx_parse_string(value);
			if(!jvalue) {
				/* backwards compatibility with old format */
				jvalue = jx_string(value);
				continue;
			}

			if(!handlers->deltadb_update_event(query,key,name,jvalue)) break;

		} else if(command=='R') {
			n=fscanf(stream,"%s %s\n",key,name);
			if(n!=2) {
				corrupt_data(stream);
				continue;
			}

			if(!handlers->deltadb_remove_event(query,key,name)) break;

		} else if(command=='T') {
			n = fscanf(stream,"%lld\n",&current);
			if(n!=1) {
				corrupt_data(stream);
				continue;
			}

			if(!handlers->deltadb_time_event(query,starttime,stoptime,current)) break;

			if(stoptime && current>stoptime) return 0;

		} else if(command=='t') {
			long long change;
			n = fscanf(stream,"%lld\n",&change);
			if(n!=1) {
				corrupt_data(stream);
				continue;
			}

			current += change;

			if(!handlers->deltadb_time_event(query,starttime,stoptime,current)) break;

			if(stoptime && current>stoptime) return 0;

		} else if(command=='\n') {
			continue;
		} else if(command==EOF) {
			break;
		} else {
			corrupt_data(stream);
		}
	}

	return 1;
}

int deltadb_process_stream_fast( struct deltadb_query *query, struct deltadb_event_handlers *handlers, FILE *stream, time_t starttime, time_t stoptime )
{
	char line[LOG_LINE_MAX];
	int n;

	long long current = 0;

	while(fgets(line,sizeof(line),stream)) {
		if(line[0]=='T') {
			n = sscanf(line,"T %lld",&current);
			if(n!=1) {
				corrupt_data(stream);
				continue;
			}
			if(stoptime && current>stoptime) return 0;

		} else if(line[0]=='t') {
			long long change;
			n = sscanf(line,"t %lld",&change);
			if(n!=1) {
				corrupt_data(stream);
				continue;
			}

			current += change;

			if(stoptime && current>stoptime) return 0;
		} else {
			/* any other type, just pass the line through */
		}

		if(!handlers->deltadb_raw_event(query,line)) break;
	}

	return 1;
}
