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

static void corrupt_data( const char *filename, const char *line )
{
	fprintf(stderr,"corrupt data in %s: %s\n",filename,line);

}

int deltadb_process_stream( struct deltadb_query *query, FILE *stream, time_t starttime, time_t stoptime )
{
	char line[LOG_LINE_MAX];
	char value[LOG_LINE_MAX];
	char name[LOG_LINE_MAX];
	char key[LOG_LINE_MAX];
	int n;
	struct jx *jvalue;

	long long current = 0;

	const char *filename = "stream";

	while(fgets(line,sizeof(line),stream)) {
		if(line[0]=='C') {
			n = sscanf(line,"C %s %[^\n]",key,value);
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
				corrupt_data(filename,line);
				continue;
			}

			if(!deltadb_create_event(query,key,jvalue)) break;

		} else if(line[0]=='D') {
			n = sscanf(line,"D %s\n",key);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}

			if(!deltadb_delete_event(query,key)) break;

		} else if(line[0]=='M') {
			n = sscanf(line,"M %s %[^\n]",key,value);
			if(n==2) {
				jvalue = jx_parse_string(value);
				if(!jvalue) {
					corrupt_data(filename,line);
					continue;
				}
			} else {
				corrupt_data(filename,line);
				continue;
			}

			if(!deltadb_merge_event(query,key,jvalue)) break;

		} else if(line[0]=='U') {
			n=sscanf(line,"U %s %s %[^\n],",key,name,value);
			if(n!=3) {
				corrupt_data(filename,line);
				continue;
			}

			jvalue = jx_parse_string(value);
			if(!jvalue) {
				/* backwards compatibility with old format */
				jvalue = jx_string(value);
				continue;
			}

			if(!deltadb_update_event(query,key,name,jvalue)) break;

		} else if(line[0]=='R') {
			n=sscanf(line,"R %s %s",key,name);
			if(n!=2) {
				corrupt_data(filename,line);
				continue;
			}

			if(!deltadb_remove_event(query,key,name)) break;

		} else if(line[0]=='T') {
			n = sscanf(line,"T %lld",&current);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}

			if(!deltadb_time_event(query,starttime,stoptime,current)) break;

			if(stoptime && current>stoptime) return 0;

		} else if(line[0]=='t') {
			long long change;
			n = sscanf(line,"t %lld",&change);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}

			current += change;

			if(!deltadb_time_event(query,starttime,stoptime,current)) break;

			if(stoptime && current>stoptime) return 0;

		} else if(line[0]=='\n') {
			continue;
		} else {
			corrupt_data(filename,line);
		}

		if(!deltadb_post_event(query,line)) break;
	}

	return 1;
}
