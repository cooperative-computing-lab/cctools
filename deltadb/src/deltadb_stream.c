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
	fprintf(stderr,"corrupt data: %s\n",line);
}

int deltadb_process_stream( struct deltadb_query *query, struct deltadb_event_handlers *handlers, FILE *stream, time_t starttime, time_t stoptime )
{
	char whole_line[LOG_LINE_MAX];
	char value[LOG_LINE_MAX];
	char name[LOG_LINE_MAX];
	char key[LOG_LINE_MAX];
	int n;
	struct jx *jvalue;

	long long current = 0;

	const char *filename = "stream";

	jx_parse_set_static_mode(true);

	while(fgets(whole_line,sizeof(whole_line),stream)) {
        char *line = whole_line;

		reconsider:

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

			if(!handlers->deltadb_create_event(query,key,jvalue)) break;

		} else if(line[0]=='D') {
			n = sscanf(line,"D %s\n",key);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}

			if(!handlers->deltadb_delete_event(query,key)) break;

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

			if(!handlers->deltadb_merge_event(query,key,jvalue)) break;

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

			if(!handlers->deltadb_update_event(query,key,name,jvalue)) break;

		} else if(line[0]=='R') {
			/*
			A correct R record should just have a key and a name.
			However, due to a bug in an earlier version, R records were
			sometimes written without a trailing newline, causing the
			next record to appear immediately after on the same line.
			*/

			n=sscanf(line,"R %s %s %s",key,name,value);
			if(n==3) {
				/*
				A third field here indicates a corrupted record.
				Check to see if the final character of key is a
				valid record type.
				*/
				char type = name[strlen(name)-1];

				if(strchr("CDUMRTt",type)) {

					/* If so, remove the character and process the R record */
					name[strlen(name)-1] = 0;
					if(!handlers->deltadb_remove_event(query,key,name)) break;
					name[strlen(name)] = type;

					/* Now process the remainder of the line as a new command. */
					int position = strlen(key) + strlen(name) + 2;
                    line = (line + position);
					goto reconsider;
				} else {
					/* Invalid type: the line is totally corrupted. */
					corrupt_data(filename,line);
					continue;
				}

			} else if(n==2) {
				/* This is a correct record with just a key and value */
				if(!handlers->deltadb_remove_event(query,key,name)) break;
			} else {
				corrupt_data(filename,line);
				continue;
			}


		} else if(line[0]=='T') {
			n = sscanf(line,"T %lld",&current);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}

			if(!handlers->deltadb_time_event(query,starttime,stoptime,current)) break;

			if(stoptime && current>stoptime) goto fail;

		} else if(line[0]=='t') {
			long long change;
			n = sscanf(line,"t %lld",&change);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}

			current += change;

			if(!handlers->deltadb_time_event(query,starttime,stoptime,current)) break;

			if(stoptime && current>stoptime) goto fail;

		} else if(line[0]=='\n') {
			continue;
		} else {
			corrupt_data(filename,line);
		}
	}

	jx_parse_set_static_mode(false);
	return 1;

fail:
	jx_parse_set_static_mode(false);
	return 0;
}

int deltadb_process_stream_fast( struct deltadb_query *query, struct deltadb_event_handlers *handlers, FILE *stream, time_t starttime, time_t stoptime )
{
	char line[LOG_LINE_MAX];
	int n;
	const char *filename = "stream";

	long long current = 0;

	while(fgets(line,sizeof(line),stream)) {
		if(line[0]=='T') {
			n = sscanf(line,"T %lld",&current);
			if(n!=1) {
				corrupt_data(filename,line);
				continue;
			}
			if(stoptime && current>stoptime) return 0;

		} else if(line[0]=='t') {
			long long change;
			n = sscanf(line,"t %lld",&change);
			if(n!=1) {
				corrupt_data(filename,line);
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
