/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
A limited-use program to upgrade (still-valid) log files
in two ways that save space:
1 - Adjacent U records are combined into one M record.
2 - T records are reduced to one per minute.
*/


#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#define LOG_LINE_MAX 4096

void corrupt_data( const char *line, int lineno )
{
	fprintf(stderr,"ABORT: corrupt data line %d: %s\n",lineno,line);
	exit(1);
}

static void emit_merge( FILE * output, struct jx *merge, const char *lastkey )
{
	if(!merge) return;
	char *str = jx_print_string(merge);
	fprintf(output,"M %s %s\n",lastkey,str);
	free(str);
	jx_delete(merge);
}

int main( int argc, char *argv[] )
{
	if(argc!=3) {
		fprintf(stderr,"use: %s <infile> <outfile>\n",argv[0]);
		return 1;
	}

	FILE *input = fopen(argv[1],"r");
	if(!input) {
		fprintf(stderr,"couldn't open %s: %s\n",argv[1],strerror(errno));
		return 1;
	}

	FILE *output = fopen(argv[2],"w");
	if(!input) {
		fprintf(stderr,"couldn't open %s: %s\n",argv[2],strerror(errno));
		return 1;
	}

	char line[LOG_LINE_MAX] = "";
	char key[LOG_LINE_MAX] = "";
	char name[LOG_LINE_MAX] = "";
	char value[LOG_LINE_MAX] = "";

	char lastkey[LOG_LINE_MAX] = "";

	struct jx *merge = 0;
	int lineno = 0;
	long time;
	long lasttime = 0;
	long time_granularity = 60;

	while(fgets(line,sizeof(line),input)) {
		lineno++;

		if(line[0]=='U') {
			if(sscanf(line,"U %s %s %[^\n]",key,name,value)==3) {
				// If a merge for a different key is pending, emit it.
				if(merge && strcmp(key,lastkey) ) {
					emit_merge(output,merge,lastkey);
					merge = 0;
				}

				// Add the current update to the merge.
				if(!merge) merge = jx_object(0);
				struct jx *jvalue = jx_parse_string(value);
				jx_insert(merge,jx_string(name),jvalue);

				// Remember the current key
				strcpy(lastkey,key);
			} else {
				corrupt_data(line,lineno);
			}
		} else if(merge) {
			emit_merge(output,merge,lastkey);
			merge = 0;
		}

		if(line[0]=='T') {
			if(sscanf(line,"T %ld",&time)) {
				if((time-lasttime)>time_granularity) {
					fputs(line,output);
					lasttime = time;
				}
			} else {
				corrupt_data(line,lineno);
			}
		} else if(line[0]!='U') {
			fputs(line,output);
		}
	}

	emit_merge(output,merge,lastkey);
	
	fclose(input);
	fclose(output);

	return 0;
}

