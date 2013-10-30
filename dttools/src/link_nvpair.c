/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "link_nvpair.h"
#include "stringtools.h"
#include <stdlib.h>

struct nvpair *link_nvpair_read( struct link *l, time_t stoptime )
{
	struct nvpair *nv = nvpair_create();
	char line[65536];
	int lines = 0;

	while(link_readline(l, line, sizeof(line), stoptime)) {
		string_chomp(line);
		if(!line[0])
			break;
		nvpair_parse(nv, line);
		lines++;
	}

	if(lines) {
		return nv;
	} else {
		nvpair_delete(nv);
		return 0;
	}
}

void link_nvpair_write( struct link *l, struct nvpair *nv, time_t stoptime )
{
	char *text = 0;
	int length = nvpair_print_alloc(nv,&text);
	link_write(l,text,length,stoptime);
	link_write(l,"\n",1,stoptime);
	free(text);
}

/* vim: set noexpandtab tabstop=4: */
