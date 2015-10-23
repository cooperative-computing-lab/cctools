/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a test program for the jx library.
It reads in a json expression on stdin and parses it.
On success, the expression is printed back and success is returned.
On failure to parse, an error is printed (in json form)
and failure is returned.
*/

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

#include <stdio.h>

int main( int argc, char *argv[] )
{
	struct jx *j = jx_parse_stream(stdin);

	if(j) {
		jx_print_file(j,stdout);
		printf("\n");
		jx_delete(j);
		return 0;
	} else {
		printf("\"jx parse error\"\n");
		return 1;
	}
}
