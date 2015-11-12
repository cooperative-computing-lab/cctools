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
	struct jx_parser *p = jx_parser_create(0);

	jx_parser_read_file(p,stdin);

	struct jx *j = jx_parse(p);

	if(!jx_parser_errors(p)) {
		jx_print_stream(j,stdout);
		printf("\n");
		jx_delete(j);
		jx_parser_delete(p);
		return 0;
	} else {
		printf("\"jx parse error: %s\"\n",jx_parser_error_string(p));
		jx_delete(j);
		jx_parser_delete(p);
		return 1;
	}
}
