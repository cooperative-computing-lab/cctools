/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a test program for the jx library.
It first reads in one JX expression which is used as the evaluation context.
Then, each successive expression is parsed and then evaluated.
The program exits on the first failure or EOF.
*/

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "jx_eval.h"

#include <stdio.h>
#include <errno.h>

int main( int argc, char *argv[] )
{
	jx_eval_enable_external(1);

	printf("Enter context expression (or {} for an empty context):\n");

	struct jx_parser *p = jx_parser_create(0);
	jx_parser_read_stream(p,stdin);

	struct jx *context = jx_parse(p);
	if(!context) return 1;
	if(jx_parser_errors(p)) {
		fprintf(stderr,"invalid context expression: %s\n",jx_parser_error_string(p));
		return 1;
	}

	printf("Now enter expressions:\n");

	while(1) {
		struct jx *j = jx_parse(p);

		if(!j && !jx_parser_errors(p)) {
			// end of file
			break;
		} else if(!jx_parser_errors(p)) {
			// successful parse
			printf("expression: ");
			jx_print_stream(j,stdout);
			printf("\n");

			struct jx *k = jx_eval(j,context);
			printf("value:      ");
			jx_print_stream(k,stdout);
			printf("\n\n");

			jx_delete(j);
			jx_delete(k);
		} else {
			// failed parse
			printf("\"jx parse error: %s\"\n",jx_parser_error_string(p));
			jx_delete(j);
			jx_parser_delete(p);
			return 1;
		}
	}
	jx_delete(context);
	jx_parser_delete(p);
	return 0;
}
