/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a program to explore the jx library.
It reads in a JX expression, evaluates it, prints out the result, then saves it to the context.
Thus, expressions can reference previous expressions with the `Out` array
The program exits once EOF is reached or after `quit` command
*/

#include "jx.h"
#include "jx_eval.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "jx_pretty_print.h"
#include "jx_match.h"

#include <stdio.h>
#include <errno.h>


#define CATALOG_URL "http://catalog.cse.nd.edu:9097/query.json" 


void insert_constants(struct jx *context) {
    jx_insert(context, jx_string("catalog"), jx_parse_string("fetch(\"" CATALOG_URL "\")"));
    jx_insert(context, jx_string("quit"), jx_string("quit"));
    jx_insert(context, jx_string("help"), jx_string("help"));
}


int main( int argc, char *argv[] )
{
    // standard JX parser
	struct jx_parser *p = jx_parser_create(0);
	jx_parser_read_stream(p, stdin);

    struct jx *context = jx_object(0);

    // continually append results of each expression to context
    struct jx *out = jx_array(0);
    jx_insert(context, jx_string("Out"), out);

    // insert helper constants
    insert_constants(context);

	for (unsigned int i=0; ; i++) {
        if (i > 0) {
            printf("--------------------\n");
        }

        printf("In [%d]: ", i);

		struct jx *j = jx_parse(p);

		if(!j && !jx_parser_errors(p)) {
			// end of file
			break;
        }

        if (jx_parser_errors(p)) {
            // failed parse
            printf("jx parse error: %s\n", jx_parser_error_string(p));

            jx_delete(j);
            jx_array_append(out, jx_object(0));
            continue;
        }

        struct jx *res = jx_eval(j, context);

        if (res->type == JX_ERROR) {
            printf("error %s\n", res->u.err->u.string_value);

            // append error string to results
            jx_array_append(out, jx_string(res->u.err->u.string_value));

        } else if (res->type == JX_STRING && strcmp(res->u.string_value, "quit") == 0) {
            break;

        } else if (res->type == JX_STRING && strcmp(res->u.string_value, "help") == 0) {
            printf("\n"
                   "  Out:     array of previous output\n"
                   "  catalog: fetch catalog data\n"
                   "  quit:    exit program\n"
                   "  help:    display this message\n"
                   "\n");
            jx_array_append(out, res);

        } else {
            printf("Out[%d]: ", i);

            jx_pretty_print_stream(res, stdout);
            printf("\n");

            // append result to results
            jx_array_append(out, res);
        }

		jx_delete(j);
	}

	jx_delete(context);
	jx_parser_delete(p);

	return 0;
}
