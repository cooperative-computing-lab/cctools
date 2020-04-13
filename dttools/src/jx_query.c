/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a test program for the jx library.
It first reads in a path to a JSON document which is used as the evaluation context.
Then, it reads a JX expression which is evaluated upon the given context.
The program exits either with the evaluated result of the expression or on the first failure.
*/

#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "jx_eval.h"

#include <stdio.h>
#include <errno.h>

int main( int argc, char *argv[] )
{
    if(argc != 3) {
        fprintf(stderr, "Must specify JSON document and JX query:\n\tjx_query <JSON> <JX>\n");
        return 1;
    }
    char *path = argv[1];
    char *query = argv[2];
    FILE *json = fopen(path, "r");
    if(!json) {
        fprintf(stderr, "error opening JSON file %s - %s\n", path, strerror(errno));
        return 1;
    }

    struct jx_parser *c = jx_parser_create(0);
    jx_parser_read_stream(c, json);
    struct jx *context = jx_parse(c);
    if(!context) return 1;
    if(jx_parser_errors(c)) {
        fprintf(stderr, "invalid context expression: %s\n", jx_parser_error_string(c));
        return 1;
    }

    struct jx_parser *q = jx_parser_create(0);
    jx_parser_read_string(q, query);
    struct jx *j = jx_parse(q);
    if(jx_parser_errors(q)) {
        fprintf(stderr, "invalid query expression: %s\n", jx_parser_error_string(q));
        return 1;
    }

    if(!j && !jx_parser_errors(q)) {
            fprintf(stderr, "invalid query expression: empty query given\n");
            return 0;
    } else if(!jx_parser_errors(q)) {
            struct jx *k = jx_eval(j, context);
            jx_print_stream(k, stdout);
            fprintf(stdout, "\n");

            jx_delete(j);
            jx_delete(k);
    } else {
            fprintf(stderr, "\"jx parse error: %s\"\n", jx_parser_error_string(q));
            jx_delete(j);
            jx_parser_delete(c);
            jx_parser_delete(q);
            return 1;
    }
    jx_parser_delete(c);
    jx_parser_delete(q);
    return 0;
}

// vim: tabstop=8 shiftwidth=4 softtabstop=4 expandtab shiftround autoindent
