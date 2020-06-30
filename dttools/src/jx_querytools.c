/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx.h"
#include "jx_eval.h"
#include "jx_getopt.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "stringtools.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>

const char * jx_parse_from_html ( const char *d ) {
    FILE *html = fopen(d, "r");
    char * line = NULL;
    size_t len = 0;
    int flag;
    while (getline(&line, &len, html) != -1) {
        if(flag) return line;
        if(string_match_regex(line, "<h1>Dumping raw contents of")) flag = 1;
    }
    return "";
}

struct jx * jx_query_select ( struct jx *c, struct jx *e ) {
    printf("expression: ");
    jx_print_stream(e,stdout);
    printf("\n");
    struct jx *k = jx_eval(e,c);
    printf("value:      ");
    jx_print_stream(k,stdout);
    printf("\n\n");
    return k;
}

struct jx * jx_evaluate_query ( struct jx *c ) {
    struct jx_parser *p = jx_parser_create(0);
    jx_parser_read_stream(p, stdin);
    struct jx *j = jx_parse(p); 
    if(!jx_parser_errors(p)) {
        struct jx *k = jx_query_select(c,j);
        jx_parser_delete(p);
        return k;
    } 
    printf("\"jx parse error: %s\"\n",jx_parser_error_string(p));
    jx_parser_delete(p);
    jx_delete(j);
    return NULL;
}

// vim: tabstop=8 shiftwidth=4 softtabstop=4 expandtab shiftround autoindent
