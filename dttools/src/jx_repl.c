/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a program to explore the jx library.
It reads in a JX expression, evaluates it, prints out the result, then saves it to the context.
Thus, previous expressions can be referenced via the `out` array
The program exits once EOF is reached or after the user enters the `quit` command
*/

#include "jx.h"
#include "jx_eval.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"

#include <stdio.h>


#define CATALOG_URL "http://catalog.cse.nd.edu:9097/query.json" 

#define MSG_WELCOME \
    "Welcome to the JX Language Explorer.  Commands end with ';'\n"\
    "\n"\
    "Type 'help;' for help\n"\
    "\n"
#define MSG_HELP \
    "\n"\
    "  help          display this message\n"\
    "  functions     display a list of functions supported by the JX language\n"\
    "  values        display a list of values supported by the JX language\n"\
    "  operators     display a list of operators supported by the JX language\n"\
    "  out           array of previous output\n"\
    "  catalog       alias to fetch catalog data\n"\
    "  quit|exit     exit program\n"\
    "\n"
#define MSG_FUNCTIONS \
    "\n"\
    "  format( \"str: %%s int: %%d float: %%f\", \"hello\", 42, 3.14159 )\n"\
    "  join( array, delim )\n"\
    "  range( start, stop, step )\n"\
    "  ceil( value )\n"\
    "  floor( value )\n"\
    "  basename( path )\n"\
    "  dirname( path )\n"\
    "  escape( string )\n"\
    "  len( array )\n"\
    "  fetch( URL/path )\n"\
    "  select( boolean, array )\n"\
    "  project( expression, array )\n"\
    "  schema( object )\n"\
    "  like( string, object )\n"\
    "\n"
#define MSG_VALUES \
    "\n"\
    "  string       \"string\"\n"\
    "  integer      42\n"\
    "  float        3.14159\n"\
    "  boolean      true | false\n"\
    "  array        [ 1, 2, 3 ]\n"\
    "  objects      { \"temp\": 32, \"name\": \"fred\" }\n"\
    "\n"
#define MSG_OPERATORS \
    "\n"\
    "  lookup           obj[\"a\"], arr[0], arr[0:10]\n"\
    "  concatenation    \"abc\" + \"def\" -> \"abcdef\"\n"\
    "  arithmetic       * / + - %%\n"\
    "  logic            and or not\n"\
    "  comparison       ==  !=  <  <=  >  >=\n"\
    "  comprehensions   [i for i in range(10)], [x*x for x in [1,2,3,4] if x %% 2 == 0]\n"\
    "\n"


void insert_constants(struct jx *context) {
    jx_insert(context, jx_string("catalog"), jx_parse_string("fetch(\"" CATALOG_URL "\")"));
    jx_insert(context, jx_string("quit"), jx_string("exit"));
    jx_insert(context, jx_string("exit"), jx_string("exit"));
    jx_insert(context, jx_string("help"), jx_string("help"));
    jx_insert(context, jx_string("functions"), jx_string("functions"));
    jx_insert(context, jx_string("values"), jx_string("values"));
    jx_insert(context, jx_string("operators"), jx_string("operators"));
}


int main(int argc, char *argv[]) {
    // standard JX parser
    struct jx_parser *p = jx_parser_create(0);
    jx_parser_read_stream(p, stdin);

    struct jx *context = jx_object(0);

    // continually append results of each expression to context
    struct jx *out = jx_array(0);
    jx_insert(context, jx_string("out"), out);

    // helper constants
    insert_constants(context);

    printf(MSG_WELCOME);
    
    for (unsigned int i=0; ; i++) {
        if (i > 0) {
            printf("--------------------\n");
        }

        printf("in [%d]: ", i);

        struct jx *j = jx_parse(p);

        if(!j && !jx_parser_errors(p)) {
            // end of file
            break;
        }

        if (jx_parser_errors(p)) {
            // failed parse
            printf("jx parse error: %s\n", jx_parser_error_string(p));

            jx_delete(j);
            break;
        }

        struct jx *res = jx_eval(j, context);

        if (res->type == JX_ERROR) {
            printf("error %s\n", res->u.err->u.string_value);

            // append error string to results
            jx_array_append(out, jx_string(res->u.err->u.string_value));
            jx_delete(j);
            continue;
        }

        if (res->type == JX_STRING && strcmp(res->u.string_value, "exit") == 0) {
            jx_delete(res);
            jx_delete(j);
            break;

        } else if (res->type == JX_STRING && strcmp(res->u.string_value, "help") == 0) {
            printf(MSG_HELP);

        } else if (res->type == JX_STRING && strcmp(res->u.string_value, "functions") == 0) {
            printf(MSG_FUNCTIONS);

        } else if (res->type == JX_STRING && strcmp(res->u.string_value, "values") == 0) {
            printf(MSG_VALUES);

        } else if (res->type == JX_STRING && strcmp(res->u.string_value, "operators") == 0) {
            printf(MSG_OPERATORS);

        } else {
            printf("out[%d]: ", i);
            jx_pretty_print_stream(res, stdout);
            printf("\n");
        }

        jx_array_append(out, res);
        jx_delete(j);
    }

    jx_delete(context);
    jx_parser_delete(p);

    return 0;
}
