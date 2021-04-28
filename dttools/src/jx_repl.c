/*
Copyright (C) 2021- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a program to explore the jx library.
It reads in a JX expression, evaluates it, prints out the result, then saves it to the context.
Thus, previous expressions can be referenced via the `out_%d` symbol 
The program exits once EOF is reached or after the user enters the `quit` command
*/

#include "jx.h"
#include "jx_sub.h"
#include "jx_eval.h"
#include "jx_parse.h"
#include "jx_print.h"
#include "jx_pretty_print.h"

#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>


#define CATALOG_URL "http://catalog.cse.nd.edu:9097/query.json" 

#define MSG_WELCOME \
    "Welcome to the JX Language Explorer.\n"\
    "\n"\
    "Type 'help' for help\n"\
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
    "  comprehensions   expr for x in [1,2,3], [x*x for x in range(10) if x %% 2 == 0]\n"\
    "\n"


void insert_constants(struct jx *context) {
    jx_insert(context, jx_string("catalog"), jx_parse_string("fetch(\"" CATALOG_URL "\")"));
    jx_insert(context, jx_string("help"), jx_string("help"));
    jx_insert(context, jx_string("functions"), jx_string("functions"));
    jx_insert(context, jx_string("values"), jx_string("values"));
    jx_insert(context, jx_string("operators"), jx_string("operators"));
    jx_insert(context, jx_string("quit"), jx_string("exit"));
    jx_insert(context, jx_string("exit"), jx_string("exit"));
}

char *sub_to_string(struct jx *j, struct jx *context) {
    // expand symbols
    struct jx *expanded = jx_sub(j, context);

    // convert to string
    char *str = jx_print_string(expanded);

    jx_delete(expanded);

    return str;
}

struct jx * parse_line(char *line) {
    struct jx_parser *p = jx_parser_create(false);
    jx_parser_read_string(p, line);
    struct jx *j = jx_parse(p);

    if(jx_parser_errors(p)) {
        // failed parse
        printf("jx parse error: %s\n", jx_parser_error_string(p));

        jx_delete(j);
        jx_parser_delete(p);

        return NULL;
    }

    jx_parser_delete(p);

    return j;
}


int main(int argc, char *argv[]) {
    struct jx *context = jx_object(0);

    // helper constants
    insert_constants(context);

    printf(MSG_WELCOME);
    
    for (unsigned int i=0; ; i++) {
        if (i > 0) {
            printf("--------------------\n");
        }

        char in[14];
        char out[14];
        char prompt[20];

        sprintf(in, "in_%d", i);
        sprintf(out, "out_%d", i);
        sprintf(prompt, "%s  : ", in);

        char *line = readline(prompt);
        add_history(line);

        if (!line) {
            // EOF
            break;
        }

        struct jx *parsed = parse_line(line);
        free(line);

        if (!parsed) {
            continue;
        }

        // insert fully-expanded expression to context
        char *expr_str = sub_to_string(parsed, context);
        jx_insert(context, jx_string(in), jx_string(expr_str));
        free(expr_str);


        struct jx *res = jx_eval(parsed, context);

        if (!res) {
            continue;
        }

        if (res->type == JX_ERROR) {
            printf("error %s\n", res->u.err->u.string_value);
            jx_delete(res);
            continue;
        }

        if (res->type == JX_STRING) {
            if (strcmp(res->u.string_value, "exit") == 0) {
                jx_delete(res);
                break;
            } else if (strcmp(res->u.string_value, "help") == 0) {
                printf(MSG_HELP);
            } else if (strcmp(res->u.string_value, "functions") == 0) {
                printf(MSG_FUNCTIONS);
            } else if (strcmp(res->u.string_value, "operators") == 0) {
                printf(MSG_OPERATORS);
            } else if (strcmp(res->u.string_value, "values") == 0) {
                printf(MSG_VALUES);
            } else if (strcmp(res->u.string_value, "help") == 0) {
                printf(MSG_HELP);
            } else {
                printf("%s : %s\n", out, res->u.string_value);
            }

        } else {
            printf("%s : ", out);
            jx_pretty_print_stream(res, stdout);
            printf("\n");
        }

        jx_delete(res);
        jx_insert(context, jx_string(out), parsed);
    }

    jx_delete(context);

    return 0;
}
