/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "deltadb_expr.h"
#include "deltadb_parser.h"

#include <stdio.h>

struct deltadb_value * deltadb_symbol_lookup( const char *name )
{
	char line[1024];
	printf("enter value of %s: ",name);
	fgets(line,sizeof(line),stdin);
	struct deltadb_value *v = deltadb_parse_string_as_value(line);
	return v;
}

int main( int argc, char *argv[] )
{
	printf("Enter expressions on the console.\n");
	printf("If an expression contains a symbol, you will be prompted for the value.\n");

	char line[1024];

	while(fgets(line,sizeof(line),stdin)) {
		struct deltadb_expr *e = deltadb_parse_string_as_expr(line);
		if(e) {
			printf("expr: ");
			deltadb_expr_print(stdout,e);
			printf("\n");
			struct deltadb_value *v = deltadb_expr_eval(e);
			if(v) {
				printf("value: ");
				deltadb_value_print(stdout,v);
				deltadb_value_delete(v);
				printf("\n");
			} else {
				printf("EVAL ERROR\n");
			}
			deltadb_expr_delete(e);
		} else {
			printf("PARSE ERROR\n");
		}
	}
}
