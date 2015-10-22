#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

#include <stdio.h>

int main( int argc, char *argv[] )
{
	struct jx *j = jx_parse_file(stdin);

	if(j) {
		jx_print_file(j,stdout);
		jx_delete(j);
		return 0;
	} else {
		return 1;
	}
}
