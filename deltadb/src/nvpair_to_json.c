
#include "jx.h"
#include "jx_parse.h"
#include "jx_print.h"

#include "nvpair.h"
#include "nvpair_jx.h"

#include <stdio.h>

int main( int argc, char *argv[] )
{
	int count =0;
	int first = 0;

	printf("{\n");

	while(1) {
		struct nvpair *nv = nvpair_create();
		int r = nvpair_parse_stream(nv,stdin);
		if(r) {
			struct jx *j = nvpair_to_jx(nv);
			const char *name = jx_lookup_string(j,"name");
			const char *host = jx_lookup_string(j,"host");
			int port = jx_lookup_integer(j,"port");

			if(first) {
				first = 0;
			} else {
				printf(",\n");
			}
			printf("\"%s:%s:%d\":",name,host,port);

			jx_print_stream(j,stdout);
			count++;
		} else if(r<0) {
			fprintf(stderr,"nvpair conversion error!\n");
		} else {
			break;
		}

	}

	printf("\n}\n");

	fprintf(stderr,"%d records converted.\n",count);
	return 0;
}
