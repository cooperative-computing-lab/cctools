#include "tracer.table.h"
#include "tracer.table64.h"
#include "int_sizes.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int mkalloc( const char *path, INT64_T size, mode_t mode )
{
#ifdef CCTOOLS_ARCH_I386
	return syscall(SYSCALL32_parrot_mkalloc,path,&size,mode);
#else
	return syscall(SYSCALL64_parrot_mkalloc,path,&size,mode);
#endif
}


int main( int argc, char *argv[] )
{
	INT64_T size;

	if(argc!=3) {
		printf("use: parrot_mkalloc <path> <size>\n");
		return 0;
	}

	size = string_metric_parse(argv[2]);

	if(mkalloc(argv[1],size,0777)==0) {
		return 0;
	} else {
		if(errno==ENOSYS || errno==EINVAL) {
			fprintf(stderr,"mkalloc: This filesystem does not support allocations.\n");
		} else {
			fprintf(stderr,"mkalloc: %s\n",strerror(errno));
		}
		return 1;
	}
}
