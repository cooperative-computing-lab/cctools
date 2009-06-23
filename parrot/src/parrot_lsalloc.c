#include "tracer.table.h"
#include "tracer.table64.h"
#include "int_sizes.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int lsalloc( const char *path, char *alloc_path, INT64_T *total, INT64_T *inuse )
{
#ifdef CCTOOLS_ARCH_I386
	return syscall(SYSCALL32_parrot_lsalloc,path,alloc_path,total,inuse);
#else
	return syscall(SYSCALL64_parrot_lsalloc,path,alloc_path,total,inuse);
#endif
}

int main( int argc, char *argv[] )
{
	const char *path;
	char alloc_path[4096];
	INT64_T total,inuse;

	if(argc<2) {
		path = ".";
	} else {
		path = argv[1];
	}

	if(argc>2 || path[0]=='-') {
		printf("use: parrot_lsalloc [path]\n");
		return 0;
	}

	if(lsalloc(path,alloc_path,&total,&inuse)==0) {
		printf("%s\n",alloc_path);
		printf("%sB TOTAL\n",string_metric(total,-1,0));
		printf("%sB INUSE\n",string_metric(inuse,-1,0));
		printf("%sB AVAIL\n",string_metric((total-inuse),-1,0));
		return 0;
	} else {
		if(errno==ENOSYS || errno==EINVAL) {
			fprintf(stderr,"lsalloc: This filesystem does not support allocations.\n");
		} else {
			fprintf(stderr,"lsalloc: %s\n",strerror(errno));
		}
		return 1;
	}
}
