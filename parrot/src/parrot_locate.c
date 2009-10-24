#include "tracer.table.h"
#include "tracer.table64.h"
#include "int_sizes.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int parrot_locate( const char *path, char *buf, int size )
{
#ifdef CCTOOLS_CPU_I386
	return syscall(SYSCALL32_parrot_locate,path,buf,size);
#else
	return syscall(SYSCALL64_parrot_locate,path,buf,size);
#endif
}

int main( int argc, char *argv[] )
{
	const char *path;
	char buf[4096];
	int result;

	if(argc<2) {
		path = ".";
	} else {
		path = argv[1];
	}

	if(argc>2 || path[0]=='-') {
		printf("use: parrot_locate [path]\n");
		return 0;
	}

	result = parrot_locate(path,buf,sizeof(buf));
	if(result < 0) {
		if(errno==ENOSYS || errno==EINVAL)
			fprintf(stderr,"locate: This filesystem doesn't support parrot_locate\n");
		else
			fprintf(stderr, "locate: %s\n", strerror(errno));
		return 1;
	} else do {
		buf[result] = 0;
		printf("%s\n",buf);
	} while( (result = parrot_locate(NULL,buf,sizeof(buf))) >= 0);
	return 0;
}
