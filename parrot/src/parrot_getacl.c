
#include "tracer.table.h"
#include "int_sizes.h"
#include "stringtools.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

int parrot_getacl( const char *path, char *buf, int size )
{
	return syscall(SYSCALL32_parrot_getacl,path,buf,size);
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
		printf("use: parrot_getacl [path]\n");
		return 0;
	}

	result = parrot_getacl(path,buf,sizeof(buf));
	if(result>=0) {
		buf[result] = 0;
		printf("%s",buf);
		return 0;
	} else {
		if(errno==ENOSYS || errno==EINVAL) {
			fprintf(stderr,"getacl: This filesystem does not support Parrot access controls.\n");
		} else {
			fprintf(stderr,"getacl: %s\n",strerror(errno));
		}
		return 1;
	}
}
