#include "tracer.table.h"
#include "tracer.table64.h"
#include "md5.h"

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

int parrot_md5( const char *filename, char *digest )
{
#ifdef CCTOOLS_ARCH_I386
	return syscall(SYSCALL32_parrot_md5,filename,digest);
#else
	return syscall(SYSCALL64_parrot_md5,filename,digest);
#endif
}

int main( int argc, char *argv[] )
{
	int i;
	char digest[16];

	if(argc<2) {
		printf("use: parrot_md5 <file> ...\n");
		return 1;
	}

	for(i=1;i<argc;i++) {
		if(parrot_md5(argv[i],digest)>=0 || md5_file(argv[i],digest)) {
			printf("%s %s\n",md5_string(digest),argv[i]);
		} else {
			fprintf(stderr,"parrot_md5: %s: %s\n",argv[i],strerror(errno));
		}
	}

	return 0;
}
