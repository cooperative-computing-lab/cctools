/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

static const char *helptext = "This program takes a list of files and generates\na tar archive file on the standard output.  It does one thing\nthat normal tar cannot: it gives the files in the archive different\nnames from what they are actually called.  This allows us to create\nan efficient streamed archive output for the BXGrid repository.\n\nExample:\n    tar_stream file.list > package.tar\n\nWhere file.list contains:\n\n    dataone.jpg  /package/1.jpg\n    datatwo.jpg  /package/2.jpg\n\nWill create an archive containing the files dataone.jpg and datatwo.jpg\nbut named 1.jpg and 2.jpg withing the package directory of the archive file.\n";

#include "copy_stream.h"

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

struct tar_header {
	char name[100];
	char mode[8];
	char owner[8];
	char group[8];
	char size[12];
	char mtime[12];
	char checksum[8];
	char link[1];
	char linkname[100];
	char padding[255];
};

#define TAR_LINE_MAX 4096

int main( int argc, char *argv[] )
{
	FILE *listfile;
	FILE *datafile;

	char line[TAR_LINE_MAX];
	char physical_name[TAR_LINE_MAX];
	char logical_name[TAR_LINE_MAX];

	int i, result, fields;
	struct tar_header header;
	struct stat info;

	char *zeros = malloc(512);

	memset(zeros,0,512);

	if(argc!=2) {
		fprintf(stderr,"\nuse: tar_stream <listfile>\n\n%s",helptext);
		return 1;
	}

	listfile = fopen(argv[1],"r");
	if(!listfile) {
		fprintf(stderr,"could not open %s: %s\n",argv[1],strerror(errno));
		return 1;
	}

	while(fgets(line,sizeof(line),listfile)) {
		fields = sscanf(line,"%s %s",physical_name,logical_name);
		if(fields!=2) {
			fprintf(stderr,"syntax error in %s: %s\n",argv[1],line);
			return 1;
		}

		memset(&header,0,sizeof(header));

		result = stat(physical_name,&info);
		if(result!=0) {
			fprintf(stderr,"could't stat %s: %s\n",physical_name,strerror(errno));
			return 1;
		}

		strcpy(header.name,logical_name);
		sprintf(header.mode,"0000600");
		sprintf(header.owner,"0000000");
		sprintf(header.group,"0000000");
		sprintf(header.size,"%011o",(int)info.st_size);
		sprintf(header.mtime,"%011o",(int)info.st_mtime);
		sprintf(header.checksum,"        ");
		header.link[0] = '0';

		unsigned checksum = 0;
		unsigned char *headerdata = (void*)&header;

		for(i=0;i<sizeof(header);i++) {
			checksum += headerdata[i];
		}

		sprintf(header.checksum,"%06o",checksum);

		fwrite(&header,sizeof(header),1,stdout);

		datafile = fopen(physical_name,"r");
		if(!datafile) {
			fprintf(stderr,"couldn't open %s: %s\n",physical_name,strerror(errno));
			return 1;
		}

		copy_stream_to_stream(datafile,stdout);

		fclose(datafile);

		int padlength = 512 - (info.st_size % 512);
		if(padlength!=512) fwrite(zeros,padlength,1,stdout);

	}

	fwrite(zeros,512,1,stdout);
	fwrite(zeros,512,1,stdout);

	fflush(stdout);

	return 0;
}
