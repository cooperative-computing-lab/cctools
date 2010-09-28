/*
Copyright (C) 2009- The University of Notre Dame & University of Cambridge
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <errno.h>

#include "sequence_compression.h"

#include "debug.h"

static void show_version(const char *cmd)
{
        printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
        printf("Use: %s [options]  fasta_reads > compressed_reads\n", cmd);
        printf("where options are:\n");
	printf(" -v  Show version string.\n");
	printf(" -h  Show this help screen\n");
}

int main(int argc, char ** argv)
{
	const char *progname = "sand_compress_reads";
	FILE * input;
	struct seq *s;
	struct cseq *c;
	char d;

        while((d=getopt(argc,argv,"chi"))!=(char)-1) {
                switch(d) {
		case 'v':
			show_version(progname);
			exit(0);
			break;
                case 'h':
                        show_help(progname);
                        exit(0);
                        break;
                }
        }

	if( optind<(argc-1) ) {
		input = fopen(argv[optind], "r");
		if (!input) fatal("couldn't open %s: %s\n",argv[optind],strerror(errno));
	} else {
		input = stdin;
	}

	while (!feof(input))
	{
		s = seq_read(input); 
		c = seq_compress(s);
		cseq_print(stdout, c);
		cseq_free(c);
		seq_free(s);
	}

	fclose(input);
	return 0;
}
