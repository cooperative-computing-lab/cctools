/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
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
        printf("Use: %s [options]  compressed_reads > fasta_reads\n", cmd);
        printf("where options are:\n");
	printf(" -v  Show version string.\n");
	printf(" -h  Show this help screen\n");
}

int main(int argc, char ** argv)
{
	const char *progname = "sand_uncompress_reads";
	FILE * input;
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

	if (optind<(argc-1)) {
		input = fopen(argv[optind], "r");
		if (!input) fatal("couldn't open %s: %s\n",argv[optind],strerror(errno));
	} else {
		input = stdin;
	}

	while((c=cseq_read(input))) {
		seq s = cseq_uncompress(c);
		seq_print(stdout,s);
		seq_free(s);
		cseq_free(c);
	}

	fclose(input);
	return 0;
}
