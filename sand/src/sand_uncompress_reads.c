/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "compressed_sequence.h"

#include "cctools.h"
#include "debug.h"

static void show_help(const char *cmd)
{
        printf("Use: %s [options]  compressed_reads > fasta_reads\n", cmd);
        printf("where options are:\n");
	printf(" -q  Quiet mode: suppress summary line.\n");
	printf(" -v  Show version string.\n");
	printf(" -h  Show this help screen\n");
}

int main(int argc, char ** argv)
{
	const char *progname = "sand_uncompress_reads";
	FILE * infile;
	FILE * outfile;
	struct cseq *c;
	char d;
	int quiet_mode = 0;
	int count = 0;

        while((d=getopt(argc,argv,"qhi"))!=(char)-1) {
                switch(d) {
		case 'q':
			quiet_mode = 1;
			break;
		case 'v':
			cctools_version_print(stdout, progname);
			exit(0);
			break;
                case 'h':
		default:
                        show_help(progname);
                        exit(0);
                        break;
                }
        }

	cctools_version_debug(D_DEBUG, argv[0]);

	if( optind<argc ) {
		infile = fopen(argv[optind], "r");
		if(!infile) {
			fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind],strerror(errno));
			return 1;
		}
		optind++;
	} else {
		infile = stdin;
	}

	if( optind<argc ) {
		outfile = fopen(argv[optind],"w");
		if(!outfile) {
			fprintf(stderr,"%s: couldn't open %s: %s\n",progname,argv[optind],strerror(errno));
			return 1;
		}
		optind++;
	} else {
		outfile = stdout;
	}

	while((c=cseq_read(infile))) {
		struct seq *s = cseq_uncompress(c);
		seq_print(outfile,s);
		seq_free(s);
		cseq_free(c);
		count++;
	}

	if(!quiet_mode) {
		fprintf(stderr,"%d sequences uncompressed.\n",count);
	}

	fclose(infile);
	fclose(outfile);

	return 0;
}
