/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h> 
#include "sequence_alignment.h"
#include "sequence_compression.h"

static void show_help(const char *cmd)
{
        printf("Use: %s [options]  fasta_reads > compressed_reads\n", cmd);
        printf("where options are:\n");
	printf(" -c             Remove Celera read_ids if file came from Celera's gatekeeper\n"); 
	printf(" -h             Show this help screen\n");
}


int main(int argc, char ** argv)
{
	const char *progname = "sand_compress_reads";
	FILE * input;
	seq s;
	cseq c;
	char d;
	char *clip = "";

        while((d=getopt(argc,argv,"ch"))!=(char)-1) {
                switch(d) {
                case 'c':
			clip = "clip";
                        break;
                case 'h':
                        show_help(progname);
                        exit(0);
                        break;
                }
        }

	if (argc == 2 && clip == "")
	{
		input = fopen(argv[1], "r");
		if (!input)
		{
			fprintf(stderr, "ERROR: Could not open file %s for reading.\n", argv[1]);
			exit(1);
		}
	}
	else if (argc == 3)
	{
		input = fopen(argv[2], "r");
                if (!input)
                {
                        fprintf(stderr, "ERROR: Could not open file %s for reading.\n", argv[1]);
                        exit(1);
                }

	}
	else
	{
		input = stdin;
	}

	//s = get_next_sequence(input);

	while (!feof(input))
	{
		if(clip != ""){
			//fprintf(stderr, "clipping enabled"); 
			s = get_next_sequence_clip(input); 
		}	
		else{
			s = get_next_sequence(input); 
		}
		if (!s.id) { fprintf(stdout, ">>\n"); continue; }
		c = compress_seq(s);
		free_seq(s);
		print_cseq(stdout, c);
		free_cseq(c);
	}

	fclose(input);
	return 0;
}
