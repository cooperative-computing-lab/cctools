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
#include "sequence_alignment.h"
#include "sequence_compression.h"
#include "sand_align_macros.h"

static void show_help(const char *cmd)
{
        printf("Use: %s [options]  fasta_reads > compressed_reads\n", cmd);
        printf("where options are:\n");
	printf(" -c             Remove Celera read_ids if file came from Celera's gatekeeper\n");
	printf(" -i             Remove read_ids but leave the Celera internal ids if the file came from Celera's gatekeeper\n"); 
	printf(" -h             Show this help screen\n");
}


int main(int argc, char ** argv)
{
	const char *progname = "sand_compress_reads";
	FILE * input;
	seq s;
	cseq c;
	char d;
	char clip = 0;
	char internal = 0;
	char tmp_id[128];

        while((d=getopt(argc,argv,"chi"))!=(char)-1) {
                switch(d) {
                case 'c':
			clip = 1;
                        break;
		case 'i':
			internal = 1;
			break; 
                case 'h':
                        show_help(progname);
                        exit(0);
                        break;
                }
        }

	if (argc == 2 && clip == 0 && internal == 0)
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


	while (!feof(input))
	{
		s = get_next_sequence(input); 
		if(clip != 0 || internal != 0){
			strcpy(tmp_id, s.id);
			strcpy(s.id, strtok(tmp_id,","));//ext_id
			if(internal != 0){
				strcpy(s.id, strtok(NULL,","));//int_id
			}
		}
		if (!s.id) { fprintf(stdout, ">>\n"); continue; }
		c = compress_seq(s);
		print_cseq(stdout, c);
		free_cseq(c);
		free_seq(s);
	}

	fclose(input);
	return 0;
}
