#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sequence_alignment.h"

int main(int argc, char ** argv)
{
	FILE * input;
	seq s;
	//cseq c;

	if (argc == 2)
	{
		input = fopen(argv[1], "r");
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

	char tmp[500];
	while (!feof(input))
	{
		s = get_next_sequence(input); 
		if (!s.id) { fprintf(stdout, ">>\n"); continue; }
		//c = compress_seq(s);
		strcpy(tmp, s.metadata);
		sprintf(s.metadata, "%d %d %s", s.length, s.length, tmp);
		print_sequence(stdout, s);
		free_seq(s);
		//print_cseq(stdout, c);
		//free_cseq(c);
	}

	fclose(input);
	return 0;
}
