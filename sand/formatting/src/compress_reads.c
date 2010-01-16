#include <stdio.h>
#include <stdlib.h>
#include "sequence_alignment.h"
#include "sequence_compression.h"

int main(int argc, char ** argv)
{
	FILE * input;
	seq s;
	cseq c;

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

	while (!feof(input))
	{
		s = get_next_sequence(input); 
		if (!s.id) { fprintf(stdout, ">>\n"); continue; }
		c = compress_seq(s);
		free_seq(s);
		print_cseq(stdout, c);
		free_cseq(c);
	}

	fclose(input);
	return 0;
}
