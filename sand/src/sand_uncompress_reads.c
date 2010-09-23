/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

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

	c = get_next_cseq(input);

	while (!feof(input))
	{
		if (!c.ext_id)
		{
			fprintf(stdout, ">>\n");
			c = get_next_cseq(input);
			continue;
		}
		s = uncompress_seq(c);
		free_cseq(c);
		print_sequence(stdout, s);
		free_seq(s);
		c = get_next_cseq(input);
	}
	if (c.ext_id)
	{
		s = uncompress_seq(c);
		free_cseq(c);
		print_sequence(stdout, s);
		free_seq(s);
	}

	fclose(input);
	return 0;
}
