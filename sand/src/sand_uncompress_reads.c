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

int main(int argc, char ** argv)
{
	FILE * input;
	seq s;
	cseq c;

	if (argc == 2) {
		input = fopen(argv[1], "r");
		if (!input) fatal("couldn't open %s: %s\n",argv[1],strerror(errno));
	} else {
		input = stdin;
	}

	c = cseq_read(input);

	while (!feof(input))
	{
		if (!c.ext_id)
		{
			fprintf(stdout, ">>\n");
			c = cseq_read(input);
			continue;
		}
		s = cseq_uncompress(c);
		cseq_free(c);
		seq_print(stdout, s);
		seq_free(s);
		c = cseq_read(input);
	}

	if (c.ext_id)
	{
		s = cseq_uncompress(c);
		cseq_free(c);
		seq_print(stdout, s);
		seq_free(s);
	}

	fclose(input);
	return 0;
}
