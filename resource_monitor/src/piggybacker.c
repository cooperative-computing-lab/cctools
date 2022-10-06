/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "copy_stream.h"
#include "int_sizes.h"

uint64_t write_h_file(char *path_h, char *path_lib)
{
	FILE *fh, *fl;
	struct stat buf;
	unsigned char *lib_data;
	int empty = 0;
	int n;

	if(!path_lib || stat(path_lib, &buf) != 0 || !(fl = fopen(path_lib, "r")))
	{
		n = 0;
		empty = 1;
	}
	else
		n = buf.st_size;

	fh = fopen(path_h,   "w");
	if(!fh)
	{
		if(!empty)
			fclose(fl);
		return -1;
	}

	if(empty)
	{
		fprintf(fh, "static char *lib_helper_data;\n");
	}
	else
	{
		copy_stream_to_buffer(fl, (char **) &lib_data, NULL);

		fprintf(fh, "static char lib_helper_data[%" PRIu64 "] = {\n", (uint64_t) n);

		int i, column;
		for(i = 0, column = 0; i < n; i++, column++)
			if(column == 10)
			{
				column = 0;
				fprintf(fh, "%u,\n", lib_data[i]);
			}
			else
				fprintf(fh, "%u,", lib_data[i]);

		fprintf(fh, "};\n");
		free(lib_data);
	}

	return n;
}

int main(int argc, char **argv)
{
	/* name.h define_macro libname */

	if(argc == 2)
		write_h_file(argv[1], NULL);
	else if(argc == 3)
		write_h_file(argv[1], argv[2]);
	else
		return 1;

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
