/*
Copyright (C) 2013- The University of Notre Dame This software is
distributed under the GNU General Public License.  See the file
COPYING for details.
*/

/* Simple program that reallocates memory from the heap, to use
 * as a test to the resource_monitor */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <inttypes.h>

#include <sys/types.h>

//in kB
#define MAX_SIZE 900000

int main(const int argc, const char **argv) {
	long long int max_size, size, i = 0, j = 0;
	char *buffer = NULL;

	max_size = MAX_SIZE;
	printf("max_size: %lld\n", max_size);

	size=1;
	do
	{
		j++;
		if( j%10000000 != 0) 
			continue;

		buffer = realloc(buffer, size*1024);
		memset((void *) buffer, 65, size*1024);
		printf("size: %lld\n", size*1024);
		//size *= 2;
		size = random() % MAX_SIZE;
		i++;
		sleep(1);
	}while(size < 2*max_size && i < 5);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
