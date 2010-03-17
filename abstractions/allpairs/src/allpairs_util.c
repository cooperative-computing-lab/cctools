/*
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <pthread.h>

#include "chirp_reli.h"
#include "chirp_matrix.h"

#include "debug.h"
#include "auth_all.h"
#include "stringtools.h"
#include "xmalloc.h"
#include "fast_popen.h"

#include "memory_info.h"
#include "load_average.h"

#define CHIRP_STABLE_ELEMENTS_MAX 100000
#define CHIRP_MOVING_ELEMENTS_MAX 100000
#define CHIRP_PROCESSOR_MAX 64
#define MAX_FILENAME_LEN 512
#define MAX_FUNCNAME_LEN 128
#define MAX_THREAD_NUMBER 128
#define USING_INNER_FUNCTION 0
#define USING_OUTER_FUNCTION 1
#define NO_COMPARE_FUNCTION 2


void get_absolute_path(char *local_path, char *path)
{
	char *p;

	p = strchr(path, '/');
	if(p == NULL || p != path) {
		getcwd(local_path, CHIRP_PATH_MAX);
		strcat(local_path, "/");
		strcat(local_path, path);
		if(local_path[strlen(local_path) - 1] != '/')
			strcat(local_path, "/");
	} else {
		// Given path is an absolute path
		strcpy(local_path, path);
		if(local_path[strlen(local_path) - 1] != '/')
			strcat(local_path, "/");
	}
}

// This function count "cached" memory as free memory besides the standard "free" memory
long get_free_mem()
{				// kb
	FILE *meminfo;
	int mem_free, mem_buffer, mem_cached, tmp;
	char buffer[128];
	char item[20];
	//int shiftbytes = 10;
	if((meminfo = fopen("/proc/meminfo", "r")) == NULL) {
		fprintf(stderr, "Cannot open /proc/meminfo!\n");
		return -1;
	}

	mem_free = mem_buffer = mem_cached = -1;

	while(fgets(buffer, 128, meminfo) != NULL) {
		if(mem_free != -1 && mem_buffer != -1 && mem_cached != -1)
			break;
		if(sscanf(buffer, "%s%d", item, &tmp) == 2) {
			if(!strcmp(item, "MemFree:")) {
				mem_free = tmp;
			} else if(!strcmp(item, "Buffers:")) {
				mem_buffer = tmp;
			} else if(!strcmp(item, "Cached:")) {
				mem_cached = tmp;
			} else {
			}
		}
	}

	fclose(meminfo);
	if(mem_free == -1 || mem_buffer == -1 || mem_cached == -1)
		return -1;
	else
		return (long) ((mem_free + mem_buffer + mem_cached) * 1024);	// >> shiftbytes;
}

int get_element_size(const char *filename)
{				// In Bytes
	struct stat s;

	if(stat(filename, &s) == -1)
		return -1;

	return s.st_size;
}


int file_line_count(const char *filename)
{
	FILE *fp;
	char buffer[MAX_FILENAME_LEN];
	int count = 0;
	int i;

	if((fp = fopen(filename, "r")) == NULL)
		return -1;

	while(fgets(buffer, MAX_FILENAME_LEN, fp) != NULL) {
		for(i = 0; i < strlen(buffer); i++) {
			if(isspace(buffer[i]) != 1) {
				count++;
				break;
			}
		}
	}

	fclose(fp);

	return count;
}

int validate_coordinates(const char *setAfile, const char *setBfile, int *p, int *q, int *r, int *s)
{
	int x1, y1, x2, y2;
	int lineCount1, lineCount2;
	int coordsInvalid = 1;

	x1 = *p;
	y1 = *q;
	x2 = *r;
	y2 = *s;

	lineCount1 = file_line_count(setAfile);
	lineCount2 = file_line_count(setBfile);

	if(x1 != -1 && x2 != -1 && y1 != -1 && y2 != -1) {
		if(x1 >= 0 && x1 <= lineCount1 && x2 >= 0 && x2 <= lineCount1 && x2 > x1 && y1 >= 0 && y1 <= lineCount2 && y2 >= 0 && y2 <= lineCount2 && y2 > y1) {
			lineCount1 = x2 - x1 + 1;
			lineCount2 = y2 - y1 + 1;
			coordsInvalid = 0;
		}		//else {
		// optional coordinates error, compute the whole matrix
		//}
	}
	//else if (x1==-1 && x2 ==-1 && y1==-1 && y2==-1) {
	// no optional coordinates, compute the whole matrix
	//} else {
	// missing optional coordinates, compute the whole matrix
	//}

	if(coordsInvalid) {
		*p = 0;
		*q = 0;
		*r = lineCount1 - 1;
		*s = lineCount2 - 1;
	}
	debug(D_DEBUG, "Start point:\t[%d, %d]\n", *p, *q);
	debug(D_DEBUG, "End point:  \t[%d, %d]\n", *r, *s);

	return 0;
}
