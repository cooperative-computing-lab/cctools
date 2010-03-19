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

//
void get_absolute_path(char *local_path, char *path);

// This function count "cached" memory as free memory besides the standard "free" memory
long get_free_mem(); // kb

//
int get_element_size(const char *filename); // In Bytes

//  
int file_line_count(const char *filename);

//
int validate_coordinates(const char *setAfile, const char *setBfile, int *p, int *q, int *r, int *s);

//
double elapsedtime(struct timeval *t_start, struct timeval *t_end);
