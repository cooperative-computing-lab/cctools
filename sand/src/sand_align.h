/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SAND_ALIGN_H
#define SAND_ALIGN_H

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include <sys/stat.h>
#include <sys/resource.h>

#include "debug.h"
#include "work_queue.h"
#include "hash_table.h"
#include "stringtools.h"
#include "macros.h"

#define ASSEMBLY_LINE_MAX 4096
#define SEQUENCE_ID_MAX 255
#define SEQUENCE_METADATA_MAX 255
#define ALIGNMENT_METADATA_MAX 255
#define ALIGNMENT_FLAG_MAX 2
#define CAND_FILE_LINE_MAX ((2*SEQUENCE_ID_MAX)+ALIGNMENT_FLAG_MAX+ALIGNMENT_METADATA_MAX+4)
#define SEQUENCE_FILE_LINE_MAX (SEQUENCE_ID_MAX+1+10+1+10+1+SEQUENCE_METADATA_MAX)
#define MAX_FILENAME 255

struct sequence {
	char sequence_name[SEQUENCE_ID_MAX];
	int num_bases;
	int num_bytes;
	unsigned char* sequence_data;
	unsigned char* metadata;
};

#endif
