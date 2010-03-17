/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
	A set of macros to determine what kind of alignment program to make. By default only COMPRESSION is activated.
*/

//#define TRUNCATE 5
//#define SPEEDTEST
#define COMPRESSION
//#define STATIC_MATRIX_SIZE 1400
//#define UMD_COMPRESSION 1

#define ASSEMBLY_LINE_MAX 4096
#define SEQUENCE_ID_MAX 255
#define SEQUENCE_METADATA_MAX 255
#define ALIGNMENT_METADATA_MAX 255
#define ALIGNMENT_FLAG_MAX 2
#define CAND_FILE_LINE_MAX ((2*SEQUENCE_ID_MAX)+ALIGNMENT_FLAG_MAX+ALIGNMENT_METADATA_MAX+4)
#define SEQUENCE_FILE_LINE_MAX (SEQUENCE_ID_MAX+1+10+1+10+1+SEQUENCE_METADATA_MAX)
#define MAX_FILENAME 255
