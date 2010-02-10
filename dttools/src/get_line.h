/*
Copyright (C) 2009- The University of Notre Dame
Originally written by Kevin Partington (27 January 2009)
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __LINEREADER_H__
#define __LINEREADER_H__

#define LINE_MAX 1048576

/** Read a line of any length from a file.
@param fp A file pointer, pointing to the beginning of the next line to be read.
@return A pointer to the line read. This should be used instead of the original buffer pointer once the function returns.
 **/
char * get_line( FILE *fp );

#endif
