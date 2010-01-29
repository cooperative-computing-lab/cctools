/*
Copyright (C) 2009- The University of Notre Dame
Originally written by Kevin Partington (27 January 2009)
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef __LINEREADER_H__
#define __LINEREADER_H__

/** Read a line of any length from a file.
The function is invoked with a stack buffer ("buffer") of a set size.
It will try to populate the stack buffer as much as it can, and if a newline
is not found in the string (possibly meaning a line is too long), then a heap
buffer ("other") allocated to the appropriate size will be used instead. The
user does not know which buffer is returned (either the stack buffer or the
heap buffer), nor does the user need to care. Even at EOF, the function must be
called again, so the heap buffer will be freed.
 **/
char * get_line( FILE *fp, char *buffer, int size );

#endif
