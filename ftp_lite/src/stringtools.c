/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "stringtools.h"

#include <string.h>
#include <stdio.h>
#include <math.h>

void string_chomp( char *buffer )
{
	int pos;
	char *c;

	pos = strlen(buffer);
	c = &buffer[pos];

	while(1) {
		c--;
		if( (*c=='\n') || (*c=='\r') ) {
			*c=0;
		} else {
			break;
		}
	}
}

/* vim: set noexpandtab tabstop=4: */
