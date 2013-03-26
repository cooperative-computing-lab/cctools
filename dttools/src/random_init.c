/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "random_init.h"

#include <stdlib.h>
#include <unistd.h>
#include <time.h>

void random_init( void )
{
	srand((unsigned int) (getpid() ^ time(NULL)));
}
