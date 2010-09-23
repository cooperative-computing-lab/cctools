/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/*
This is a bit of a hack, but it's a step in the right direction.
sand_sw_alignment and sand_banded_alignment are almost identical programs.
To generate the latter, we just recompile the former with this macro in place.
A better solution would be to just pass in a command line argument,
but the rest of the framework isn't set up for that yet.
*/

#define DO_BANDED_ALIGNMENT
#include "sand_sw_alignment.c"
