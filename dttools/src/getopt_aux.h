/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Auxiliary functions for common processing of options */
						  
#include "getopt.h"

/** Writes port to port_file. Use when calling -Z file, when the application
chooses an arbitrary port to run.
*/

void opts_write_port_file(const char *port_file, const int port);

