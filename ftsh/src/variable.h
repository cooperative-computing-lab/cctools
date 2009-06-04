/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef VARIABLE_H
#define VARIABLE_H

int  variable_frame_push( int line, int argc, char **argv );
void variable_frame_pop();

void   variable_rval_set( char *rval );
char * variable_rval_get();

int  variable_shift( int n, int line );
char * variable_subst( char *value, int line );

#endif


