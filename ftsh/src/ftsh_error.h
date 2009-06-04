/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef FTSH_ERROR_H
#define FTSH_ERROR_H

#include <stdarg.h>
#include <stdio.h>

#define FTSH_ERROR_SYNTAX 0
#define FTSH_ERROR_FAILURE  10
#define FTSH_ERROR_COMMAND  20
#define FTSH_ERROR_STRUCTURE 30
#define FTSH_ERROR_PROCESS   40

void ftsh_error( int level, int line, const char *fmt, ... );
void ftsh_fatal( int line, const char *fmt, ... );

void ftsh_error_name( const char *name );
void ftsh_error_stream( FILE *stream );
void ftsh_error_level( int level );
void ftsh_error_decimal_time( int onoff );

#endif
