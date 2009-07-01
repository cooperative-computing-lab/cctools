/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef STRINGTOOLS_H
#define STRINGTOOLS_H

#include "int_sizes.h"

typedef char * (*string_subst_lookup_t) ( const char *name, void *arg );

void   string_from_ip_address( const unsigned char *ip_addr_bytes, char *str );
int    string_to_ip_address( const char * str, unsigned char *ip_addr_bytes );
int    string_ip_subnet( const char *addr, char *subnet );
void   string_chomp( char *str );
int    string_match( const char *pattern, const char *text );
char * string_front( const char *str, int max );
const char * string_back( const char *str, int max );
const char * string_basename( const char *str );
void string_dirname( const char *path, char *dir );
char * string_metric( double invalue, int power_needed, char *buffer );
INT64_T string_metric_parse( const char *str );
int    string_time_parse( const char *str );
int    string_split( char *str, int *argc, char ***argv );
int    string_split_quotes( char *str, int *argc, char ***argv );
char * string_pad_right( char *str, int length );
char * string_pad_left( char *str, int length );
void string_cookie( char *str, int length );
char * string_subst( char *value, string_subst_lookup_t lookup, void *arg );
char * string_combine( char *first, char *second );
char * string_combine_multi( char *first, ... );
char * string_signal( int sig );
void   string_split_path( const char *str, char *first, char *rest );
void   string_collapse_path( const char *longpath, char *shortpath, int remove_dotdot );
void   string_tolower( char *str );
void   string_toupper( char *str );
int    string_isspace( const char *str );
int    string_is_integer( const char *str );
void   string_replace_backslash_codes( const char *instr, char *outstr );

int strpos(char *str, char c);
int strrpos(char *str, char c);
int getDateString(char* str);

char * strsep (char **stringp, const char *delim);

#endif
