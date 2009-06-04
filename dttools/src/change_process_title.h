#ifndef CHANGE_PROCESS_TITLE_H
#define CHANGE_PROCESS_TITLE_H

/** @file change_process_title.h
Change the title of a process in ps and top.
This module only works on Linux, and has no effect on other platforms.
@ref change_process_title_init must be called once before processing arguments,
and then @ref change_process_title may be called many times to change the title.
*/

/**
Intialize the ability to change the process title.
@param argv The argument vector passed to the main() function.
*/

void change_process_title_init( char **argv );

/**
Change the process title.
@param fmt A printf-style formatting string.
*/

void change_process_title( const char *fmt, ... );

#endif
