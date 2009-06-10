/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef TRACER_H
#define TRACER_H

#include <sys/types.h>
#include "int_sizes.h"

#define TRACER_ARGS_MAX 8

void tracer_prepare();

struct tracer * tracer_attach( pid_t pid );
void            tracer_detach( struct tracer *t );
void		tracer_continue( struct tracer *t, int signum );

int             tracer_args_get( struct tracer *t, INT64_T *syscall, INT64_T args[TRACER_ARGS_MAX] );
int             tracer_args_set( struct tracer *t, INT64_T syscall, INT64_T args[TRACER_ARGS_MAX], int nargs );

INT64_T		tracer_args_get_alternate_args5( struct tracer *t );

int             tracer_result_get( struct tracer *t, INT64_T *result );
int             tracer_result_set( struct tracer *t, INT64_T result );

int             tracer_copy_out( struct tracer *t, const void *data, const void *uaddr, int length );
int             tracer_copy_in( struct tracer *t, void *data, const void *uaddr, int length );
int             tracer_copy_in_string( struct tracer *t, char *data, const void *uaddr, int maxlength );

int             tracer_is_64bit( struct tracer *t );

const char *    tracer_syscall32_name( int syscall );
const char *    tracer_syscall64_name( int syscall );
const char *    tracer_syscall_name( struct tracer *t, int syscall );

#include "tracer.table.h"
#include "tracer.table64.h"

#endif
