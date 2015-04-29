/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef TRACER_H
#define TRACER_H

#include <sys/types.h>
#include "int_sizes.h"

#define TRACER_ARGS_MAX 8

struct tracer;

int tracer_attach( pid_t pid );
void tracer_detach( struct tracer *t );
struct tracer *tracer_init( pid_t pid );
int tracer_continue( struct tracer *t, int signum );
int tracer_listen( struct tracer *t );
int tracer_getevent( struct tracer *t, unsigned long *message );

int tracer_args_get( struct tracer *t, INT64_T *syscall, INT64_T args[TRACER_ARGS_MAX] );
int tracer_args_set( struct tracer *t, INT64_T syscall, const INT64_T args[], int nargs );

void tracer_has_args5_bug( struct tracer *t );

int tracer_result_get( struct tracer *t, INT64_T *result );
int tracer_result_set( struct tracer *t, INT64_T result );

int tracer_stack_get( struct tracer *t, uintptr_t *ptr );

/* Atomic write only. (all or nothing) */
#define TRACER_O_ATOMIC (1<<0)
/* Fast write only. (so caller can use channel otherwise) */
#define TRACER_O_FAST   (1<<1)
ssize_t tracer_copy_out( struct tracer *t, const void *data, const void *uaddr, size_t length, int flags );
ssize_t tracer_copy_in( struct tracer *t, void *data, const void *uaddr, size_t length, int flags );
ssize_t tracer_copy_in_string( struct tracer *t, char *data, const void *uaddr, size_t maxlength, int flags );

int tracer_is_64bit( struct tracer *t );

const char *tracer_syscall32_name( int syscall );
const char *tracer_syscall64_name( int syscall );
const char *tracer_syscall_name( struct tracer *t, int syscall );

#include "tracer.table.h"
#include "tracer.table64.h"

#endif
