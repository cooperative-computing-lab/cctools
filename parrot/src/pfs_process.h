/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef PFS_PROCESS_H
#define PFS_PROCESS_H

#include "pfs_types.h"
#include "pfs_table.h"
#include "pfs_sysdeps.h"

extern "C" {
#include "tracer.h"
}

#include <sys/types.h>
#include <sys/resource.h>
#include <sys/time.h>

#define PFS_PROCESS_FLAGS_STARTUP 1
#define PFS_PROCESS_FLAGS_ASYNC 2

#define PFS_PROCESS_STATE_KERNEL 0
#define PFS_PROCESS_STATE_USER 1
#define PFS_PROCESS_STATE_WAITPID 2
#define PFS_PROCESS_STATE_WAITREAD 3
#define PFS_PROCESS_STATE_WAITWRITE 4
#define PFS_PROCESS_STATE_DONE 5


struct pfs_process {
	char name[PFS_PATH_MAX];
	char new_logical_name[PFS_PATH_MAX];
	char new_physical_name[PFS_PATH_MAX];
	char scratch_data[PFS_SCRATCH_SIZE];
	char tty[PFS_PATH_MAX];

	mode_t umask;
	pid_t  pid, ppid;
	int    flags, state;
	pfs_table *table;
	struct tracer *tracer;
	struct timeval seltime;

	pfs_size_t io_channel_offset;
	PTRINT_T heap_address;

	INT64_T syscall;
	INT64_T syscall_original;
	INT64_T syscall_dummy;
	INT64_T syscall_result;
	INT64_T syscall_args[TRACER_ARGS_MAX];
	INT64_T syscall_args_changed;
	INT64_T actual_result;

	int did_stream_warning;
	int diverted_length;
	int signal_interruptible[256];

	pid_t          wait_pid;
	int           *wait_ustatus;
	struct rusage *wait_urusage;
	int            wait_options;

	struct rusage  exit_rusage;
	int            exit_status;
	int 	       exit_signal;
	int            interrupted;
	int            nsyscalls;
};

struct pfs_process * pfs_process_create( pid_t pid, pid_t ppid, int share_table, int exit_signal );
struct pfs_process * pfs_process_lookup( pid_t pid );
void pfs_process_delete( struct pfs_process *p );

void pfs_process_stop( struct pfs_process *p, int status, struct rusage usage );
int pfs_process_waitpid( struct pfs_process *p, pid_t wait_pid, int *wait_ustatus, int wait_options, struct rusage *wait_urusage );

void pfs_process_sigio();
void pfs_process_wake( pid_t pid );
int  pfs_process_count();
int  pfs_process_raise( pid_t pid, int sig, int really_sendit );

extern "C" int  pfs_process_getpid();
extern "C" char * pfs_process_name();
extern "C" void pfs_process_kill();
extern "C" void pfs_process_killall();

PTRINT_T pfs_process_heap_address( struct pfs_process *p );

extern struct pfs_process *pfs_current;

#endif
