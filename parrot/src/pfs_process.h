/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef PFS_PROCESS_H
#define PFS_PROCESS_H

#include "pfs_types.h"
#include "pfs_table.h"
#include "pfs_sysdeps.h"

extern "C" {
#include "int_sizes.h"
#include "tracer.h"
}

#include <sys/types.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <stdint.h>

enum {
	PFS_PROCESS_FLAGS_STARTUP = (1<<0),
	PFS_PROCESS_FLAGS_ASYNC   = (1<<1)
};

enum pfs_process_state {
	PFS_PROCESS_STATE_KERNEL,
	PFS_PROCESS_STATE_USER,
};

#define PFS_SCRATCH_SIZE 4096

struct pfs_process {
	enum pfs_process_state state;

	char name[PFS_PATH_MAX];
	char new_logical_name[PFS_PATH_MAX];
	char new_physical_name[PFS_PATH_MAX];

	mode_t umask;
	pid_t  pid, ppid, tgid;
	int flags;
	uint64_t nsyscalls;
	pfs_table *table;
	struct tracer *tracer;

	size_t diverted_length;
	pfs_size_t io_channel_offset;

	char scratch_data[PFS_SCRATCH_SIZE];
	size_t scratch_used;

	INT64_T syscall;
	INT64_T syscall_original;
	INT64_T syscall_dummy;
	INT64_T syscall_parrotfd;
	INT64_T syscall_result;
	INT64_T syscall_args[TRACER_ARGS_MAX];
	INT64_T syscall_args_changed;

	int completing_execve;
	int did_stream_warning;
};

struct pfs_process * pfs_process_create( pid_t pid, pid_t ppid, int share_table );
void pfs_process_exec( struct pfs_process *p );
void pfs_process_stop( struct pfs_process *p, int status, struct rusage *usage );

extern "C" int pfs_process_getpid();
int  pfs_process_count();
struct pfs_process * pfs_process_lookup( pid_t pid );
int  pfs_process_cankill( pid_t pid );
extern "C" char *pfs_process_name();

extern "C" void pfs_process_killall();
extern "C" void pfs_process_kill_everyone(int sig);
extern "C" void pfs_process_sigio(int sig);

uintptr_t pfs_process_scratch_address( struct pfs_process *p );
void pfs_process_scratch_get( struct pfs_process *p, void *data, size_t len );
uintptr_t pfs_process_scratch_set( struct pfs_process *p, const void *data, size_t len );
void pfs_process_scratch_restore( struct pfs_process *p );

int pfs_process_stat( pid_t pid, int fd, struct stat *buf );
void pfs_process_bootstrapfd( void );

extern struct pfs_process *pfs_current;
extern int parrot_dir_fd;

#endif

/* vim: set noexpandtab tabstop=4: */
