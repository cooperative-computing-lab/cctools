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
#include "pfs_resolve.h"
#include "tracer.h"
}

#include <sys/types.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <stdint.h>

#define PFS_NGROUPS_MAX 128

typedef enum {
	PFS_PID_MODE_NORMAL,
	PFS_PID_MODE_FIXED,
	PFS_PID_MODE_WARP
} pfs_pid_mode_t;

extern pfs_pid_mode_t pfs_pid_mode;

enum {
	PFS_PROCESS_FLAGS_STARTUP = (1<<0),
	PFS_PROCESS_FLAGS_ASYNC   = (1<<1)
};

enum pfs_process_state {
	PFS_PROCESS_STATE_KERNEL,
	PFS_PROCESS_STATE_USER,
};

#define PFS_SCRATCH_SPACE (8*4096)
struct pfs_process {
	char name[PFS_PATH_MAX];
	pid_t pid, ppid, tgid;
	uid_t ruid, euid, suid, set_uid;
	gid_t rgid, egid, sgid, set_gid;
	gid_t groups[PFS_NGROUPS_MAX + 1];
	int ngroups;
	mode_t umask;
	int flags;
	struct pfs_mount_entry *ns;

	enum pfs_process_state state;
	uint64_t nsyscalls;
	pfs_table *table;
	struct tracer *tracer;

	size_t diverted_length;
	pfs_size_t io_channel_offset;
	int completing_execve;
	int did_stream_warning;
	char new_logical_name[PFS_PATH_MAX]; /* saved during execve */
	int exefd; /* during execve */

	INT64_T syscall;
	INT64_T syscall_original;
	INT64_T syscall_dummy;
	INT64_T syscall_parrotfd;
	INT64_T syscall_result;
	INT64_T syscall_args[TRACER_ARGS_MAX];
	INT64_T syscall_args_changed;

	char tmp[4096];
};

struct pfs_process * pfs_process_create( pid_t pid, struct pfs_process *parent, int thread, int share_table );
void pfs_process_exec( struct pfs_process *p );
void pfs_process_stop( struct pfs_process *p, int status, struct rusage *usage );

extern "C" int pfs_process_getpid();
int  pfs_process_count();
struct pfs_process * pfs_process_lookup( pid_t pid );
int  pfs_process_cankill( pid_t pid );
extern "C" char *pfs_process_name();
extern "C" struct pfs_mount_entry *pfs_process_current_ns(void);

extern "C" void pfs_process_killall();
extern "C" void pfs_process_kill_everyone(int sig);
extern "C" void pfs_process_sigio(int sig);

uintptr_t pfs_process_scratch_address( struct pfs_process *p );
void pfs_process_scratch_get( struct pfs_process *p, void *data, size_t len );
uintptr_t pfs_process_scratch_set( struct pfs_process *p, const void *data, size_t len );
void pfs_process_scratch_restore( struct pfs_process *p );

void pfs_process_pathtofilename( char *path );
int pfs_process_stat( pid_t pid, int fd, struct stat *buf );
void pfs_process_bootstrapfd( void );

int pfs_process_setresuid( struct pfs_process *p, uid_t ruid, uid_t euid, uid_t suid );
int pfs_process_setreuid( struct pfs_process *p, uid_t ruid, uid_t euid );
int pfs_process_setuid( struct pfs_process *p, uid_t uid );
int pfs_process_setresgid( struct pfs_process *p, gid_t rgid, uid_t egid, uid_t sgid );
int pfs_process_setregid( struct pfs_process *p, gid_t rgid, uid_t egid );
int pfs_process_setgid( struct pfs_process *p, gid_t gid );
int pfs_process_getgroups(struct pfs_process *p, int size, gid_t list[]);
int pfs_process_setgroups( struct pfs_process *p, size_t size, const gid_t *list );

extern struct pfs_process *pfs_current;
extern int parrot_dir_fd;

#endif

/* vim: set noexpandtab tabstop=4: */
