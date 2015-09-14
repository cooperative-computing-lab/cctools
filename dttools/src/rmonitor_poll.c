/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <sys/time.h>

#include "debug.h"
#include "path_disk_size_info.h"
#include "macros.h"
#include "xxmalloc.h"

#include "rmonitor_poll_internal.h"

/***
 * Helper functions
***/

#define div_round_up(a, b) (((a) + (b) - 1) / (b))

uint64_t usecs_since_epoch()
{
	uint64_t usecs;
	struct timeval time;

	gettimeofday(&time, NULL);

	usecs  = time.tv_sec;
	usecs *= ONE_SECOND;
	usecs += time.tv_usec;

	return usecs;
}

/***
 * Functions to track the whole process tree.  They call the
 * functions defined just above, accumulating the resources of
 * all the processes.
***/

void rmonitor_poll_all_processes_once(struct itable *processes, struct rmonitor_process_info *acc)
{
	uint64_t pid;
	struct rmonitor_process_info *p;

	bzero(acc, sizeof( struct rmonitor_process_info ));

	itable_firstkey(processes);
	while(itable_nextkey(processes, &pid, (void **) &p))
	{
		rmonitor_poll_process_once(p);

		acc_mem_usage(&acc->mem, &p->mem);

		acc_cpu_time_usage(&acc->cpu, &p->cpu);

		acc_sys_io_usage(&acc->io, &p->io);
		acc_map_io_usage(&acc->io, &p->io);
	}
}

void rmonitor_poll_all_wds_once(struct hash_table *wdirs, struct rmonitor_wdir_info *acc, int max_time_for_measurement)
{
	struct rmonitor_wdir_info *d;
	char *path;

	bzero(acc, sizeof( struct rmonitor_wdir_info ));

	if(hash_table_size(wdirs) > 0) {
		if(max_time_for_measurement > 0) {
			/* split time available across all directories to measure. */
			max_time_for_measurement = MAX(1, max_time_for_measurement/hash_table_size(wdirs));
		}

		hash_table_firstkey(wdirs);
		while(hash_table_nextkey(wdirs, &path, (void **) &d))
		{
			rmonitor_poll_wd_once(d, max_time_for_measurement);
			acc_wd_usage(acc, d);
		}
	}
}

void rmonitor_poll_all_fss_once(struct itable *filesysms, struct rmonitor_filesys_info *acc)
{
	struct rmonitor_filesys_info *f;
	uint64_t dev_id;

	bzero(acc, sizeof( struct rmonitor_filesys_info ));

	itable_firstkey(filesysms);
	while(itable_nextkey(filesysms, &dev_id, (void **) &f))
	{
		rmonitor_poll_fs_once(f);
		acc_dsk_usage(&acc->disk, &f->disk);
	}
}


/***
 * Functions to monitor a single process, workind directory, or
 * filesystem.
***/

int rmonitor_poll_process_once(struct rmonitor_process_info *p)
{
	debug(D_DEBUG, "monitoring process: %d\n", p->pid);

	rmonitor_get_cpu_time_usage(p->pid, &p->cpu);
	rmonitor_get_mem_usage(p->pid, &p->mem);
	rmonitor_get_sys_io_usage(p->pid, &p->io);
	//rmonitor_get_map_io_usage(p->pid, &p->io);

	return 0;
}

int rmonitor_poll_wd_once(struct rmonitor_wdir_info *d, int max_time_for_measurement)
{
	debug(D_DEBUG, "monitoring dir %s\n", d->path);

	rmonitor_get_wd_usage(d, max_time_for_measurement);

	return 0;
}

int rmonitor_poll_fs_once(struct rmonitor_filesys_info *f)
{
	rmonitor_get_dsk_usage(f->path, &f->disk);

	f->disk.f_bfree  = f->disk_initial.f_bfree  - f->disk.f_bfree;
	f->disk.f_bavail = f->disk_initial.f_bavail - f->disk.f_bavail;
	f->disk.f_ffree  = f->disk_initial.f_ffree  - f->disk.f_ffree;

	return 0;
}

/***
 * Utility functions (open log files, proc files, measure time)
 ***/

FILE *open_proc_file(pid_t pid, char *filename)
{
		FILE *fproc;
		char fproc_path[PATH_MAX];

#if defined(CCTOOLS_OPSYS_DARWIN)
		return NULL;
#endif

		if(pid > 0)
		{
			sprintf(fproc_path, "/proc/%d/%s", pid, filename);
		}
		else
		{
			sprintf(fproc_path, "/proc/%s", filename);
		}

		if((fproc = fopen(fproc_path, "r")) == NULL)
		{
				debug(D_DEBUG, "could not process file %s : %s\n", fproc_path, strerror(errno));
				return NULL;
		}

		return fproc;
}

/* Parse a /proc file looking for line attribute: value */
int rmonitor_get_int_attribute(FILE *fstatus, char *attribute, uint64_t *value, int rewind_flag)
{
	char proc_attr_line[PATH_MAX];
	int not_found = 1;
	int n = strlen(attribute);

	if(!fstatus)
		return not_found;

	proc_attr_line[PATH_MAX - 2] = '\0';
	proc_attr_line[PATH_MAX - 1] = '\0';

	if(rewind_flag)
		rewind(fstatus);

	while( fgets(proc_attr_line, PATH_MAX - 2, fstatus) )
	{
		if(strncmp(attribute, proc_attr_line, n) == 0)
		{
			//We make sure that fgets got a whole line
			if(proc_attr_line[PATH_MAX - 2] == '\n')
				proc_attr_line[PATH_MAX - 2] = '\0';
			if(strlen(proc_attr_line) == PATH_MAX - 2)
				return -1;

			sscanf(proc_attr_line, "%*s %" SCNu64, value);
			not_found = 0;
			break;
		}
	}

	return not_found;
}

uint64_t clicks_to_usecs(uint64_t clicks)
{
	return ((clicks * ONE_SECOND) / sysconf(_SC_CLK_TCK));
}

/***
 * Low level resource monitor functions.
 ***/

int rmonitor_get_start_time(pid_t pid, uint64_t *start_time)
{
	/* /dev/proc/[pid]/stat */

	uint64_t start_clicks;
	double uptime;

	FILE *fstat = open_proc_file(pid, "stat");
	if(!fstat)
		return 1;

	int n;
	n = fscanf(fstat,
			"%*s" /* pid */ "%*s" /* cmd line */ "%*s" /* state */ "%*s" /* pid of parent */
			"%*s" /* group ID */ "%*s" /* session id */ "%*s" /* tty pid */ "%*s" /* tty group ID */
			"%*s" /* linux/sched.h flags */ "%*s %*s %*s %*s" /* faults */
			"%*s" /* user mode time (in clock ticks)   (field 14)  */
			"%*s" /* kernel mode time (in clock ticks) (field 15) */
			"%*s" /* time (clock ticks) waiting for children in user mode */
			"%*s" /* time (clock ticks) waiting for children in kernel mode */
			"%*s" /* priority */ "%*s" /* nice */
			"%*s" /* num threads */ "%*s" /* always 0 */
			"%" SCNu64 /* clock ticks since start     (field 22) */
			/* .... */,
			&start_clicks);
	fclose(fstat);

	if(n != 1)
		return 1;

	FILE *fuptime = open_proc_file(0, "uptime");
	if(!fuptime)
		return 1;

	n = fscanf(fuptime, "%lf %*s", &uptime);
	fclose(fuptime);

	if(n != 1)
		return 1;

	uint64_t origin = usecs_since_epoch() - (uptime * ONE_SECOND);
	*start_time     = origin + clicks_to_usecs(start_clicks);

	return 0;
}

int rmonitor_get_cpu_time_usage(pid_t pid, struct rmonitor_cpu_time_info *cpu)
{
	/* /dev/proc/[pid]/stat */

	uint64_t kernel, user;


	FILE *fstat = open_proc_file(pid, "stat");
	if(!fstat)
		return 1;

	int n;
	n = fscanf(fstat,
			"%*s" /* pid */ "%*s" /* cmd line */ "%*s" /* state */ "%*s" /* pid of parent */
			"%*s" /* group ID */ "%*s" /* session id */ "%*s" /* tty pid */ "%*s" /* tty group ID */
			"%*s" /* linux/sched.h flags */ "%*s %*s %*s %*s" /* faults */
			"%" SCNu64 /* user mode time (in clock ticks) */
			"%" SCNu64 /* kernel mode time (in clock ticks) */
			/* .... */,
			&kernel, &user);
	fclose(fstat);

	if(n != 2)
		return 1;

	uint64_t accum = clicks_to_usecs(kernel) + clicks_to_usecs(user);

	cpu->delta       = accum  - cpu->accumulated;
	cpu->accumulated = accum;

	return 0;
}

void acc_cpu_time_usage(struct rmonitor_cpu_time_info *acc, struct rmonitor_cpu_time_info *other)
{
	acc->delta += other->delta;
}


int rmonitor_get_swap_usage(pid_t pid, struct rmonitor_mem_info *mem)
{
	FILE *fsmaps = open_proc_file(pid, "smaps");
	if(!fsmaps)
		return 1;

	/* in kB */
	uint64_t accum = 0;
	uint64_t value = 0;

	while(rmonitor_get_int_attribute(fsmaps, "Swap:", &value, 0) == 0)
		accum += value;

	mem->swap = accum;

	fclose(fsmaps);

	return 0;
}

int rmonitor_get_mem_usage(pid_t pid, struct rmonitor_mem_info *mem)
{
	// /dev/proc/[pid]/status:

	FILE *fmem = open_proc_file(pid, "status");
	if(!fmem)
		return 1;

	int status = 0;

	/* in kB */
	status |= rmonitor_get_int_attribute(fmem, "VmPeak:", &mem->virtual,  1);
	status |= rmonitor_get_int_attribute(fmem, "VmHWM:",  &mem->resident, 1);
	status |= rmonitor_get_int_attribute(fmem, "VmLib:",  &mem->shared,   1);
	status |= rmonitor_get_int_attribute(fmem, "VmExe:",  &mem->text,     1);
	status |= rmonitor_get_int_attribute(fmem, "VmData:", &mem->data,     1);
	status |= rmonitor_get_swap_usage(pid, mem);

	fclose(fmem);

	/* One of the fields was not found, so we return early. */
	if(status)
		return 1;

	/* in MB */
	mem->virtual  = div_round_up(mem->virtual,  1024);
	mem->resident = div_round_up(mem->resident, 1024);
	mem->shared   = div_round_up(mem->shared,   1024);
	mem->text     = div_round_up(mem->text,     1024);
	mem->data     = div_round_up(mem->data,     1024);
	mem->swap     = div_round_up(mem->swap,     1024);

	return 0;
}

void acc_mem_usage(struct rmonitor_mem_info *acc, struct rmonitor_mem_info *other)
{
		acc->virtual  += other->virtual;
		acc->resident += other->resident;
		acc->shared   += other->shared;
		acc->data     += other->data;
		acc->swap     += other->swap;
}

int rmonitor_get_sys_io_usage(pid_t pid, struct rmonitor_io_info *io)
{
	/* /proc/[pid]/io: if process dies before we read the file,
	   then info is lost, as if the process did not read or write
	   any characters.
	*/

	FILE *fio = open_proc_file(pid, "io");
	uint64_t cread, cwritten;
	int rstatus, wstatus;

	io->delta_chars_read = 0;
	io->delta_chars_written = 0;

	if(!fio)
		return 1;

	/* We really want "bytes_read", but there are issues with
	 * distributed filesystems. Instead, we also count page
	 * faulting in another function below. */
	rstatus  = rmonitor_get_int_attribute(fio, "rchar", &cread, 1);
	wstatus  = rmonitor_get_int_attribute(fio, "write_bytes", &cwritten, 1);

	fclose(fio);

	if(rstatus || wstatus)
		return 1;

	io->delta_chars_read    = cread    - io->chars_read;
	io->delta_chars_written = cwritten - io->chars_written;

	io->chars_read = cread;
	io->chars_written = cwritten;

	return 0;
}

void acc_sys_io_usage(struct rmonitor_io_info *acc, struct rmonitor_io_info *other)
{
	acc->delta_chars_read    += other->delta_chars_read;
	acc->delta_chars_written += other->delta_chars_written;
}

/* We compute the resident memory changes from mmap files. */
int rmonitor_get_map_io_usage(pid_t pid, struct rmonitor_io_info *io)
{
	/* /dev/proc/[pid]/smaps */

	uint64_t kbytes_resident_accum;
	uint64_t kbytes_resident;

	kbytes_resident_accum    = 0;
	io->delta_bytes_faulted = 0;

	FILE *fsmaps = open_proc_file(pid, "smaps");
	if(!fsmaps)
	{
		return 1;
	}

	char dummy_line[1024];

	/* Look for next mmap file */
	while(fgets(dummy_line, 1024, fsmaps))
		if(strchr(dummy_line, '/'))
			if(rmonitor_get_int_attribute(fsmaps, "Rss:", &kbytes_resident, 0) == 0)
				kbytes_resident_accum += kbytes_resident;

	if((kbytes_resident_accum * 1024) > io->bytes_faulted)
		io->delta_bytes_faulted = (kbytes_resident_accum * 1024) - io->bytes_faulted;

	/* in bytes */
	io->bytes_faulted = (kbytes_resident_accum * 1024);

	fclose(fsmaps);

	return 0;
}

void acc_map_io_usage(struct rmonitor_io_info *acc, struct rmonitor_io_info *other)
{
	acc->delta_bytes_faulted += other->delta_bytes_faulted;
}


/***
 * Low level resource monitor functions.
 ***/

int rmonitor_get_dsk_usage(const char *path, struct statfs *disk)
{
	char cwd[PATH_MAX];

	debug(D_DEBUG, "statfs on path: %s\n", path);

	if(statfs(path, disk) > 0)
	{
		debug(D_DEBUG, "could not statfs on %s : %s\n", cwd, strerror(errno));
		return 1;
	}

	return 0;
}

void acc_dsk_usage(struct statfs *acc, struct statfs *other)
{
	acc->f_bfree  += other->f_bfree;
	acc->f_bavail += other->f_bavail;
	acc->f_ffree  += other->f_ffree;
}

int rmonitor_get_wd_usage(struct rmonitor_wdir_info *d, int max_time_for_measurement)
{
	/* We need a pointer to a pointer, which it is not possible from a struct. Use a dummy variable. */
	struct path_disk_size_info *state = d->state;
	path_disk_size_info_get_r(d->path, max_time_for_measurement, &state);
	d->state = state;

	d->files = d->state->last_file_count_complete;
	d->byte_count = d->state->last_byte_size_complete;

	return 0;
}

void acc_wd_usage(struct rmonitor_wdir_info *acc, struct rmonitor_wdir_info *other)
{
	acc->files       += other->files;
	acc->byte_count  += other->byte_count;
}

char *rmonitor_get_command_line(pid_t pid)
{
	/* /dev/proc/[pid]/cmdline */

	FILE *fline = open_proc_file(pid, "cmdline");
	if(!fline)
		return NULL;

	char cmdline[PATH_MAX];
	ssize_t cmdline_len = read(fileno(fline), cmdline, PATH_MAX);

	if(cmdline_len < 1)
		return NULL;

	int i;
	for(i=0; i < cmdline_len - 1; i++) { /* -1 because cmdline ends with two \0. */
		if(cmdline[i] == '\0')
			cmdline[i] = ' ';
	}

	return xxstrdup(cmdline);
}

void rmonitor_info_to_rmsummary(struct rmsummary *tr, struct rmonitor_process_info *p, struct rmonitor_wdir_info *d, struct rmonitor_filesys_info *f, uint64_t start_time)
{
	tr->start        = start_time;
	tr->end          = usecs_since_epoch();
	tr->wall_time    = tr->end - tr->start;
	tr->cpu_time     = p->cpu.accumulated;
	tr->cores        = -1;

	if(tr->wall_time > 0)
		tr->cores = (int64_t) ceil( ((double) tr->cpu_time)/tr->wall_time);

	tr->max_concurrent_processes = -1;
	tr->total_processes          = -1;

	tr->virtual_memory    = (int64_t) p->mem.virtual;
	tr->resident_memory   = (int64_t) p->mem.resident;
	tr->swap_memory       = (int64_t) p->mem.swap;

	tr->bytes_read        = (int64_t)  p->io.chars_read;
	tr->bytes_written     = (int64_t)  p->io.chars_written;

	tr->workdir_num_files = -1;
	tr->workdir_footprint = -1;

	if(d) {
		tr->workdir_num_files = (int64_t) (d->files);
		tr->workdir_footprint = (int64_t) (d->byte_count + ONE_MEGABYTE - 1) / ONE_MEGABYTE;
	}

	tr->fs_nodes = -1;
	if(f) {
		tr->fs_nodes          = (int64_t) f->disk.f_ffree;
	}
}

int rmonitor_measure_process(struct rmsummary *tr, pid_t pid) {
	int err;

	memset(tr, 0, sizeof(struct rmsummary));

	struct rmonitor_process_info p;
	p.pid = pid;

	err = rmonitor_poll_process_once(&p);
	if(err != 0)
		return err;

	struct rmonitor_wdir_info d;
	char cwd_link[PATH_MAX];
	char cwd_org[PATH_MAX];
	snprintf(cwd_link, PATH_MAX, "/proc/%d/cwd", pid);
	readlink(cwd_link, cwd_org, PATH_MAX);

	d.path = cwd_org;
	d.state = NULL;

	err = rmonitor_poll_wd_once(&d, -1);
	if(err != 0)
		return err;

	uint64_t start;
	err = rmonitor_get_start_time(pid, &start);
	if(err != 0)
		return err;

	rmonitor_info_to_rmsummary(tr, &p, &d, NULL, start);
	tr->command = rmonitor_get_command_line(pid);

	return 0;
}

int rmonitor_measure_process_update_to_peak(struct rmsummary *tr, pid_t pid) {

	struct rmsummary now;
	int err = rmonitor_measure_process(&now, pid);

	if(err != 0)
		return err;

	rmsummary_merge_max(tr, &now);

	return 0;
}

/* vim: set noexpandtab tabstop=4: */
