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

#include "debug.h"

#include "rmonitor_poll.h"

/***
 * Helper functions
***/

#define div_round_up(a, b) (((a) + (b) - 1) / (b))

/***
 * Functions to track the whole process tree.  They call the
 * functions defined just above, accumulating the resources of
 * all the processes.
***/

void monitor_poll_all_processes_once(struct itable *processes, struct process_info *acc)
{
	uint64_t pid;
	struct process_info *p;

	bzero(acc, sizeof( struct process_info ));

	itable_firstkey(processes);
	while(itable_nextkey(processes, &pid, (void **) &p))
	{
		monitor_poll_process_once(p);

		acc_mem_usage(&acc->mem, &p->mem);
        
		acc_cpu_time_usage(&acc->cpu, &p->cpu);

		acc_sys_io_usage(&acc->io, &p->io);
		acc_map_io_usage(&acc->io, &p->io);
	}
}

void monitor_poll_all_wds_once(struct hash_table *wdirs, struct wdir_info *acc)
{
	struct wdir_info *d;
	char *path;

	bzero(acc, sizeof( struct wdir_info ));

	hash_table_firstkey(wdirs);
	while(hash_table_nextkey(wdirs, &path, (void **) &d))
	{
		monitor_poll_wd_once(d);
		acc_wd_usage(acc, d);
	}
}

void monitor_poll_all_fss_once(struct itable *filesysms, struct filesys_info *acc)
{
	struct   filesys_info *f;
	uint64_t dev_id;

	bzero(acc, sizeof( struct filesys_info ));

	itable_firstkey(filesysms);
	while(itable_nextkey(filesysms, &dev_id, (void **) &f))
	{
		monitor_poll_fs_once(f);
		acc_dsk_usage(&acc->disk, &f->disk);
	}
}


/***
 * Functions to monitor a single process, workind directory, or
 * filesystem.
 ***/

int monitor_poll_process_once(struct process_info *p)
{
	debug(D_DEBUG, "monitoring process: %d\n", p->pid);

	get_cpu_time_usage(p->pid, &p->cpu);
	get_mem_usage(p->pid, &p->mem);
	get_sys_io_usage(p->pid, &p->io);
	get_map_io_usage(p->pid, &p->io);

	return 0;
}

int monitor_poll_wd_once(struct wdir_info *d)
{
	debug(D_DEBUG, "monitoring dir %s\n", d->path);

	get_wd_usage(d);

	return 0;
}

int monitor_poll_fs_once(struct filesys_info *f)
{
	get_dsk_usage(f->path, &f->disk);

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

#if defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_FREEBSD)
        return NULL;
#endif

        sprintf(fproc_path, "/proc/%d/%s", pid, filename);

        if((fproc = fopen(fproc_path, "r")) == NULL)
        {
                debug(D_DEBUG, "could not process file %s : %s\n", fproc_path, strerror(errno));
                return NULL;
        }

        return fproc;
}

/* Parse a /proc file looking for line attribute: value */
int get_int_attribute(FILE *fstatus, char *attribute, uint64_t *value, int rewind_flag)
{
    char proc_attr_line[PATH_MAX];
    int not_found = 1;
    int n = strlen(attribute);

    if(!fstatus)
        return not_found;

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

int get_cpu_time_linux(pid_t pid, uint64_t *accum)
{
	/* /dev/proc/[pid]/stat */

	uint64_t kernel, user;

	FILE *fstat = open_proc_file(pid, "stat");
	if(!fstat)
		return 1;

	fscanf(fstat,
	       "%*s" /* pid */ "%*s" /* cmd line */ "%*s" /* state */ "%*s" /* pid of parent */
	       "%*s" /* group ID */ "%*s" /* session id */ "%*s" /* tty pid */ "%*s" /* tty group ID */
	       "%*s" /* linux/sched.h flags */ "%*s %*s %*s %*s" /* faults */
	       "%" SCNu64 /* user mode time (in clock ticks) */
	       "%" SCNu64 /* kernel mode time (in clock ticks) */
	       /* .... */,
	       &kernel, &user);

	*accum = clicks_to_usecs(kernel) + clicks_to_usecs(user); 

	fclose(fstat);

	return 0;
}

#if defined(CCTOOLS_OPSYS_FREEBSD)
int get_cpu_time_freebsd(pid_t pid, uint64_t *accum)
{
	int count;
	struct kinfo_proc *kp = kvm_getprocs(kd_fbsd, KERN_PROC_PID, pid, &count);

	if((kp == NULL) || (count < 1))
		return 1;

	/* According to ps(1): * This counts time spent handling interrupts.  We
	 * could * fix this, but it is not 100% trivial (and interrupt * time
	 * fractions only work on the sparc anyway).   XXX */

	*accum  = kp->ki_runtime;

	return 0;
}
#endif

int get_cpu_time_usage(pid_t pid, struct cpu_time_info *cpu)
{
	uint64_t accum;

	cpu->delta = 0;

#if   defined(CCTOOLS_OPSYS_LINUX)
	if(get_cpu_time_linux(pid, &accum) != 0)
		return 1;
#elif defined(CCTOOLS_OPSYS_FREEBSD)
	if(get_cpu_time_freebsd(pid, &accum) != 0)
		return 1;
#else
	return 0;
#endif

	cpu->delta       = accum  - cpu->accumulated;
	cpu->accumulated = accum;

	return 0;
}

void acc_cpu_time_usage(struct cpu_time_info *acc, struct cpu_time_info *other)
{
	acc->delta += other->delta;
}


int get_swap_linux(pid_t pid, struct mem_info *mem)
{
	FILE *fsmaps = open_proc_file(pid, "smaps");
	if(!fsmaps)
		return 1;

	/* in kB */
	uint64_t accum = 0;
	uint64_t value = 0;

	while(get_int_attribute(fsmaps, "Swap:", &value, 0) == 0)
		accum += value; 

	mem->swap = accum;

	fclose(fsmaps);

	return 0;
}

int get_mem_linux(pid_t pid, struct mem_info *mem)
{
	// /dev/proc/[pid]/status: 
    
	FILE *fmem = open_proc_file(pid, "status");
	if(!fmem)
		return 1;

	/* in kB */
	get_int_attribute(fmem, "VmPeak:", &mem->virtual,  1);
	get_int_attribute(fmem, "VmHWM:",  &mem->resident, 1);
	get_int_attribute(fmem, "VmLib:",  &mem->shared,   1);
	get_int_attribute(fmem, "VmExe:",  &mem->text,     1);
	get_int_attribute(fmem, "VmData:", &mem->data,     1);
	get_swap_linux(pid, mem);

	/* in MB */
	mem->virtual  = div_round_up(mem->virtual,  1024);
	mem->resident = div_round_up(mem->resident, 1024);
	mem->shared   = div_round_up(mem->shared,   1024);
	mem->text     = div_round_up(mem->text,     1024);
	mem->data     = div_round_up(mem->data,     1024);
	mem->swap     = div_round_up(mem->swap,     1024);

	fclose(fmem);

	return 0;
}

#if defined(CCTOOLS_OPSYS_FREEBSD)
int get_mem_freebsd(pid_t pid, struct mem_info *mem)
{
	int count;
	struct kinfo_proc *kp = kvm_getprocs(kd_fbsd, KERN_PROC_PID, pid, &count);

	if((kp == NULL) || (count < 1))
		return 1;
	
	/* in MB */
	mem->resident = kp->ki_rssize * sysconf(_SC_PAGESIZE); //Multiply pages by pages size.
	mem->virtual = kp->ki_size;

	return 0;
}
#endif

int get_mem_usage(pid_t pid, struct mem_info *mem)
{
#if   defined(CCTOOLS_OPSYS_LINUX)
	if(get_mem_linux(pid, mem) != 0)
		return 1;
#elif defined(CCTOOLS_OPSYS_FREEBSD)
	if(get_mem_freebsd(pid, mem) != 0)
		return 1;
#else
	return 0;
#endif


	return 0;
}

void acc_mem_usage(struct mem_info *acc, struct mem_info *other)
{
        acc->virtual  += other->virtual;
        acc->resident += other->resident;
        acc->shared   += other->shared;
        acc->data     += other->data;
        acc->swap     += other->swap;
}

int get_sys_io_usage(pid_t pid, struct io_info *io)
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
	rstatus  = get_int_attribute(fio, "rchar", &cread, 1);
	wstatus  = get_int_attribute(fio, "write_bytes", &cwritten, 1);

	fclose(fio);

	if(rstatus || wstatus)
		return 1;

	io->delta_chars_read    = cread    - io->chars_read;
	io->delta_chars_written = cwritten - io->chars_written;

	io->chars_read = cread;
	io->chars_written = cwritten;

	return 0;
}

void acc_sys_io_usage(struct io_info *acc, struct io_info *other)
{
	acc->delta_chars_read    += other->delta_chars_read;
	acc->delta_chars_written += other->delta_chars_written;
}

/* We compute the resident memory changes from mmap files. */
int get_map_io_usage(pid_t pid, struct io_info *io)
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
			if(get_int_attribute(fsmaps, "Rss:", &kbytes_resident, 0) == 0)
				kbytes_resident_accum += kbytes_resident;

	if((kbytes_resident_accum * 1024) > io->bytes_faulted)
		io->delta_bytes_faulted = (kbytes_resident_accum * 1024) - io->bytes_faulted;
    
	/* in bytes */
	io->bytes_faulted = (kbytes_resident_accum * 1024);

	fclose(fsmaps);

	return 0;
}

void acc_map_io_usage(struct io_info *acc, struct io_info *other)
{
	acc->delta_bytes_faulted += other->delta_bytes_faulted;
}


/***
 * Low level resource monitor functions.
 ***/

int get_dsk_usage(const char *path, struct statfs *disk)
{
	char cwd[PATH_MAX];

	debug(D_DEBUG, "statfs on path: %s\n", path);

	if(statfs(path, disk) > 0)
	{
		debug(D_DEBUG, "could statfs on %s : %s\n", cwd, strerror(errno));
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

static struct wdir_info *temporary_dir_info;

static int update_wd_usage(const char *path, const struct stat *s, int typeflag, struct FTW *f)
{
	switch(typeflag)
	{
		case FTW_D:
			temporary_dir_info->directories++;
			break;
		case FTW_DNR:
			debug(D_DEBUG, "ftw cannot read %s\n", path);
			temporary_dir_info->directories++;
			break;
		case FTW_DP:
			break;
		case FTW_F:
			temporary_dir_info->directories++;
			temporary_dir_info->byte_count  += s->st_size;
			temporary_dir_info->block_count += s->st_blocks;
			break;
		case FTW_NS:
			break;
		case FTW_SL:
			temporary_dir_info->files++;
			break;
		case FTW_SLN:
			break;
		default:
			break;
	}

	return 0;
}

int get_wd_usage(struct wdir_info *d)
{
	int result;

	d->files = 0;
	d->directories = 0;
	d->byte_count = 0;
	d->block_count = 0;

	temporary_dir_info = d;
	result = nftw(d->path, update_wd_usage, MAX_FILE_DESCRIPTOR_COUNT, FTW_PHYS);
	temporary_dir_info = NULL;

	if(result == -1) {
		debug(D_DEBUG, "ftw error\n");
		result = 1;
	}

	return result;
}

void acc_wd_usage(struct wdir_info *acc, struct wdir_info *other)
{
	acc->files       += other->files;
	acc->directories += other->directories;
	acc->byte_count  += other->byte_count;
	acc->block_count += other->block_count;
}



/* vim: set noexpandtab tabstop=4: */
