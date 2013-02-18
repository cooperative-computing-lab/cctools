/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/* Monitors a set of programs for CPU time, memory and
 * disk utilization. The monitor works 'indirectly', that is, by
 * observing how the environment changed while a process was
 * running, therefore all the information reported should be
 * considered just as an estimate (this is in contrast with
 * direct methods, such as ptrace).
 *
 * Use as:
 *
 * resource_monitor -i 120 some-command-line
 *
 * to monitor some-command-line at two minutes intervals.
 *
 * Each monitor taget resource has three functions:
 * get_RESOURCE_usage, hdr_RESOURCE_usage, and
 * log_RESOURCE_usage. For example, for memory we have
 * get_mem_usage, hdr_mem_usage, and log_mem_usage. In general,
 * all functions return 0 on success, or some other integer on
 * failure. The exception are function that open files, which
 * return NULL on failure, or a file pointer on success.
 *
 * The get_RESOURCE_usage functions are called at given intervals.
 * Between each interval, the monitor does nothing. All processes
 * monitored write to the same text log file. If no file name is provided,
 * for the log, then the log file is written to a file called
 * log-monitor-PID, in which PID is the pid of the monitor.
 *
 * log_RESOURCE_usage writes the corresponding information to the
 * log. Each field is separated by \t. There are three kind of
 * rows, one for processes, one for working directories, and one
 * for filesystems. The functions hdr_RESOURCE_usage are called
 * only once, and write the order of the columns, per type of
 * row, at the beginning of the log file.
 *
 * Currently, the columns are:
 * 
 * id: One of P (process), D (working directory), F (filesystem)
 * wall:   Wall time (in clicks).
 *
 * Subsequently, For id P:
 *
 * pid:    Pid of the process
 * user:   Time the process has spent in user mode (in clicks).
 * kernel: Time the process has spent in kernel mode (in clicks).
 * vmem:   Current total virtual memory size.
 * rssmem  Current total resident memory size.
 * shmem   Amount of shared memory.
 * rchars  Read char count using *read system calls.
 * wchars  Writen char count using *write system calls.
 * wd      Pathname of the working directory.
 * fs      Filesystem id of the working directory
 *
 * For id D:
 * 
 * wd      Pathname of the directory.
 * files   File count of the directory.
 * dirs    Directory count of the directory.
 * bytes   Total byte count of files in the directory.
 * blks    Block count (512 bytes) in the directory.
 *
 * For id F:
 *
 * devid   Filesystem id
 * frBlks  Free blocks of the filesystem. 
 * AvBlks  Available blocks of the filesystem. 
 * frNodes Free nodes of the filesystem. 
 *
 * The log file is written to the home directory of the monitor
 * process. A flag will be added later to indicate a prefered
 * output file.
 *
 * While all the logic supports the monitoring of several
 * processes by the same monitor, only one monitor can
 * be specified at the command line. This is because we plan to
 * wrap the calls to fork and clone in the monitor such that we
 * can also monitor the process children.
 *
 * Each monitored process gets a 'struct process_info', itself
 * composed of 'struct mem_info', 'struct click_info', etc. There
 * is a global variable, 'processes', that keeps a table relating pids to
 * the corresponding struct process_info.
 *
 * Likewise, there are tables that relate paths to 'struct
 * working_dir_info' ('working_dirs'), and device ids to 'struct
 * filesys_info' ('filesysms').
 *
 * The monitor program handles SIGCHLD, by either retrieving the
 * last usage of the child (getrusage through waitpid) and
 * removing it from the table above described, or logging SIGSTOP
 * and SIGCONT. On SIGINT, the monitor sends the sigint signal to
 * the first processes it created, and cleans up the monitoring
 * tables.
 *
 * monitor takes the -i<seconds> flag, which indicates how often
 * the resources are checked. The logic is there to allow, say,
 * memory to be checked twice as often as disk, but right now all
 * the resources are checked at each interval.
 *
 */

/* BUGS:
 *
 * BSDs: kvm interface is not implemented.
 *
 * io: may report zero if process ends before we read
 * /proc/[pid]/io.
 *
 * statfs: always reports the same numbers in AFS.
 * statfs: Called in current working directory. A process might
 * be writting in a different filesystem.
 *
 * If the process writes something outside the working directory,
 * right now we are out of luck.
 *
 * If processes share the same working directory, or the same
 * filesystem, then we are duplicating the most expensive checks.
 *
 */

#include "hash_table.h"
#include "itable.h"
#include "stringtools.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <limits.h>
#include <fts.h>

#include <sys/select.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/stat.h>

#include <inttypes.h>
#include <sys/types.h>

#if defined(__APPLE__) || defined(__FreeBSD__)
	#include <sys/param.h>
    #include <sys/mount.h>
#else
	#include  <sys/vfs.h>
#endif


#define DEFAULT_INTERVAL 60 /* in seconds */

FILE  *log_file;                /* All monitoring information of all processes is written here. */

uint64_t clicks_initial;        /* Time at which monitoring started, in clicks. */

pid_t  first_process_pid;

struct itable *processes;       /* Maps the pid of a process to a unique struct process_info. */
struct hash_table *working_dirs; /* Maps paths to working directory structures. */
struct itable *filesysms;        /* Maps st_dev ids (from stat syscall) to filesystem structures. */
struct itable *working_dirs_rc;  /* Counts how many process_info use a working_dir_info. */
struct itable *filesys_rc;       /* Counts how many working_dir_info use a filesys_info. */

//time in clicks, no seconds:
struct click_info
{
	unsigned long int user_time;
	unsigned long int kernel_time;
};

struct mem_info
{
	uint64_t virtual; 
	uint64_t resident;
	uint64_t shared;
	uint64_t text;
	uint64_t data;
};

struct io_info
{
	uint64_t chars_read;
	uint64_t chars_written;
};

struct working_dir_info
{
	uint64_t wall_time;
	char 	*path;
	int      files;
	int      directories;
	off_t    byte_count;
	blkcnt_t block_count;

	struct filesys_info *fs;
};

struct filesys_info
{
	uint64_t        wall_time;
	int             id;
	char           *path;			// Sample path on the filesystem.
	struct statfs   disk;			// Current result of statfs call.
	struct statfs   disk_initial;   // Result of the first time we call statfs.
};

struct process_info
{
	uint64_t    wall_time;
	pid_t       pid;
	const char *cmd;
	int         running;

	struct mem_info   mem;
	struct click_info click;
	struct io_info    io;

	struct working_dir_info *wd;
};


FILE *open_log_file(const char *filename)
{
	FILE *log;
	char flog_path[PATH_MAX];	

	if(filename)
		strncpy(flog_path, filename, PATH_MAX);
	else
	{
		sprintf(flog_path, "log-monitor-%d", getpid());
		mkstemp(flog_path);
	}

	if((log = fopen(flog_path, "w")) == NULL)
	{
		debug(D_DEBUG, "could not open log file %s : %s\n", flog_path, strerror(errno));
		return NULL;
	}

	return log;
}


FILE *open_proc_file(pid_t pid, char *filename)
{
		FILE *fproc;
		char fproc_path[PATH_MAX];	
		
		sprintf(fproc_path, "/proc/%d/%s", pid, filename);

		if((fproc = fopen(fproc_path, "r")) == NULL)
		{
				debug(D_DEBUG, "could not process file %s : %s\n", fproc_path, strerror(errno));
				return NULL;
		}

		return fproc;
}

int get_fs_usage(const char *path, struct statfs *disk)
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

void log_fs_usage(struct statfs *disk, struct statfs *disk_initial)
{
	/* Free blocks . Available blocks . Free nodes */

	fprintf(log_file, "%ld\t", disk->f_bfree - disk_initial->f_bfree);
	fprintf(log_file, "%ld\t", disk->f_bavail - disk_initial->f_bavail);
	fprintf(log_file, "%ld", disk->f_ffree - disk_initial->f_ffree);
}

void hdr_fs_usage()
{
	fprintf(log_file, "frBlks\tavBlks\tfrNodes");
}

int get_wd_usage(struct working_dir_info *d)
{
	char *argv[] = {d->path, NULL};
	FTS *hierarchy;
	FTSENT *entry;

	d->files = 0;
	d->directories = 0;
	d->byte_count = 0;
	d->block_count = 0;

	hierarchy = fts_open(argv, FTS_PHYSICAL, NULL);

	if(!hierarchy)
	{
		debug(D_DEBUG, "fts_open error: %s\n", strerror(errno));
		return 1;
	}

	while( (entry = fts_read(hierarchy)) )
	{
		switch(entry->fts_info)
		{
			case FTS_D:
				d->directories++;
				break;
			case FTS_DC:
			case FTS_DP:
				break;
			case FTS_SL:
			case FTS_DEFAULT:
				d->files++;
				break;
			case FTS_F:
				d->files++;
				d->byte_count  += entry->fts_statp->st_size;
				d->block_count += entry->fts_statp->st_blocks;
				break;
			case FTS_ERR:
				debug(D_DEBUG, "fts_read error %s: %s\n", entry->fts_name, strerror(errno));
				break;
			default:
				break;
		}
	}

	fts_close(hierarchy);

	return 0;
}

void log_wd_usage(struct working_dir_info *dir)
{
	/* files . dirs . bytes . blocks */
	fprintf(log_file, "%d\t%d\t%d\t%d", dir->files, dir->directories, (int) dir->byte_count, (int) dir->block_count);

}

void hdr_wd_usage()
{
	fprintf(log_file, "files\tdirs\tbytes\tblks");
}


double timeval_to_double(struct timeval *time, struct timeval *origin)
{
	double  secs = time->tv_sec  - origin->tv_sec;
	double usecs = time->tv_usec - origin->tv_usec;

	return(secs + usecs/1000000.0);

}

int get_click_usage(pid_t pid, struct click_info *click)
{
	/* /dev/proc/[pid]/stat */
	
	FILE *fstat = open_proc_file(pid, "stat");
	if(!fstat)
	{
		return 1;
	}


	fscanf(fstat,
			"%*s" /* pid */ "%*s" /* cmd line */ "%*s" /* state */ "%*s" /* pid of parent */
			"%*s" /* group ID */ "%*s" /* session id */ "%*s" /* tty pid */ "%*s" /* tty group ID */
			"%*s" /* linux/sched.h flags */ "%*s %*s %*s %*s" /* faults */
			"%lu" /* user mode time (in clock ticks) */
			"%lu" /* kernel mode time (in clock ticks) */
			/* .... */,
			&click->user_time, &click->kernel_time);


	return 0;
}

void log_click_usage(struct click_info *click)
{
	/* user . kernel . time */
	fprintf(log_file, "%ld\t%ld", click->user_time, click->kernel_time);
}

void hdr_click_usage()
{
	fprintf(log_file, "user\tkernel");
}

int get_mem_usage(pid_t pid, struct mem_info *mem)
{
	// /dev/proc/[pid]/statm: 
	// total-size resident shared-pages text unused data+stack unused
	
	FILE *fmem = open_proc_file(pid, "statm");
	if(!fmem)
		return 1;

	fscanf(fmem, 
			"%" SCNu64 " %" SCNu64 " %" SCNu64 " %" SCNu64 " %*s %" SCNu64 " %*s",
			&mem->virtual, 
			&mem->resident, 
			&mem->shared, 
			&mem->text,
			&mem->data);

	mem->shared *= sysconf(_SC_PAGESIZE); //Multiply pages by pages size.

	fclose(fmem);

	return 0;
}

void log_mem_usage(struct mem_info *mem)
{
	/* total virtual . resident . shared */
	fprintf(log_file, "%" PRIu64 "\t%" PRIu64 "\t%" PRIu64,
				mem->virtual, mem->resident, mem->shared);
}

void hdr_mem_usage()
{
	fprintf(log_file, "vmem\trssmem\tshmem");
}


int get_int_attribute(FILE *fstatus, char *attribute, uint64_t *value)
{
	char proc_attr_line[PATH_MAX];
	int not_found = 1;
	int n = strlen(attribute);

	proc_attr_line[PATH_MAX - 1] = '\0';

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

int get_io_usage(pid_t pid, struct io_info *io)
{
	// /proc/[pid]/io: if process dies before we read the file, then info is
	// lost, as if the process did not read or write any characters.

	FILE *fio = open_proc_file(pid, "io");
	int rstatus, wstatus;

	if(!fio)
		return 1;

	rstatus = get_int_attribute(fio, "rchar", &io->chars_read);
	wstatus = get_int_attribute(fio, "wchar", &io->chars_written);

	fclose(fio);

	if(rstatus || wstatus)
		return 1;
	else
		return 0;

}

void log_io_usage(struct io_info *io)
{
	/* total chars read . total chars written */
	fprintf(log_file, "%" PRIu64 "\t%" PRIu64, io->chars_read, io->chars_written);
}
		
void hdr_io_usage()
{
	fprintf(log_file, "rchars\twchars");
}

void hdr_process_log()
{
	fprintf(log_file, "wall\ttype=P\tpid");
}

void hdr_wd_log()
{
	fprintf(log_file, "wall\ttype=D\tpath");
}

void hdr_fs_log()
{
	fprintf(log_file, "wall\ttype=F\tdevid");
}

void monitor_log_hdr()
{

	fprintf(log_file, "\none second = %ld clicks\n\n", sysconf(_SC_CLK_TCK));

	hdr_process_log();
	fprintf(log_file, "\t");

	hdr_click_usage();
	fprintf(log_file, "\t");

	hdr_mem_usage();
	fprintf(log_file, "\t");

	hdr_io_usage();
	fprintf(log_file, "\n");

	hdr_wd_log();
	fprintf(log_file, "\t");

	hdr_wd_usage();
	fprintf(log_file, "\n");

	hdr_fs_log();
	fprintf(log_file, "\t");

	hdr_fs_usage();
	fprintf(log_file, "\n\n");
}

void log_wall_clicks(uint64_t clicks)
{
	fprintf(log_file, "%" PRIu64, clicks);
}

void log_process_info(struct process_info *p)
{
	log_wall_clicks(p->wall_time);
	fprintf(log_file, "\t");

	fprintf(log_file, "P\t%d\t", p->pid);

	log_click_usage(&p->click);
	fprintf(log_file, "\t");

	log_mem_usage(&p->mem);
	fprintf(log_file, "\t");

	log_io_usage(&p->io);
	fprintf(log_file, "\t");

	fprintf(log_file, "%s", p->wd->path);
	fprintf(log_file, "\t");

	fprintf(log_file, "%d", p->wd->fs->id);
	fprintf(log_file, "\n");
}

void log_filesystem_info(struct filesys_info *f)
{
	log_wall_clicks(f->wall_time);
	fprintf(log_file, "\t");

	fprintf(log_file, "F\t%d\t", f->id);

	log_fs_usage(&f->disk, &f->disk_initial);
	fprintf(log_file, "\n");
}

void log_working_dir_info(struct working_dir_info *d)
{
	log_wall_clicks(d->wall_time);
	fprintf(log_file, "\t");

	fprintf(log_file, "D\t%s\t", d->path);

	log_wd_usage(d);
	fprintf(log_file, "\n");
}

int itable_addto_count(struct itable *table, void *key, int value)
{
	int64_t count = (int64_t) itable_lookup(table, (int64_t) key);
	count += value;                              //we get 0 if lookup fails, so that's ok.

	if(count != 0)
		itable_insert(table, (int64_t) key, (void *) count);
	else
		itable_remove(table, (int64_t) key);

	return count;
}

int inc_fs_count(struct filesys_info *f)
{
	int64_t count = itable_addto_count(filesys_rc, f, 1);

	debug(D_DEBUG, "filesystem %d reference count +1, now %d references.\n", f->id, count);

	return count;
}

int dec_fs_count(struct filesys_info *f)
{
	int64_t count = itable_addto_count(filesys_rc, f, -1);

	debug(D_DEBUG, "filesystem %d reference count -1, now %d references.\n", f->id, count);

	if(count < 1)
	{
		debug(D_DEBUG, "filesystem %d is not monitored anymore.\n", f->id, count);
		free(f);	
	}

	return count;
}

int inc_wd_count(struct working_dir_info *d)
{
	int64_t count = itable_addto_count(working_dirs_rc, d, 1);

	debug(D_DEBUG, "working directory %s reference count +1, now %d references.\n", d->path, count); 

	return count;
}

int dec_wd_count(struct working_dir_info *d)
{
	int64_t count = (int64_t) itable_addto_count(working_dirs_rc, d, -1);

	debug(D_DEBUG, "working directory %s reference count -1, now %d references.\n", d->path, count); 

	if(count < 1)
	{
		debug(D_DEBUG, "working directory %s is not monitored anymore.\n", d->path);

		dec_fs_count((void *) d->fs);
		free(d->path);
		free(d);	
	}

	return count;
}

int get_device_id(char *path)
{
	struct stat dinfo;

	if(stat(path, &dinfo) != 0)
	{
		debug(D_DEBUG, "stat call on %s failed : %s\n", path, strerror(errno));
		return -1;
	}

	return dinfo.st_dev;
}

struct filesys_info *lookup_or_create_fs(struct filesys_info *previous, char *path)
{
	uint64_t dev_id = get_device_id(path);
	struct filesys_info *inventory = itable_lookup(filesysms, dev_id);

	if(!inventory) 
	{
		debug(D_DEBUG, "filesystem %d added to monitor.\n", dev_id);

		inventory = (struct filesys_info *) malloc(sizeof(struct filesys_info));
		inventory->path = xxstrdup(path);
		inventory->id   = dev_id;
		itable_insert(filesysms, dev_id, (void *) inventory);
		get_fs_usage(inventory->path, &inventory->disk_initial);
	}

	if(inventory != previous)
	{
		inc_fs_count(inventory);
		if(previous)
			dec_fs_count(previous);
	}

	return inventory;

}

struct working_dir_info *lookup_or_create_wd(struct working_dir_info *previous, char *path)
{
	struct working_dir_info *inventory = hash_table_lookup(working_dirs, path);

	if(!inventory) 
	{
		debug(D_DEBUG, "working directory %s added to monitor.\n", path);

		inventory = (struct working_dir_info *) malloc(sizeof(struct working_dir_info));
		inventory->path = xxstrdup(path);
		hash_table_insert(working_dirs, inventory->path, (void *) inventory);

		if(previous)
			inventory->fs = lookup_or_create_fs(previous->fs, inventory->path);
		else
			inventory->fs = lookup_or_create_fs(NULL, inventory->path);
	}

	if(inventory != previous)
	{
		inc_wd_count(inventory);
		if(previous)
			dec_wd_count(previous);
	}

	return inventory;
}

int get_wd(struct process_info *p)
{
	char  *symlink = string_format("/proc/%d/cwd", p->pid);
	char   target[PATH_MAX];

	memset(target, 0, PATH_MAX); //readlink does not add '\0' to its result.

	if(readlink(symlink, target, PATH_MAX - 1) < 0) 
	{
		debug(D_DEBUG, "could not read symlink to working directory %s : %s\n", symlink, strerror(errno));
		return 1;
	}

	p->wd = lookup_or_create_wd(p->wd, target);

	return 0;
}


uint64_t clicks_since_epoch()
{
	uint64_t clicks;
	struct timeval time; 

	gettimeofday(&time, NULL);

	clicks  = time.tv_sec * sysconf(_SC_CLK_TCK); 
	clicks += (time.tv_usec * sysconf(_SC_CLK_TCK))/1000000; 

	return clicks;
}

uint64_t clicks_since_launched()
{
	return (clicks_since_epoch() - clicks_initial);
}

int monitor_process_once(struct process_info *p)
{
	p->wall_time = clicks_since_launched();

	get_click_usage(p->pid, &p->click);
	get_mem_usage(p->pid, &p->mem);
	get_io_usage(p->pid, &p->io);
	get_wd(p);

	return 0;
}

int monitor_wd_once(struct working_dir_info *d)
{
	d->wall_time = clicks_since_launched();

	get_wd_usage(d);

	return 0;
}

int monitor_fs_once(struct filesys_info *f)
{
	f->wall_time = clicks_since_launched();

	get_fs_usage(f->path, &f->disk);

	return 0;
}

void monitor_final_cleanup(int signum)
{
	pid_t pid;
	struct process_info     *p = NULL;

	if(itable_lookup(processes, first_process_pid))
	{
		debug(D_DEBUG, "sending SIGINT to first process (%d).\n", first_process_pid);
		kill(first_process_pid, SIGINT);
	}

	debug(D_DEBUG, "cleaning up remaining processes.\n");
	itable_firstkey(processes);
	while(itable_nextkey(processes, (uint64_t *) &pid, (void **) &p))
	{
		monitor_process_once(p);
		log_process_info(p);
	}

}


//this is really three different functions: (need to split)
int monitor_resources(long int interval /*in seconds */)
{
	pid_t pid;
	char path[PATH_MAX];
	int dev_id;

	struct process_info     *p = NULL;
	struct working_dir_info *d = NULL;
	struct filesys_info     *f = NULL;

	struct timeval timeout;

	// Loop while there are processes to monitor.
	while(1)
	{ 
		hash_table_firstkey(working_dirs);
		while(hash_table_nextkey(working_dirs, (char **) &path, (void **) &d))
		{
			monitor_wd_once(d);
			log_working_dir_info(d);
		}

		itable_firstkey(filesysms);
		while(itable_nextkey(filesysms, (uint64_t *) &dev_id, (void **) &f))
		{
			monitor_fs_once(f);
			log_filesystem_info(f);
		}

		itable_firstkey(processes);
		while(itable_nextkey(processes, (uint64_t *) &pid, (void **) &p))
		{
			monitor_process_once(p);
			log_process_info(p);
		}
		
		/* wait for interval seconds. */
		timeout.tv_sec  = interval;
		timeout.tv_usec = 0;

		if(itable_size(processes) < 1)
			break;
		
		debug(D_DEBUG, "sleeping for: %d seconds\n", interval);
		select(0, NULL, NULL, NULL, &timeout);
	}

	return 0;
}

void wakeup_after_fork(int signum)
{
	if(signum == SIGCONT)
		signal(SIGCONT, SIG_DFL);
}

pid_t monitor_fork(void)
{
	pid_t pid;
	sigset_t set;
	void (*prev_handler)(int signum);


	pid = fork();

	prev_handler = signal(SIGCONT, wakeup_after_fork);
	sigfillset(&set);
	sigdelset(&set, SIGCONT);

	if(pid > 0)
	{
		debug(D_DEBUG, "fork %d -> %d\n", getpid(), pid);

		signal(SIGCONT, prev_handler);
		struct process_info *p = malloc(sizeof(struct process_info));
		p->pid = pid;
		p->running = 0;

		fprintf(log_file, "fork:\t%d\t%d\n", getpid(), pid);

		itable_insert(processes, p->pid, (void *) p);

		monitor_process_once(p);

		p->running = 1;
		kill(pid, SIGCONT);
	}
	else
	{
	//	sigsuspend(&set);
		signal(SIGCONT, prev_handler);
	}

	return pid;
}

	
struct process_info *spawn_first_process(const char *cmd)
{
	pid_t pid;
	pid = monitor_fork();

	fprintf(log_file, "command: %s\n", cmd);

	if(pid > 0)
	{
		first_process_pid = pid;
		close(STDIN_FILENO);
		close(STDOUT_FILENO);
		setpgid(pid, 0);
	}
	else if(pid < 0)
		fatal("fork failed: %s\n", strerror(errno));
	else //child
	{
		debug(D_DEBUG, "executing: %s\n", cmd);
		execlp("sh", "sh", "-c", cmd, (char *) NULL);
		//We get here only if execlp fails.
		fatal("error executing %s:\n", cmd, strerror(errno));
	}

	return itable_lookup(processes, pid);

}

void cleanup_process(struct process_info *p)
{
	debug(D_DEBUG, "cleaning process: %d\n", p->pid);

	dec_wd_count(p->wd);
	itable_remove(processes, p->pid);
	free(p);
}

// Return the pid of the child that sent the signal, without removing the
// waitable state.
pid_t waiting_child()
{
	pid_t pid;
	
#if defined(__APPLE__) || defined(__FreeBSD__)
	int status;
	pid = wait4(-1, &status, WSTOPPED | WCONTINUED | WNOWAIT, NULL);
#else
	siginfo_t cinfo;
	if(waitid(P_ALL, 0, &cinfo, WEXITED | WSTOPPED | WCONTINUED | WNOWAIT) == 0)
		pid = cinfo.si_pid;
	else
		return -1;
#endif

	return pid;
}

/* sigchild signal handler */
void monitor_check_child(const int signal)
{
	int status;
	pid_t pid;

	//zombie, tell us who you were!
	pid = waiting_child();

	//monitor that process once more, maybe for the last time.
	struct process_info *p = itable_lookup(processes, pid);
	monitor_process_once(p);

	//die zombie die
	waitpid(pid, &status, WNOHANG);

	if( WIFEXITED(status) )
	{
			fprintf(log_file, "\nProcess %d finished normally: %d.\n", p->pid, WEXITSTATUS(status) );
			cleanup_process(p);
	} 
	else if ( WIFSIGNALED(status) )
	{
			fprintf(log_file, "\nProcess %d terminated with signal: %s.\n", p->pid, strsignal(WTERMSIG(status)) );
			cleanup_process(p);
	} 
	else if ( WIFSTOPPED(status) )
	{
			fprintf(log_file, "\nProcess %d on hold with signal: %s.\n", p->pid, strsignal(WIFSTOPPED(status)) );
			p->running = 0;
	} 
	else if ( WIFCONTINUED(status) )
	{
			fprintf(log_file, "\nProcess %d received SIGCONT.\n", p->pid );
			p->running = 1;
	}

	//if there are no more process running, do the cleanup and
	//exit.
	if(itable_size(processes) < 1)
		monitor_final_cleanup(SIGINT);
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] command-line-and-options\n", cmd);
	fprintf(stdout, "-i <n>			Interval bewteen observations, in seconds. (default=%d)\n", DEFAULT_INTERVAL);
	fprintf(stdout, "-d <subsystem>		Enable debugging for this subsystem.\n");
	fprintf(stdout, "-o <directory>		Write logs to this directory. NOT IMPLEMENTED (default=.)\n");
}



int main(int argc, char **argv) {
	int i;
	char cmd[1024] = {'\0'};
	char c;
	uint64_t interval = DEFAULT_INTERVAL;

	debug_config(argv[0]);

	signal(SIGCHLD, monitor_check_child);
	signal(SIGINT, monitor_final_cleanup);


	while((c = getopt(argc, argv, "d:i:")) > 0)
	{
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'i':
				interval = strtoll(optarg, NULL, 10);
				if(interval < 1)
					fatal("interval cannot be set to less than one second.");
					break;
			default:
				show_help(argv[0]);
				return 1;
				break;
		}
	}
		

	processes    = itable_create(0);
	working_dirs = hash_table_create(0,0);
	filesysms    = itable_create(0);
	
	working_dirs_rc = itable_create(0);
	filesys_rc      = itable_create(0);

	//this is ugly, concatenating command and arguments
	for(i = optind; i < argc; i++)
	{
		strcat(cmd, argv[i]);
		strcat(cmd, " ");
	}
	
	log_file = open_log_file(NULL);
	monitor_log_hdr(log_file);

	clicks_initial = clicks_since_epoch();

	spawn_first_process(cmd);

	monitor_resources(interval);

	fclose(log_file);

	exit(0);

}
