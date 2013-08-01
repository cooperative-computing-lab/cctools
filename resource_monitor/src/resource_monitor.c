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
 * resource_monitor -i 120 -- some-command-line-and-options
 *
 * to monitor some-command-line at two minutes intervals (120
 * seconds).
 *
 * Each monitor target resource has two functions:
 * get_RESOURCE_usage, and acc_RESOURCE_usage. For example, for memory we have
 * get_mem_usage, and acc_mem_usage. In general, all functions
 * return 0 on success, or some other integer on failure. The
* exception are function that open files, which return NULL on
 * failure, or a file pointer on success.
 *
 * The acc_RESOURCE_usage(accum, other) adds the contents of
 * other, field by field, to accum.
 *
 * monitor_CATEGORY_summary writes the corresponding information
 * to the log. CATEGORY is one of process, working directory of
 * filesystem. Each field is separated by \t.
 *
 * Currently, the columns are:
 * 
 * wall:          wall time (in usecs).
 * no.proc:       number of processes
 * cpu-time:      user-mode time + kernel-mode time.
 * vmem:          current total memory size (virtual).
 * rss:           current total resident size.
 * swap:          current total swap usage.
 * bytes_read:    read chars count using *read system calls
 * bytes_written: writen char count using *write system calls.
 * files+dir      total file + directory count of all working directories.
 * footprint      total byte count of all working directories.
 *
 * The log file is written to the home directory of the monitor
 * process. A flag will be added later to indicate a prefered
 * output file. Additionally, a summary log file is written at
 * the end, reporting the command run, starting and ending times,
 * and maximum, of the resources monitored.
 *
 * Each monitored process gets a 'struct process_info', itself
 * composed of 'struct mem_info', 'struct cpu_time_info', etc. There
 * is a global variable, 'processes', that keeps a table relating pids to
 * the corresponding struct process_info.
 *
 * Likewise, there are tables that relate paths to 'struct
 * wdir_info' ('wdirs'), and device ids to 'struct
 * filesys_info' ('filesysms').
 *
 * The process tree is summarized from the struct *_info into
 * struct rmsummary. For each time interval there are three
 * struct rmsummary: current, maximum, and minimum. 
 *
 * Grandchildren processes are tracked via the helper library,
 * which wraps the family of fork functions.
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
 * LOTS of code repetition that probably can be eliminated with
 * calls to function pointers and some macros.
 *
 * BSDs: kvm interface for swap is not implemented.
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
 * For /a/b, if a and b are working directories of two different
 * processes, then b usage is logged twice.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <limits.h>

#include <sys/select.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/times.h>

#include <inttypes.h>
#include <sys/types.h>

#include "cctools.h"
#include "hash_table.h"
#include "itable.h"
#include "list.h"
#include "stringtools.h"
#include "debug.h"
#include "xxmalloc.h"
#include "copy_stream.h"
#include "getopt.h"
#include "create_dir.h"

#include "rmonitor.h"
#include "rmonitor_poll.h"

#define RESOURCE_MONITOR_USE_INOTIFY 1
#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
#include <sys/inotify.h>
#include <sys/ioctl.h>
#endif

#include "rmonitor_helper_comm.h"
#include "rmonitor_piggyback.h"

#define RESOURCES_EXCEEDED_EXIT_CODE 147

#define ONE_MEGABYTE 1048576  /* this many bytes */
#define ONE_SECOND   1000000  /* this many usecs */    

#define DEFAULT_INTERVAL       ONE_SECOND        /* in useconds */

#define DEFAULT_LOG_NAME "resource-pid-%d"     /* %d is used for the value of getpid() */

FILE  *log_summary = NULL;      /* Final statistics are written to this file. */
FILE  *log_series  = NULL;      /* Resource events and samples are written to this file. */
FILE  *log_opened  = NULL;      /* List of opened files is written to this file. */

int    monitor_queue_fd = -1;  /* File descriptor of a datagram socket to which (great)
                                  grandchildren processes report to the monitor. */
static int monitor_inotify_fd = -1;

pid_t  first_process_pid;              /* pid of the process given at the command line */
pid_t  first_process_sigchild_status;  /* exit status flags of the process given at the command line */
pid_t  first_process_already_waited = 0;  /* exit status flags of the process given at the command line */

struct itable *processes;       /* Maps the pid of a process to a unique struct process_info. */
struct hash_table *wdirs;       /* Maps paths to working directory structures. */
struct itable *filesysms;       /* Maps st_dev ids (from stat syscall) to filesystem structures. */
struct hash_table *files;       /* Keeps track of which files have been opened. */

#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
static char **inotify_watches;  /* Keeps track of created inotify watches. */
static int alloced_inotify_watches = 0;
#endif

struct itable *wdirs_rc;        /* Counts how many process_info use a wdir_info. */
struct itable *filesys_rc;      /* Counts how many wdir_info use a filesys_info. */


char *lib_helper_name = NULL;  /* Name of the helper library that is 
                                  automatically extracted */

int lib_helper_extracted;       /* Boolean flag to indicate whether the bundled
                                   helper library was automatically extracted
                                   */ 

#if defined(CCTOOLS_OPSYS_FREEBSD)
kvm_t *kd_fbsd;
#endif

struct rmsummary *summary;
struct rmsummary *resources_limits;
struct rmsummary *resources_flags;

/*** 
 * Utility functions (open log files, proc files, measure time)
 ***/

uint64_t usecs_since_epoch()
{
    uint64_t usecs;
    struct timeval time; 

    gettimeofday(&time, NULL);

    usecs  = time.tv_sec * ONE_SECOND;
    usecs += time.tv_usec;

    return usecs;
}

uint64_t usecs_since_launched()
{
	return (usecs_since_epoch() - summary->start);
}

char *default_summary_name(char *template_path)
{
	if(template_path)
		return string_format("%s.summary", template_path);
	else
		return string_format(DEFAULT_LOG_NAME ".summary", getpid());
}

char *default_series_name(char *template_path)
{
    if(template_path)
        return string_format("%s.series", template_path);
    else
        return string_format(DEFAULT_LOG_NAME ".series", getpid());
}

char *default_opened_name(char *template_path)
{
    if(template_path)
        return string_format("%s.files", template_path);
    else
        return string_format(DEFAULT_LOG_NAME ".files", getpid());
}

FILE *open_log_file(const char *log_path)
{
    FILE *log_file;
    char *dirname;

    if(log_path)
    {
        dirname = xxstrdup(log_path);
        string_dirname(log_path, dirname);
        if(!create_dir(dirname, 0755))
            fatal("could not create directory %s : %s\n", dirname, strerror(errno));

        if((log_file = fopen(log_path, "w")) == NULL)
            fatal("could not open log file %s : %s\n", log_path, strerror(errno));

        free(dirname);
    }
    else
	    return NULL;

    return log_file;
}

void parse_limits_string(struct rmsummary *limits, char *str)
{
	struct rmsummary *s;
	s = rmsummary_parse_single(str, ',');

	rmsummary_merge_override(limits, s);

	free(s);
}

void parse_limits_file(struct rmsummary *limits, char *path)
{
	struct rmsummary *s;
	s = rmsummary_parse_file_single(path);

	rmsummary_merge_override(limits, s);

	free(s);
}

/***
 * Reference count for filesystems and working directories auxiliary functions.
 ***/

int itable_addto_count(struct itable *table, void *key, int value)
{
    uintptr_t count = (uintptr_t) itable_lookup(table, (uintptr_t) key);
    count += value;                              //we get 0 if lookup fails, so that's ok.

    if(count > 0)
        itable_insert(table, (uintptr_t) key, (void *) count);
    else
        itable_remove(table, (uintptr_t) key);

    return count;
}

int inc_fs_count(struct filesys_info *f)
{
    int count = itable_addto_count(filesys_rc, f, 1);

    debug(D_DEBUG, "filesystem %d reference count +1, now %d references.\n", f->id, count);

    return count;
}

int dec_fs_count(struct filesys_info *f)
{
    int count = itable_addto_count(filesys_rc, f, -1);

    debug(D_DEBUG, "filesystem %d reference count -1, now %d references.\n", f->id, count);

    if(count < 1)
    {
        debug(D_DEBUG, "filesystem %d is not monitored anymore.\n", f->id, count);
        free(f->path);
        free(f);    
    }

    return count;
}

int inc_wd_count(struct wdir_info *d)
{
    int count = itable_addto_count(wdirs_rc, d, 1);

    debug(D_DEBUG, "working directory '%s' reference count +1, now %d references.\n", d->path, count); 

    return count;
}

int dec_wd_count(struct wdir_info *d)
{
    int count = itable_addto_count(wdirs_rc, d, -1);

    debug(D_DEBUG, "working directory '%s' reference count -1, now %d references.\n", d->path, count); 

    if(count < 1)
    {
        debug(D_DEBUG, "working directory '%s' is not monitored anymore.\n", d->path);

        hash_table_remove(wdirs, d->path); 

        dec_fs_count((void *) d->fs);
        free(d->path);
        free(d);    
    }

    return count;
}

/***
 * Functions to track a working directory, or filesystem.
 ***/

int get_device_id(char *path)
{
    struct stat dinfo;

    if(stat(path, &dinfo) != 0)
    {
        debug(D_DEBUG, "stat call on '%s' failed : %s\n", path, strerror(errno));
        return -1;
    }

    return dinfo.st_dev;
}

struct filesys_info *lookup_or_create_fs(char *path)
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
        get_dsk_usage(inventory->path, &inventory->disk_initial);
    }

    inc_fs_count(inventory);

    return inventory;
}

struct wdir_info *lookup_or_create_wd(struct wdir_info *previous, char *path)
{
    struct wdir_info *inventory; 

    if(strlen(path) < 1 || access(path, F_OK) != 0)
        return previous;

    inventory = hash_table_lookup(wdirs, path);

    if(!inventory) 
    {
        debug(D_DEBUG, "working directory '%s' added to monitor.\n", path);

        inventory = (struct wdir_info *) malloc(sizeof(struct wdir_info));
        inventory->path = xxstrdup(path);
        hash_table_insert(wdirs, inventory->path, (void *) inventory);

        inventory->fs = lookup_or_create_fs(inventory->path);
    }

    if(inventory != previous)
    {
        inc_wd_count(inventory);
        if(previous)
            dec_wd_count(previous);
    }

    debug(D_DEBUG, "filesystem of %s is %d\n", inventory->path, inventory->fs->id);

    return inventory;
}

/***
 * Logging functions. The process tree is summarized in struct
 * rmsummary's, computing current value, maximum, and minimums.
***/

void monitor_summary_header()
{
    if(log_series)
    {
	    fprintf(log_series, "# Units:\n");
	    fprintf(log_series, "# wall_clock and cpu_time in microseconds\n");
	    fprintf(log_series, "# virtual, resident and swap memory in megabytes.\n");
	    fprintf(log_series, "# footprint in megabytes.\n");
	    fprintf(log_series, "# cpu_time, bytes_read, and bytes_written show cummulative values.\n");
	    fprintf(log_series, "# wall_clock, max_concurrent_processes, virtual, resident, swap, files, and footprint show values at the sample point.\n");

	    fprintf(log_series, "#");
	    fprintf(log_series,  "%-20s", "wall_clock");
	    fprintf(log_series, " %20s", "cpu_time");
	    fprintf(log_series, " %25s", "max_concurrent_processes");
	    fprintf(log_series, " %25s", "virtual_memory");
	    fprintf(log_series, " %25s", "resident_memory");
	    fprintf(log_series, " %25s", "swap_memory");
	    fprintf(log_series, " %25s", "bytes_read");
	    fprintf(log_series, " %25s", "bytes_written");

	    if(resources_flags->workdir_footprint)
	    {
		    fprintf(log_series, " %25s", "workdir_num_files");
		    fprintf(log_series, " %25s", "workdir_footprint");
	    }

	    fprintf(log_series, "\n");
    }
}

void monitor_collate_tree(struct rmsummary *tr, struct process_info *p, struct wdir_info *d, struct filesys_info *f)
{
	tr->wall_time         = usecs_since_epoch() - summary->start;

	tr->max_concurrent_processes     = (int64_t) itable_size(processes);
	tr->total_processes     = summary->total_processes;

	tr->cpu_time          = p->cpu.delta + tr->cpu_time;
	tr->virtual_memory    = (int64_t) p->mem.virtual;
	tr->resident_memory   = (int64_t) p->mem.resident;
	tr->swap_memory       = (int64_t) p->mem.swap;

	tr->bytes_read        = (int64_t) (p->io.delta_chars_read + tr->bytes_read);
	tr->bytes_read       += (int64_t)  p->io.delta_bytes_faulted;
	tr->bytes_written     = (int64_t) (p->io.delta_chars_written + tr->bytes_written);

	tr->workdir_num_files = (int64_t) (d->files + d->directories);
	tr->workdir_footprint = (int64_t) (d->byte_count + ONE_MEGABYTE - 1) / ONE_MEGABYTE;

	tr->fs_nodes          = (int64_t) f->disk.f_ffree;
}

void monitor_find_max_tree(struct rmsummary *result, struct rmsummary *tr)
{
    if(!tr)
        return;

    rmsummary_merge_max(result, tr);
}

void monitor_log_row(struct rmsummary *tr)
{
	if(log_series)
	{
		fprintf(log_series,  "%-20" PRId64, tr->wall_time + summary->start);
		fprintf(log_series, " %20" PRId64, tr->cpu_time);
		fprintf(log_series, " %25" PRId64, tr->max_concurrent_processes);
		fprintf(log_series, " %25" PRId64, tr->virtual_memory);
		fprintf(log_series, " %25" PRId64, tr->resident_memory);
		fprintf(log_series, " %25" PRId64, tr->swap_memory);
		fprintf(log_series, " %25" PRId64, tr->bytes_read);
		fprintf(log_series, " %25" PRId64, tr->bytes_written);

		if(resources_flags->workdir_footprint)
		{
			fprintf(log_series, " %25" PRId64, tr->workdir_num_files);
			fprintf(log_series, " %25" PRId64, tr->workdir_footprint);
		}

		fprintf(log_series, "\n");
                               
		/* are we going to keep monitoring the whole filesystem? */
		// fprintf(log_series "%" PRId64 "\n", tr->fs_nodes);
	}
}

void decode_zombie_status(struct rmsummary *summary, int wait_status)
{
	if( WIFEXITED(wait_status) )
	{
		debug(D_DEBUG, "process %d finished: %d.\n", first_process_pid, WEXITSTATUS(wait_status));
		summary->exit_type = xxstrdup("normal");
		summary->exit_status = WEXITSTATUS(first_process_sigchild_status);
	} 
	else if ( WIFSIGNALED(wait_status) || WIFSTOPPED(wait_status) )
	{
		debug(D_DEBUG, "process %d terminated: %s.\n",
		      first_process_pid,
		      strsignal(WIFSIGNALED(wait_status) ? WTERMSIG(wait_status) : WSTOPSIG(wait_status)));

		summary->exit_type = xxstrdup("signal");
		summary->exit_status = -1;

		if(WIFSIGNALED(wait_status))
			summary->signal    = WTERMSIG(wait_status);
		else
			summary->signal    = WSTOPSIG(wait_status);
	} 

	if(summary->limits_exceeded)
	{
		free(summary->exit_type);
		summary->exit_type = xxstrdup("limits");
		summary->exit_status = RESOURCES_EXCEEDED_EXIT_CODE;
	}

}

int monitor_file_io_summaries()
{
#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
	if (monitor_inotify_fd >= 0)
	{
		char *fname;
		struct stat buf;
		struct file_info *finfo;

		fprintf(log_opened, "%-15s\n%-15s %6s %20s %20s %6s %6s %6s %6s\n",
			"#path", "#", "device", "size_initial(B)", "size_final(B)", "opens", "closes", "reads", "writes");

		hash_table_firstkey(files);
		while(hash_table_nextkey(files, &fname, (void **) &finfo))
		{
			/* If size_on_close is unknwon, perform a stat on the file. */

			if(finfo->size_on_close < 0 && stat(fname, &buf) == 0)
				finfo->size_on_close = buf.st_size;
			
			fprintf(log_opened, "%-15s\n%-15s ", fname, "");
			fprintf(log_opened, "%6" PRId64 " %20lld %20lld",
				finfo->device,
				(long long int) finfo->size_on_open,
				(long long int) finfo->size_on_close);
			fprintf(log_opened, " %6" PRId64 " %6" PRId64,
				finfo->n_opens,
				finfo->n_closes);
			fprintf(log_opened, " %6" PRId64 " %6" PRId64 "\n",
				finfo->n_reads,
				finfo->n_writes);
		}
	}
#endif
	return 0;
}

int monitor_final_summary()
{
	decode_zombie_status(summary, first_process_sigchild_status);

	summary->end       = usecs_since_epoch();
	summary->wall_time = summary->end - summary->start;

	if(summary->exit_status == 0 && summary->limits_exceeded)
		summary->exit_status = RESOURCES_EXCEEDED_EXIT_CODE;
	
	if(log_summary)
		rmsummary_print(log_summary, summary);

	if(log_opened)
		monitor_file_io_summaries();

	return summary->exit_status;
}

void monitor_inotify_add_watch(char *filename)
{
	/* Perhaps here we can do something more to the files, like a
	 * final stat */
	
#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
	struct file_info *finfo;
	char **new_inotify_watches;
	struct stat fst;
	int iwd;

	finfo = hash_table_lookup(files, filename);
	if (finfo)
	{
		(finfo->n_references)++;
		(finfo->n_opens)++;
		return;
	}

	finfo = calloc(1, sizeof(struct file_info));
	if (finfo != NULL)
	{
		finfo->n_opens = 1;
		finfo->size_on_open= -1;
		finfo->size_on_close = -1;
		if (stat(filename, &fst) >= 0)
		{
			finfo->size_on_open  = fst.st_size;
			finfo->device        = fst.st_dev;
		}
	}

	hash_table_insert(files, filename, finfo);
	if (monitor_inotify_fd >= 0)
	{
		if ((iwd = inotify_add_watch(monitor_inotify_fd, 
					     filename, 
					     IN_CLOSE_WRITE|IN_CLOSE_NOWRITE|
					     IN_ACCESS|IN_MODIFY)) < 0)
		{
			debug(D_DEBUG, "inotify_add_watch for file %s fails: %s", filename, strerror(errno));
		} else {
			debug(D_DEBUG, "added watch (id: %d) for file %s", iwd, filename);
			if (iwd >= alloced_inotify_watches)
			{
				new_inotify_watches = (char **)realloc(inotify_watches, (iwd+50) * (sizeof(char *)));
				if (new_inotify_watches != NULL)
				{
					alloced_inotify_watches = iwd+50;
					inotify_watches = new_inotify_watches;
				} else {
					debug(D_DEBUG, "Out of memory trying to expand inotify_watches");
				}
			}
			if (iwd < alloced_inotify_watches)
			{
				inotify_watches[iwd] = strdup(filename);
				if (finfo != NULL) finfo->n_references = 1;
			} else {
				debug(D_DEBUG, "Out of memory: Removing inotify watch for %s", filename);
				inotify_rm_watch(monitor_inotify_fd, iwd);
			}
		} 
	}
#endif
}

/***
 * Functions that modify the processes tracking table, and
 * cleanup of processes in the zombie state.
 ***/

int ping_process(pid_t pid)
{
    return (kill(pid, 0) == 0);
}

void monitor_track_process(pid_t pid)
{
	char *newpath;
	struct process_info *p; 
    
	if(!ping_process(pid))
		return;

	p = itable_lookup(processes, pid);

	if(p)
		return;

	p = malloc(sizeof(struct process_info));
	bzero(p, sizeof(struct process_info));

	p->pid = pid;
	p->running = 0;

	newpath = getcwd(NULL, 0);
	p->wd   = lookup_or_create_wd(NULL, newpath);
	free(newpath);

	itable_insert(processes, p->pid, (void *) p);

	p->running = 1;
	p->waiting = 0;

	summary->total_processes++;
}

void monitor_untrack_process(uint64_t pid)
{
	struct process_info *p = itable_lookup(processes, pid);

	if(p)
		p->running = 0;
}

void cleanup_zombie(struct process_info *p)
{
  debug(D_DEBUG, "cleaning process: %d\n", p->pid);

  if(p->wd)
    dec_wd_count(p->wd);

  kill(p->pid, SIGTERM);

  itable_remove(processes, p->pid);
  free(p);
}

void cleanup_zombies(void)
{
  uint64_t pid;
  struct process_info *p;

  itable_firstkey(processes);
  while(itable_nextkey(processes, &pid, (void **) &p))
    if(!p->running)
      cleanup_zombie(p);
}

void release_waiting_process(pid_t pid)
{
	kill(pid, SIGCONT);
}

void release_waiting_processes(void)
{
	uint64_t pid;
	struct process_info *p;

	itable_firstkey(processes);
	while(itable_nextkey(processes, &pid, (void **) &p))
		if(p->waiting)
			release_waiting_process(pid);
}

void ping_processes(void)
{
    uint64_t pid;
    struct process_info *p;

    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
        if(!ping_process(pid))
        {
            debug(D_DEBUG, "cannot find %d process.\n", pid);
            monitor_untrack_process(pid);
        }
}

struct rmsummary *monitor_rusage_tree(void)
{
    struct rusage usg;
    struct rmsummary *tr_usg = calloc(1, sizeof(struct rmsummary));

    debug(D_DEBUG, "calling getrusage.\n");

    if(getrusage(RUSAGE_CHILDREN, &usg) != 0)
    {
        debug(D_DEBUG, "getrusage failed: %s\n", strerror(errno));
        return NULL;
    }

    /* Here we add the maximum recorded + the io from memory maps */
    tr_usg->bytes_read     =  summary->bytes_read + usg.ru_majflt * sysconf(_SC_PAGESIZE);

    tr_usg->resident_memory = (usg.ru_maxrss + ONE_MEGABYTE - 1) / ONE_MEGABYTE;

    debug(D_DEBUG, "rusage faults: %d resident memory: %d.\n", usg.ru_majflt, usg.ru_maxrss);

    return tr_usg;
}

/* sigchild signal handler */
void monitor_check_child(const int signal)
{
    uint64_t pid = waitpid(first_process_pid, &first_process_sigchild_status, 
                           WNOHANG | WCONTINUED | WUNTRACED);

    if(pid != (uint64_t) first_process_pid)
	    return;

    debug(D_DEBUG, "SIGCHLD from %d : ", first_process_pid);

    if(WIFEXITED(first_process_sigchild_status))
    {
        debug(D_DEBUG, "exit\n");
    }
    else if(WIFSIGNALED(first_process_sigchild_status))
    {
      debug(D_DEBUG, "signal\n");
    }
    else if(WIFSTOPPED(first_process_sigchild_status))
    {
      debug(D_DEBUG, "stop\n");

      switch(WSTOPSIG(first_process_sigchild_status))
      {
        case SIGTTIN: 
          debug(D_NOTICE, "Process asked for input from the terminal, try the -f option to bring the child process in foreground.\n");
          break;
        case SIGTTOU:
          debug(D_NOTICE, "Process wants to write to the standard output, but the current terminal settings do not allow this. Please try the -f option to bring the child process in foreground.\n");
          break;
        default:
          return;
          break;
      }
    }
    else if(WIFCONTINUED(first_process_sigchild_status))
    {
      debug(D_DEBUG, "continue\n");
      return;
    }

    first_process_already_waited = 1;

    struct process_info *p;
    debug(D_DEBUG, "adding all processes to cleanup list.\n");
    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
      monitor_untrack_process(pid);

    /* get the peak values from getrusage */
    struct rmsummary *tr_usg = monitor_rusage_tree();
    monitor_find_max_tree(summary, tr_usg);
    free(tr_usg);
}

//SIGINT, SIGQUIT, SIGTERM signal handler.
void monitor_final_cleanup(int signum)
{
    uint64_t pid;
    struct   process_info *p;
    int      status;


    //ask politely to quit
    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
    {
        debug(D_DEBUG, "sending %s to process %d.\n", strsignal(signum), pid);

        kill(pid, signum);
    }

    ping_processes();
    cleanup_zombies();

    if(itable_size(processes) > 0)
        sleep(5);

    if(!first_process_already_waited)
	    monitor_check_child(signum);

    signal(SIGCHLD, SIG_DFL);

    //we did ask...
    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
    {
        debug(D_DEBUG, "sending %s to process %d.\n", strsignal(SIGKILL), pid);

        kill(pid, SIGKILL);

        monitor_untrack_process(pid);
    }

    cleanup_zombies();

    if(lib_helper_extracted)
        unlink(lib_helper_name);

    status = monitor_final_summary();

    if(log_summary)
	    fclose(log_summary);
    if(log_series)
	    fclose(log_series);
    if(log_opened)
	    fclose(log_opened);

    exit(status);
}

// The following keeps getting uglier and uglier! Rethink how to do it!
//
#define over_limit_check(tr, fld, mult, fmt)				\
	if(resources_limits->fld > -1 && (tr)->fld > 0 && resources_limits->fld - (tr)->fld < 0)\
	{								\
		debug(D_DEBUG, "Limit " #fld " broken.\n");		\
		char *tmp;						\
		if((tr)->limits_exceeded)                               \
		{							\
			tmp = string_format("%s, " #fld ": %" fmt, (tr)->limits_exceeded, mult * resources_limits->fld); \
			free((tr)->limits_exceeded);			\
			(tr)->limits_exceeded = tmp;			\
		}							\
		else							\
			(tr)->limits_exceeded = string_format(#fld ": %" fmt, mult * resources_limits->fld); \
	}

/* return 0 means above limit, 1 means limist ok */
int monitor_check_limits(struct rmsummary *tr)
{
	tr->limits_exceeded = NULL;

	if(!resources_limits)
		return 1;

	over_limit_check(tr, start, 1.0/ONE_SECOND, "lf");
	over_limit_check(tr, end,   1.0/ONE_SECOND, "lf");
	over_limit_check(tr, wall_time, 1.0/ONE_SECOND, "lf");
	over_limit_check(tr, cpu_time,  1.0/ONE_SECOND, "lf");
	over_limit_check(tr, max_concurrent_processes,   1, PRId64);
	over_limit_check(tr, total_processes,   1, PRId64);
	over_limit_check(tr, virtual_memory,  1, PRId64);
	over_limit_check(tr, resident_memory, 1, PRId64);
	over_limit_check(tr, swap_memory,     1, PRId64);
	over_limit_check(tr, bytes_read,      1, PRId64);
	over_limit_check(tr, bytes_written,   1, PRId64);
	over_limit_check(tr, workdir_num_files, 1, PRId64);
	over_limit_check(tr, workdir_footprint, 1, PRId64);

	if(tr->limits_exceeded)
		return 0;
	else
		return 1;
}

/***
 * Functions that communicate with the helper library,
 * (un)tracking resources as messages arrive.
***/

void write_helper_lib(void)
{    
    uint64_t n;

    lib_helper_name = xxstrdup("librmonitor_helper.so.XXXXXX");

    if(access(lib_helper_name, R_OK | X_OK) == 0)
    {
        lib_helper_extracted = 0;
        return;
    }

    int flib = mkstemp(lib_helper_name);
    if(flib == -1)
        return;

    n = sizeof(lib_helper_data);
    write(flib, lib_helper_data, n);
    close(flib);

    chmod(lib_helper_name, 0777);

    lib_helper_extracted = 1;
}

void monitor_handle_inotify(void)
{
#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
	struct inotify_event *evdata;
	struct file_info *finfo;
	struct stat fst;
	char *fname;
	int nbytes, evc, i;
	if (monitor_inotify_fd >= 0)
	{
		if (ioctl(monitor_inotify_fd, FIONREAD, &nbytes) >= 0)
		{

			evdata = (struct inotify_event *) malloc(nbytes);
			if (evdata == NULL) return;
			if (read(monitor_inotify_fd, evdata, nbytes) != nbytes)
			{
				free(evdata);
				return;
			}
			evc = nbytes/sizeof(*evdata);
			for(i = 0; i < evc; i++)
			{
				if (evdata[i].wd >= alloced_inotify_watches) continue;
				if ((fname = inotify_watches[evdata[i].wd]) == NULL) continue;
				finfo = hash_table_lookup(files, fname);
				if (finfo == NULL) continue;
				if (evdata[i].mask & IN_ACCESS) (finfo->n_reads)++;
				if (evdata[i].mask & IN_MODIFY) (finfo->n_writes)++;
				if ((evdata[i].mask & IN_CLOSE_WRITE) || 
				    (evdata[i].mask & IN_CLOSE_NOWRITE))
				{
					(finfo->n_closes)++;
					if (stat(fname, &fst) >= 0)
					{
						finfo->size_on_close = fst.st_size;
					}
					/* Decrease reference count and remove watch of zero */
					(finfo->n_references)--;
					if (finfo->n_references == 0)
					{
						inotify_rm_watch(monitor_inotify_fd, evdata[i].wd);
						debug(D_DEBUG, "removed watch (id: %d) for file %s", evdata[i].wd, fname);
						free(fname);
						inotify_watches[evdata[i].wd] = NULL;
					}
				}
			}
		}
		free(evdata);
	}
#endif
}

void monitor_dispatch_msg(void)
{
	struct monitor_msg msg;
	struct process_info *p;

	recv_monitor_msg(monitor_queue_fd, &msg);

	debug(D_DEBUG,"message \"%s\" from %d\n", str_msgtype(msg.type), msg.origin);

	p = itable_lookup(processes, (uint64_t) msg.origin);

	if(!p)
	{
		/* We either got a malformed message, message from a
		process we are not tracking anymore, or a message from
		a newly created process.  */
		if( msg.type == END_WAIT )
        {
			release_waiting_process(msg.origin);
			return;
        }
		else if(msg.type != BRANCH)
			return;
	}

	switch(msg.type)
	{
        case BRANCH:
		monitor_track_process(msg.origin);
		if(summary->max_concurrent_processes < itable_size(processes))
			summary->max_concurrent_processes = itable_size(processes);
		break;
        case END_WAIT:
		p->waiting = 1;
		break;
        case END:
		monitor_untrack_process(msg.data.p);
		break;
        case CHDIR:
		p->wd = lookup_or_create_wd(p->wd, msg.data.s);
		break;
        case OPEN:
		debug(D_DEBUG, "File %s has been opened.\n", msg.data.s);
		monitor_inotify_add_watch(msg.data.s);
		break;
        case READ:
		break;
        case WRITE:
		break;
        default:
		break;
	};

	if(!monitor_check_limits(summary))
		monitor_final_cleanup(SIGTERM);

}

int wait_for_messages(int interval)
{
    struct timeval timeout;

    /* wait for interval. */
    timeout.tv_sec  = 0;
    timeout.tv_usec = interval;

    debug(D_DEBUG, "sleeping for: %lf seconds\n", ((double) interval / ONE_SECOND));

    //If grandchildren processes cannot talk to us, simply wait.
    //Else, wait, and check socket for messages.
    if ((monitor_queue_fd < 0) && (monitor_inotify_fd < 0))
    {
        select(1, NULL, NULL, NULL, &timeout);
    }
    else
    {
        int count = 1;

	/* Figure out the number of file descriptors to pass to select */
        int nfds = (monitor_queue_fd > monitor_inotify_fd ? monitor_queue_fd + 1 : monitor_inotify_fd + 1);

        while(count > 0)
        {
            fd_set rset;
            FD_ZERO(&rset);
            if (monitor_queue_fd > 0)   FD_SET(monitor_queue_fd, &rset);
            if (monitor_inotify_fd > 0) FD_SET(monitor_inotify_fd, &rset);

            timeout.tv_sec   = 0;
            timeout.tv_usec  = interval;
            interval = 0;                     //Next loop we do not wait at all
            count = select(nfds, &rset, NULL, NULL, &timeout);

            if(count > 0)
	    {
                if (FD_ISSET(monitor_queue_fd, &rset)) monitor_dispatch_msg();
                if (FD_ISSET(monitor_inotify_fd, &rset)) monitor_handle_inotify();
            }
        }
    }

    return 0;
}

/***
 * Functions to fork the very first process. This process is
 * created and suspended before execv, until a SIGCONT is sent
 * from the monitor. 
***/

//Very first process signal handler.
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

        monitor_track_process(pid);

        signal(SIGCONT, prev_handler);
        kill(pid, SIGCONT);
    }
    else
    {
        //sigsuspend(&set);
        signal(SIGCONT, prev_handler);
    }

    return pid;
}

struct process_info *spawn_first_process(const char *cmd, int child_in_foreground)
{
    pid_t pid;

    pid = monitor_fork();

    monitor_summary_header();

    if(pid > 0)
    {
        first_process_pid = pid;
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        setpgid(pid, 0);

        if (child_in_foreground)
        {
            int fdtty, retc;
            fdtty = open("/dev/tty", O_RDWR);
            if (fdtty >= 0)
            {
                /* Try bringing the child process to the session foreground */
                retc = tcsetpgrp(fdtty, getpgid(pid));
                if (retc < 0)
                {
                 fatal("error bringing process to the session foreground (tcsetpgrp): %s\n", strerror(errno));
                }
                close(fdtty);
            } else {
                fatal("error accessing controlling terminal (/dev/tty): %s\n", strerror(errno));
            }
        }
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


static void show_help(const char *cmd)
{
    fprintf(stdout, "\nUse: %s [options] -- command-line-and-options\n\n", cmd);
    fprintf(stdout, "%-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
    fprintf(stdout, "%-30s Send debugging output to <file>.\n", "-o,--debug-file=<file>");
    fprintf(stdout, "%-30s Show this message.\n", "-h,--help");
    fprintf(stdout, "%-30s Show version string\n", "-v,--version");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Interval between observations, in microseconds. (default=%d)\n", "-i,--interval=<n>", DEFAULT_INTERVAL);
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Use maxfile with list of var: value pairs for resource limits.\n", "-l,--limits-file=<maxfile>");
    fprintf(stdout, "%-30s Use string of the form \"var: value, var: value\" to specify\n", "-L,--limits=<string>");
    fprintf(stdout, "%-30s resource limits.\n", "");
    fprintf(stdout, "%-30s Keep the monitored process in foreground (for interactive use).\n", "-f,--child-in-foreground");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Specify filename template for log files (default=resource-pid-<pid>)\n", "-O,--with-output-files=<file>");
    fprintf(stdout, "%-30s Write resource summary to <file>        (default=<template>.summary)\n", "--with-summary-file=<file>");
    fprintf(stdout, "%-30s Write resource time series to <file>    (default=<template>.series)\n", "--with-time-series=<file>");
    fprintf(stdout, "%-30s Write list of opened files to <file>    (default=<template>.files)\n", "--with-opened-files=<file>");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Do not write the summary log file.\n", "--without-summary-file"); 
    fprintf(stdout, "%-30s Do not write the time series log file.\n", "--without-time-series"); 
    fprintf(stdout, "%-30s Do not write the list of opened files.\n", "--without-opened-files"); 
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Measure working directory footprint (potentially slow).\n", "--with-disk-footprint"); 
    fprintf(stdout, "%-30s Do not measure working directory footprint (default).\n", "--without-disk-footprint"); 
}


int monitor_resources(long int interval /*in microseconds */)
{
    uint64_t round;

    struct process_info  *p_acc = calloc(1, sizeof(struct process_info)); //Automatic zeroed.
    struct wdir_info     *d_acc = calloc(1, sizeof(struct wdir_info));
    struct filesys_info  *f_acc = calloc(1, sizeof(struct filesys_info));

    struct rmsummary    *resources_now = calloc(1, sizeof(struct rmsummary));

    // Loop while there are processes to monitor, that is 
    // itable_size(processes) > 0). The check is done again in a
    // if/break pair below to mitigate a race condition in which
    // the last process exits after the while(...) is tested, but
    // before we reach select.
    round = 1;
    while(itable_size(processes) > 0)
    { 
        ping_processes();

        monitor_poll_all_processes_once(processes, p_acc);

	if(resources_flags->workdir_footprint)
		monitor_poll_all_wds_once(wdirs, d_acc);

	// monitor_fss_once(f); disabled until statfs fs id makes sense.

        monitor_collate_tree(resources_now, p_acc, d_acc, f_acc);

	monitor_find_max_tree(summary, resources_now);

        monitor_log_row(resources_now);

        if(!monitor_check_limits(summary))
            monitor_final_cleanup(SIGTERM);

	release_waiting_processes();

        cleanup_zombies();
        //If no more process are alive, break out of loop.
        if(itable_size(processes) < 1)
            break;
        
        wait_for_messages(interval);

        //cleanup processes which by terminating may have awaken
        //select.
        cleanup_zombies();

        round++;
    }

    free(resources_now);
    free(p_acc);
    free(d_acc);
    free(f_acc);

    return 0;
}

int main(int argc, char **argv) {
    int i;
    char cmd[1024] = {'\0'};
    char c;
    uint64_t interval = DEFAULT_INTERVAL;

    char *template_path = NULL;
    char *summary_path = NULL;
    char *series_path  = NULL;
    char *opened_path  = NULL;

    int use_summary = 1;
    int use_series  = 1;
    int use_opened  = 1;
    int child_in_foreground = 0;

    debug_config(argv[0]);

    signal(SIGCHLD, monitor_check_child);
    signal(SIGINT,  monitor_final_cleanup);
    signal(SIGQUIT, monitor_final_cleanup);
    signal(SIGTERM, monitor_final_cleanup);

    summary          = calloc(1, sizeof(struct rmsummary));
    resources_limits = make_rmsummary(-1);
    resources_flags  = make_rmsummary(0);

    rmsummary_read_env_vars(resources_limits);

    struct option long_options[] =
	    {
		    /* Regular Options */
		    {"debug",      required_argument, 0, 'd'},
		    {"debug-file", required_argument, 0, 'o'},
		    {"help",       required_argument, 0, 'h'},
		    {"version",    no_argument,       0, 'v'},
		    {"interval",   required_argument, 0, 'i'},
		    {"limits",     required_argument, 0, 'L'},
		    {"limits-file",required_argument, 0, 'l'},
	
		    {"with-output-files", required_argument, 0,  'O'},

		    {"with-summary-file", required_argument, 0,  0},
		    {"with-time-series",  required_argument, 0,  1 }, 
		    {"with-opened-files", required_argument, 0,  2 },

		    {"without-summary",      no_argument, 0, 3},
		    {"without-time-series",  no_argument, 0, 4}, 
		    {"without-opened-files", no_argument, 0, 5},

		    {"with-disk-footprint",    no_argument, 0, 6},
		    {"without-disk-footprint", no_argument, 0, 7},

		    {0, 0, 0, 0}
	    };

    while((c = getopt_long(argc, argv, "d:fhi:L:l:o:O:v", long_options, NULL)) >= 0)
    {
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				break;
			case 'h':
				show_help(argv[0]);
				return 0;
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			case 'i':
				interval = strtoll(optarg, NULL, 10);
				if(interval < 1)
					fatal("interval cannot be set to less than one microsecond.");
				break;
			case 'l':
				parse_limits_file(resources_limits, optarg);
				break;
			case 'L':
				parse_limits_string(resources_limits, optarg);
				break;
			case 'f':
				child_in_foreground = 1;
				break;
			case 'O':
				if(template_path)
					free(template_path);
				if(summary_path)
				{
					free(summary_path);
					summary_path = NULL;
				}
				if(series_path)
				{
					free(series_path);
					series_path = NULL;
				}
				if(opened_path)
				{
					free(opened_path);
					opened_path = NULL;
				}
				template_path = xxstrdup(optarg);
				break;
			case 0:
				if(summary_path)
					free(summary_path);
				summary_path = xxstrdup(optarg);
				use_summary = 1;
				break;
			case  1:
				if(series_path)
					free(series_path);
				series_path = xxstrdup(optarg);
				use_series  = 1;
				break;
			case  2:
				if(opened_path)
					free(opened_path);
				opened_path = xxstrdup(optarg);
				use_opened  = 1;
				break;
			case  3:
				if(summary_path)
					free(summary_path);
				summary_path = NULL;
				use_summary = 0;
				break;
			case  4:
				if(series_path)
					free(series_path);
				series_path = NULL;
				use_series  = 0;
				break;
			case  5:
				if(opened_path)
					free(opened_path);
				opened_path = NULL;
				use_opened  = 0;
				break;
			case 6:
				resources_flags->workdir_footprint = 1;
				break;
			case 7:
				resources_flags->workdir_footprint = 0;
				break;
			default:
				show_help(argv[0]);
				return 1;
				break;
		}
	}

    rmsummary_debug_report(resources_limits);

    //this is ugly, concatenating command and arguments
    if(optind < argc)
    {
        for(i = optind; i < argc; i++)
        {
            strcat(cmd, argv[i]);
            strcat(cmd, " ");
        }
    }
    else
    {
        show_help(argv[0]);
        return 1;
    }


    if(getenv(RESOURCE_MONITOR_INFO_ENV_VAR))
    {
        debug(D_DEBUG, "using upstream monitor. executing: %s\n", cmd);
        execlp("sh", "sh", "-c", cmd, (char *) NULL);
        //We get here only if execlp fails.
        fatal("error executing %s:\n", cmd, strerror(errno));
    }

#ifdef CCTOOLS_USE_RMONITOR_HELPER_LIB
    write_helper_lib();
    monitor_helper_init(lib_helper_name, &monitor_queue_fd);
#endif

#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
    monitor_inotify_fd = inotify_init();
    alloced_inotify_watches = 100;
    inotify_watches = (char **)(calloc(alloced_inotify_watches, sizeof(char *)));
    if (inotify_watches == NULL) alloced_inotify_watches = 0;
#endif

#if defined(CCTOOLS_OPSYS_FREEBSD)
    kd_fbsd = kvm_open(NULL, "/dev/null", NULL, O_RDONLY, "kvm_open");
#endif

    processes = itable_create(0);
    wdirs     = hash_table_create(0,0);
    filesysms = itable_create(0);
    files     = hash_table_create(0,0); 

    wdirs_rc   = itable_create(0);
    filesys_rc = itable_create(0);

    if(use_summary && !summary_path)
        summary_path = default_summary_name(template_path);
    if(use_series && !series_path)
        series_path = default_series_name(template_path);
    if(use_opened && !opened_path)
        opened_path = default_opened_name(template_path);

    log_summary = open_log_file(summary_path);
    log_series  = open_log_file(series_path);
    log_opened  = open_log_file(opened_path);

    summary->command = xxstrdup(cmd);
    summary->start   = usecs_since_epoch();

    
#if defined(CCTOOLS_OPSYS_LINUX) && defined(RESOURCE_MONITOR_USE_INOTIFY)
    if(log_opened)
    {
	    monitor_inotify_fd = inotify_init();
	    alloced_inotify_watches = 100;
	    inotify_watches = (char **)(calloc(alloced_inotify_watches, sizeof(char *)));
	    if (inotify_watches == NULL) alloced_inotify_watches = 0;
    }
#endif

    spawn_first_process(cmd, child_in_foreground);

    monitor_resources(interval);

    monitor_final_cleanup(SIGTERM);

    return 0;
}

