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
 * and maximum, minimum, and average of the resources monitored.
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
#include <sys/stat.h>

#include <inttypes.h>
#include <sys/types.h>

#if defined(CCTOOLS_OPSYS_DARWIN) || defined(CCTOOLS_OPSYS_FREEBSD)
  #include <sys/param.h>
  #include <sys/mount.h>
#else
  #include  <sys/vfs.h>
#endif

#if defined(CCTOOLS_OPSYS_FREEBSD)
  #include <sys/file.h>
  #include <sys/sysctl.h>
  #include <sys/user.h>
  #include <kvm.h>
#endif

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

#include "rmonitor_helper_comm.h"
#include "rmonitor_piggyback.h"


#define ONE_MEGABYTE 1048576  /* this many bytes */
#define ONE_SECOND   1000000  /* this many usecs */    

#define DEFAULT_INTERVAL       ONE_SECOND        /* in useconds */

#define DEFAULT_LOG_NAME "resource-pid-%d"     /* %d is used for the value of getpid() */

FILE  *log_summary = NULL;      /* Final statistics are written to this file. */
FILE  *log_series  = NULL;      /* Resource events and samples are written to this file. */
FILE  *log_opened  = NULL;      /* List of opened files is written to this file. */

int    monitor_queue_fd = -1;  /* File descriptor of a datagram socket to which (great)
                                  grandchildren processes report to the monitor. */

pid_t  first_process_pid;              /* pid of the process given at the command line */
pid_t  first_process_sigchild_status;  /* exit status flags of the process given at the command line */
pid_t  first_process_already_waited = 0;  /* exit status flags of the process given at the command line */

struct itable *processes;       /* Maps the pid of a process to a unique struct process_info. */
struct hash_table *wdirs;       /* Maps paths to working directory structures. */
struct itable *filesysms;       /* Maps st_dev ids (from stat syscall) to filesystem structures. */
struct hash_table *files;       /* Keeps track of which files have been opened. */

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

char *resources[15] = { "wall_clock(seconds)", 
                        "concurrent_processes", "cpu_time(seconds)",
			"virtual_memory(kB)", "resident_memory(kB)", "swap_memory(kB)", 
                        "bytes_read", "bytes_written", 
                        "workdir_number_files_dirs", "workdir_footprint(MB)",
                        NULL };

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
    {
        /* Cheating, we write to /dev/null, thus the log file is
         * not created, but we do not have to change the rest of
         * the code. */
        if((log_file = fopen("/dev/null", "w")) == NULL)
            fatal("could not open log file %s : %s\n", "/dev/null", strerror(errno));
    }

    return log_file;
}

void initialize_limits_tree(struct rmsummary *tree, int64_t val)
{
    tree->wall_time                = val;
    tree->max_concurrent_processes = val;
    tree->cpu_time                 = val;
    tree->virtual_memory           = val;
    tree->resident_memory          = val;
    tree->swap_memory              = val;
    tree->bytes_read               = val;
    tree->bytes_written            = val;
    tree->workdir_number_files_dirs = val;
    tree->workdir_footprint         = val;


}


//BUG from hash table!! what if resource is set to zero?
//BUG the parsing limit functions are ugly
//BUG there is no error checking of unknown vars or malformed
//values
//BUG it is assumed that the values given are integers
/* The limits string has format "var: value[, var: value]*" */
#define parse_limit_string(vars, tr, fld) if(hash_table_lookup(vars, #fld)){\
                                                tr->fld = (uintptr_t) hash_table_lookup(vars, #fld);\
                                                debug(D_DEBUG, "Limit %s set to %" PRId64 "\n", #fld, tr->fld);}

void parse_limits_string(char *str, struct rmsummary *tree)
{
    struct hash_table *vars = hash_table_create(0,0);
    char *line              = xxstrdup(str);
    char *var, *value;

    var = strtok(line, ":");
    while(var)
    {
        var = string_trim_spaces(xxstrdup(var));
        value = strtok(NULL, ",");
        if(value)
        {
            uintptr_t v = atoi(value);
            hash_table_insert(vars, var, (uintptr_t *) v);
        }
        else
            break;

        var = strtok(NULL, ":");
    }

    parse_limit_string(vars, tree, wall_time);
    parse_limit_string(vars, tree, max_concurrent_processes);
    parse_limit_string(vars, tree, cpu_time);
    parse_limit_string(vars, tree, virtual_memory);
    parse_limit_string(vars, tree, resident_memory);
    parse_limit_string(vars, tree, swap_memory);
    parse_limit_string(vars, tree, bytes_read);
    parse_limit_string(vars, tree, bytes_written);
    parse_limit_string(vars, tree, workdir_number_files_dirs);
    parse_limit_string(vars, tree, workdir_footprint);

    if(tree->wall_time < INTMAX_MAX/ONE_SECOND)
	    tree->wall_time *= ONE_SECOND;

    if(tree->cpu_time < INTMAX_MAX/ONE_SECOND)
	    tree->cpu_time *= ONE_SECOND;

    hash_table_delete(vars);
}

/* Every line of the limits file has the format resource: value */
#define parse_limit_file(file, tr, fld) if(!get_int_attribute(file, #fld ":", (uint64_t *) &tr->fld, 1))\
                                            debug(D_DEBUG, "Limit %s set to %" PRId64 "\n", #fld, tr->fld);
void parse_limits_file(char *path, struct rmsummary *tree)
{
    FILE *flimits = fopen(path, "r");
    
    parse_limit_file(flimits, tree, wall_time);
    parse_limit_file(flimits, tree, max_concurrent_processes);
    parse_limit_file(flimits, tree, cpu_time);
    parse_limit_file(flimits, tree, virtual_memory);
    parse_limit_file(flimits, tree, swap_memory);
    parse_limit_file(flimits, tree, bytes_read);
    parse_limit_file(flimits, tree, bytes_written);
    parse_limit_file(flimits, tree, workdir_number_files_dirs);
    parse_limit_file(flimits, tree, workdir_footprint);

    if(tree->wall_time < INTMAX_MAX/ONE_SECOND)
	    tree->wall_time *= ONE_SECOND;

    if(tree->cpu_time < INTMAX_MAX/ONE_SECOND)
	    tree->cpu_time *= ONE_SECOND;
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
    int i;

    fprintf(log_series, "#");
    for(i = 0; resources[i]; i++)
        fprintf(log_series, "%s\t", resources[i]);

    fprintf(log_series, "\n");
}

void monitor_collate_tree(struct rmsummary *tr, struct process_info *p, struct wdir_info *d, struct filesys_info *f)
{
    tr->wall_time                = usecs_since_epoch() - summary->start;

    tr->max_concurrent_processes = (int64_t) itable_size(processes);
    tr->cpu_time                 = p->cpu.delta + tr->cpu_time;
    tr->virtual_memory           = (int64_t) p->mem.virtual;
    tr->resident_memory          = (int64_t) p->mem.resident;
    tr->swap_memory              = (int64_t) p->mem.swap;

    tr->bytes_read               = (int64_t) (p->io.delta_chars_read + tr->bytes_read);
    tr->bytes_read              += (int64_t)  p->io.delta_bytes_faulted;
    tr->bytes_written            = (int64_t) (p->io.delta_chars_written + tr->bytes_written);

    tr->workdir_number_files_dirs = (int64_t) (d->files + d->directories);
    tr->workdir_footprint         = (int64_t) d->byte_count;

    tr->fs_nodes                 = (int64_t) f->disk.f_ffree;
}

void monitor_find_max_tree(struct rmsummary *result, struct rmsummary *tr)
{
    if(!tr)
        return;

    if(result->wall_time < tr->wall_time)
        result->wall_time = tr->wall_time;

    if(result->max_concurrent_processes < tr->max_concurrent_processes)
        result->max_concurrent_processes = tr->max_concurrent_processes;

    if(result->cpu_time < tr->cpu_time)
        result->cpu_time = tr->cpu_time;

    if(result->virtual_memory < tr->virtual_memory)
        result->virtual_memory = tr->virtual_memory;

    if(result->resident_memory < tr->resident_memory)
        result->resident_memory = tr->resident_memory;

    if(result->swap_memory < tr->swap_memory)
        result->swap_memory = tr->swap_memory;

    if(result->bytes_read < tr->bytes_read)
        result->bytes_read = tr->bytes_read;

    if(result->bytes_written < tr->bytes_written)
        result->bytes_written = tr->bytes_written;

    if(result->workdir_number_files_dirs < tr->workdir_number_files_dirs)
        result->workdir_number_files_dirs = tr->workdir_number_files_dirs;

    if(result->workdir_footprint < tr->workdir_footprint)
        result->workdir_footprint = tr->workdir_footprint;

    if(result->fs_nodes < tr->fs_nodes)
        result->fs_nodes = tr->fs_nodes;
}

void monitor_log_row(struct rmsummary *tr)
{
    fprintf(log_series, "%" PRId64 "\t", tr->wall_time + summary->start);
    fprintf(log_series, "%" PRId64 "\t", tr->max_concurrent_processes);
    fprintf(log_series, "%" PRId64 "\t", tr->cpu_time);
    fprintf(log_series, "%" PRId64 "\t", tr->virtual_memory);
    fprintf(log_series, "%" PRId64 "\t", tr->resident_memory);
    fprintf(log_series, "%" PRId64 "\t", tr->swap_memory);
    fprintf(log_series, "%" PRId64 "\t", tr->bytes_read);
    fprintf(log_series, "%" PRId64 "\t", tr->bytes_written);

    if(resources_flags->workdir_footprint)
    {
	    fprintf(log_series, "%" PRId64 "\t", tr->workdir_number_files_dirs);
	    fprintf(log_series, "%" PRId64 "\t", tr->workdir_footprint);
    }

    fprintf(log_series, "\n");
                               
    /* are we going to keep monitoring the whole filesystem? */
    // fprintf(log_series "%" PRId64 "\n", tr->fs_nodes);

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

		if(summary->limits_exceeded)
			summary->exit_type = xxstrdup("limits");
		else
			summary->exit_type = xxstrdup("signal");

		if(WIFSIGNALED(wait_status))
			summary->signal    = WTERMSIG(wait_status);
		else
			summary->signal    = WSTOPSIG(wait_status);

		summary->exit_status = -1;
	} 

}

int monitor_final_summary()
{
	decode_zombie_status(summary, first_process_sigchild_status);

	summary->end       = usecs_since_epoch();
	summary->wall_time = summary->end - summary->start;
	
	rmsummary_print(log_summary, summary);

	if(summary->limits_exceeded && summary->exit_status == 0)
		return -1;
	else
		return summary->exit_status;
}

void monitor_write_open_file(char *filename)
{
    /* Perhaps here we can do something more to the files, like a
     * final stat */

    fprintf(log_opened, "%s\n", filename);

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

    tr_usg->resident_memory = usg.ru_maxrss;

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
          debug(D_NOTICE, "Process asked for input from the terminal, but currently the resource monitor does not support interactive applications.\n");
          break;
        case SIGTTOU:
          debug(D_NOTICE, "Process wants to write to the standard output, but the current terminal settings do not allow this. The monitor currently does not support interactive applications.\n");
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
        sleep(1);

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

    fclose(log_summary);
    fclose(log_series);
    fclose(log_opened);

    exit(status);
}

// The following keeps getting uglier and uglier! Rethink how to do it!
//
#define over_limit_check(tr, fld, mult, fmt)				\
	if((tr)->fld > 0 && resources_limits->fld - (tr)->fld < 0)	\
	{								\
		char *tmp;						\
		if((tr)->limits_exceeded)                               \
		{							\
			tmp = string_format("%s, " #fld ": %" fmt, mult * resources_limits->fld); \
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

    over_limit_check(tr, wall_time, 1.0/ONE_SECOND, "lf");
    over_limit_check(tr, max_concurrent_processes, 1, PRId64);
    over_limit_check(tr, cpu_time, 1.0/ONE_SECOND, "lf");
    over_limit_check(tr, virtual_memory, 1, PRId64);
    over_limit_check(tr, resident_memory, 1, PRId64);
    over_limit_check(tr, swap_memory, 1, PRId64);
    over_limit_check(tr, bytes_read, 1, PRId64);
    over_limit_check(tr, bytes_written, 1, PRId64);
    over_limit_check(tr, workdir_number_files_dirs, 1, PRId64);
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
		if( msg.type == WAIT )
			release_waiting_process(msg.origin);
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
        case WAIT:
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
		monitor_write_open_file(msg.data.s);
		hash_table_insert(files, msg.data.s, NULL);
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
    if(monitor_queue_fd < 0)
    {
        select(1, NULL, NULL, NULL, &timeout);
    }
    else
    {
        int count = 1;
        while(count > 0)
        {
            fd_set rset;
            FD_ZERO(&rset);
            FD_SET(monitor_queue_fd, &rset);
            timeout.tv_sec   = 0;
            timeout.tv_usec  = interval;
            interval = 0;                     //Next loop we do not wait at all
            count = select(monitor_queue_fd + 1, &rset, NULL, NULL, &timeout);

            if(count > 0)
                monitor_dispatch_msg();
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

struct process_info *spawn_first_process(const char *cmd)
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
    fprintf(stdout, "%-30s Show this message.\n", "-h,--help");
    fprintf(stdout, "%-30s Show version string\n", "-v,--version");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Interval between observations, in microseconds. (default=%d)\n", "-i,--interval=<n>", DEFAULT_INTERVAL);
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Use maxfile with list of var: value pairs for resource limits.\n", "-l,--limits-file=<maxfile>");
    fprintf(stdout, "%-30s Use string of the form \"var: value, var: value\" to specify\n", "-L,--limits=<string>");
    fprintf(stdout, "%-30s resource limits.\n", "");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Specify filename template for log files (default=resource-pid-<pid>)\n", "-o,--with-output-files=<file>");
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

    struct process_info  *p_acc = malloc(sizeof(struct process_info));
    struct wdir_info     *d_acc = malloc(sizeof(struct wdir_info));
    struct filesys_info  *f_acc = malloc(sizeof(struct filesys_info));

    struct rmsummary    *resources_now = calloc(1, sizeof(struct rmsummary)); //Automatic zeroed.

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

    debug_config(argv[0]);

    signal(SIGCHLD, monitor_check_child);
    signal(SIGINT,  monitor_final_cleanup);
    signal(SIGQUIT, monitor_final_cleanup);
    signal(SIGTERM, monitor_final_cleanup);

    summary    = calloc(1, sizeof(struct rmsummary));
    resources_limits = calloc(1, sizeof(struct rmsummary));
    resources_flags  = calloc(1, sizeof(struct rmsummary));

    initialize_limits_tree(resources_limits, INTMAX_MAX);

    struct option long_options[] =
	    {
		    /* Regular Options */
		    {"debug",      required_argument, 0, 'd'},
		    {"help",       required_argument, 0, 'h'},
		    {"version",    no_argument,       0, 'v'},
		    {"interval",   required_argument, 0, 'i'},
		    {"limits",     required_argument, 0, 'L'},
		    {"limits-file",required_argument, 0, 'l'},
	
		    {"with-output-files", required_argument, 0,  'o'},

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

    while((c = getopt_long(argc, argv, "d:hvi:L:l:o:", long_options, NULL)) >= 0)
    {
	    switch (c) {
            case 'd':
		    debug_flags_set(optarg);
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
		    parse_limits_file(optarg, resources_limits);
		    break;
            case 'L':
		    parse_limits_string(optarg, resources_limits);
		    break;
            case 'o':
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
		    use_summary = 1;
		    use_series  = 1;
		    use_opened  = 1;
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
    summary->start = usecs_since_epoch();

    spawn_first_process(cmd);

    monitor_resources(interval);

    monitor_final_cleanup(SIGTERM);

    return 0;
}

