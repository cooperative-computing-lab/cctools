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
 * rmonitor_CATEGORY_summary writes the corresponding information
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
 * bytes_read:    read chars count using *read system calls from disk.
 * bytes_written: writen char count using *write system calls to disk.
 * bytes_received:total bytes received (recv family)
 * bytes_sent:    total bytes sent     (send family)
 * total_files    total file + directory count of all working directories.
 * disk           total byte count of all working directories.
 *
 * The log file is written to the home directory of the monitor
 * process. A flag will be added later to indicate a prefered
 * output file. Additionally, a summary log file is written at
 * the end, reporting the command run, starting and ending times,
 * and maximum, of the resources monitored.
 *
 * Each monitored process gets a 'struct rmonitor_process_info', itself
 * composed of 'struct mem_info', 'struct cpu_time_info', etc. There
 * is a global variable, 'processes', that keeps a table relating pids to
 * the corresponding struct rmonitor_process_info.
 *
 * Likewise, there are tables that relate paths to 'struct
 * rmonitor_wdir_info' ('wdirs'), and device ids to 'struct
 * rmonitor_filesys_info' ('filesysms').
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

#include "buffer.h"
#include "cctools.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "getopt.h"
#include "hash_table.h"
#include "itable.h"
#include "jx.h"
#include "jx_print.h"
#include "list.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"
#include "xxmalloc.h"
#include "elfheader.h"
#include "domain_name_cache.h"

#include "rmonitor.h"
#include "rmonitor_poll_internal.h"

#define RESOURCE_MONITOR_USE_INOTIFY 1
#if defined(RESOURCE_MONITOR_USE_INOTIFY)
#include <sys/inotify.h>
#include <sys/ioctl.h>
#endif

#include "rmonitor_helper_comm.h"
#include "rmonitor_piggyback.h"

#define DEFAULT_INTERVAL       5               /* in seconds */
#define DEFAULT_LOG_NAME "resource-pid-%d"     /* %d is used for the value of getpid() */

#define ACTIVATE_DEBUG_FILE ".cctools_resource_monitor_debug"

uint64_t interval = DEFAULT_INTERVAL;

FILE  *log_summary = NULL;      /* Final statistics are written to this file. */
FILE  *log_series  = NULL;      /* Resource events and samples are written to this file. */
FILE  *log_inotify = NULL;      /* List of opened files is written to this file. */

char *template_path = NULL;     /* Prefix of all output files names */

int debug_active = 0;           /* 1 if ACTIVATE_DEBUG_FILE exists. If 1, debug info goes to ACTIVATE_DEBUG_FILE ".log" */

struct jx *verbatim_summary_fields; /* fields added to the summary without change */


int    rmonitor_queue_fd = -1;  /* File descriptor of a datagram socket to which (great)
                                  grandchildren processes report to the monitor. */
static int rmonitor_inotify_fd = -1;

pid_t  first_process_pid;                 /* pid of the process given at the command line */
int    first_process_sigchild_status;     /* exit status flags of the process given at the command line */
int    first_process_already_waited = 0;  /* exit status flags of the process given at the command line */
int    first_process_exit_status = 0;

struct itable *processes;       /* Maps the pid of a process to a unique struct rmonitor_process_info. */
struct hash_table *wdirs;       /* Maps paths to working directory structures. */
struct itable *filesysms;       /* Maps st_dev ids (from stat syscall) to filesystem structures. */
struct hash_table *files;       /* Keeps track of which files have been opened. */

static int follow_chdir = 0;    /* Keep track of all the working directories per process. */
static int pprint_summaries = 1; /* Pretty-print json summaries. */

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
static char **inotify_watches;  /* Keeps track of created inotify watches. */
static int alloced_inotify_watches = 0;
#endif

struct itable *wdirs_rc;        /* Counts how many rmonitor_process_info use a rmonitor_wdir_info. */
struct itable *filesys_rc;      /* Counts how many rmonitor_wdir_info use a rmonitor_filesys_info. */


char *lib_helper_name = NULL;  /* Name of the helper library that is
                                  automatically extracted */

int lib_helper_extracted;       /* Boolean flag to indicate whether the bundled
                                   helper library was automatically extracted
                                   */

struct rmsummary *summary;          /* final summary */
struct rmsummary *snapshot;         /* current snapshot */
struct rmsummary *resources_limits;
struct rmsummary *resources_flags;

struct list *tx_rx_sizes; /* list of network byte counts with a timestamp, to compute bandwidth. */
int64_t total_bytes_rx;   /* total bytes received */
int64_t total_bytes_tx;   /* total bytes sent */

const char *sh_cmd_line = NULL;    /* command line passed with the --sh option. */

char *snapshot_signal_file = NULL;    /* name of the file that, if
										 exists, makes the monitor record
										 an snapshot of the current
										 usage. The first line of the
										 file labels the snapshot. The
										 file is removed when the
										 snapshot is recorded, so that
										 multiple snapshots can be
										 created. */
struct list *snapshots = NULL;              /* list of snapshots, as json objects. */

/***
 * Utility functions (open log files, proc files, measure time)
 ***/

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
        path_dirname(log_path, dirname);
        if(!create_dir(dirname, 0755)) {
            debug(D_FATAL, "could not create directory %s : %s\n", dirname, strerror(errno));
			exit(RM_MONITOR_ERROR);
		}

        if((log_file = fopen(log_path, "w")) == NULL) {
            debug(D_FATAL, "could not open log file %s : %s\n", log_path, strerror(errno));
			exit(RM_MONITOR_ERROR);
		}

        free(dirname);
    }
    else
	    return NULL;

    return log_file;
}

void activate_debug_log_if_file() {
	static timestamp_t last_time = 0;

	timestamp_t current = timestamp_get();

	if((current - last_time) < 30*USECOND ) {
		return;
	}

	struct stat s;
	int status = stat(ACTIVATE_DEBUG_FILE, &s);

	if(status == 0) {
		if(!debug_active) {
			debug_active = 1;
			debug_flags_set("all");
			debug_config_file(ACTIVATE_DEBUG_FILE ".log");
			debug_config_file_size(0);
		}
	}
	else if(debug_active) {
		debug_active = 0;
		debug_flags_set("clear");
	}

	last_time = current;
}

void parse_limit_string(struct rmsummary *limits, char *str)
{
	char *pair  = xxstrdup(str);
	char *delim = strchr(pair, ':');

	if(!delim) {
		fatal("Missing ':' in '%s'\n", str);
	}

	*delim = '\0';

	char *field = string_trim_spaces(pair);
	char *value = string_trim_spaces(delim + 1);

	int status;

	if(
			strcmp(field, "start")     == 0 ||
			strcmp(field, "end")       == 0 ||
			strcmp(field, "wall_time") == 0 ||
			strcmp(field, "cpu_time")  == 0
	  ) {
		double d;
		status = string_is_float(value, &d);
		if(status) {
			rmsummary_assign_int_field(limits, field, d*1000000);
		}

	} else {
		long long i;
		status = string_is_integer(value, &i);
		if(status) {
			status = rmsummary_assign_int_field(limits, field, i);
		}
	}

	if(!status) {
		fatal("Invalid limit field '%s' or value '%s'\n", field, value);
	}

	free(pair);
}

void parse_limits_file(struct rmsummary *limits, char *path)
{
	struct rmsummary *s;
	s = rmsummary_parse_file_single(path);

	rmsummary_merge_override(limits, s);

	rmsummary_delete(s);
}

void add_verbatim_field(const char *str) {
	char *pair  = xxstrdup(str);
	char *delim = strchr(pair, ':');

	if(!delim) {
		fatal("Missing ':' in '%s'\n", str);
	}

	*delim = '\0';

	char *field = string_trim_spaces(pair);
	char *value = string_trim_spaces(delim + 1);

	if(!verbatim_summary_fields)
		verbatim_summary_fields = jx_object(NULL);

	jx_insert_string(verbatim_summary_fields, field, value);
	debug(D_RMON, "%s", pair);

	free(pair);
}

void add_snapshots() {
	if(!snapshots) {
		return;
	}

	struct jx *a = jx_array(0);

	struct jx *j;
	list_first_item(snapshots);
	while((j = list_next_item(snapshots))) {
		jx_array_insert(a, j);
	}

	jx_insert(verbatim_summary_fields, jx_string("snapshots"), a);
}


int rmonitor_determine_exec_type(const char *executable) {
	char *absolute_exec = path_which(executable);
	char exec_type[PATH_MAX];

	if(!absolute_exec)
		return 1;

	int fd = open(absolute_exec, O_RDONLY, 0);
	if(fd < 0) {
		debug(D_RMON, "Could not open '%s' for reading.", absolute_exec);
		free(absolute_exec);
		return 1;
	}

	bzero(exec_type, PATH_MAX);
	size_t n = read(fd, exec_type, sizeof(exec_type) - 1);

	if(n < 1 || lseek(fd, 0, SEEK_SET) < 0) {
		debug(D_RMON, "Could not read header of '%s'.", absolute_exec);
		strcpy(exec_type, "unknown");
	} else if(strncmp(exec_type, "#!", 2) == 0) {
		char *newline = strchr(exec_type, '\n');
		if(newline)
			*newline = '\0';
	} else {
		errno = 0;
		int rc = elf_get_interp(fd, exec_type);

		if(rc < 0) {
			if(errno == EINVAL) {
				strcpy(exec_type, "static");
			} else {
				strcpy(exec_type, "unknown");
			}
		} else {
			strcpy(exec_type, "dynamic");
		}
	}

	close(fd);

	if(strcmp(exec_type, "dynamic") != 0) {
		debug(D_NOTICE, "Executable is not dynamically linked. Some resources may be undercounted, and children processes may not be tracked.");
	}

	char *type_field = string_format("executable_type: %s", exec_type);
	add_verbatim_field(type_field);

	free(type_field);
	free(absolute_exec);

	return 0;
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

int inc_fs_count(struct rmonitor_filesys_info *f)
{
    int count = itable_addto_count(filesys_rc, f, 1);

    debug(D_RMON, "filesystem %d reference count +1, now %d references.\n", f->id, count);

    return count;
}

int dec_fs_count(struct rmonitor_filesys_info *f)
{
    int count = itable_addto_count(filesys_rc, f, -1);

    debug(D_RMON, "filesystem %d reference count -1, now %d references.\n", f->id, count);

    if(count < 1)
    {
        debug(D_RMON, "filesystem %d is not monitored anymore.\n", f->id);
        free(f->path);
        free(f);
    }

    return count;
}

int inc_wd_count(struct rmonitor_wdir_info *d)
{
    int count = itable_addto_count(wdirs_rc, d, 1);

    debug(D_RMON, "working directory '%s' reference count +1, now %d references.\n", d->path, count);

    return count;
}

int dec_wd_count(struct rmonitor_wdir_info *d)
{
    int count = itable_addto_count(wdirs_rc, d, -1);

    debug(D_RMON, "working directory '%s' reference count -1, now %d references.\n", d->path, count);

    if(count < 1)
    {
        debug(D_RMON, "working directory '%s' is not monitored anymore.\n", d->path);

		path_disk_size_info_delete_state(d->state);
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
        debug(D_RMON, "stat call on '%s' failed : %s\n", path, strerror(errno));
        return -1;
    }

    return dinfo.st_dev;
}

struct rmonitor_filesys_info *lookup_or_create_fs(char *path)
{
    uint64_t dev_id = get_device_id(path);
    struct rmonitor_filesys_info *inventory = itable_lookup(filesysms, dev_id);

    if(!inventory)
    {
        debug(D_RMON, "filesystem %"PRId64" added to monitor.\n", dev_id);

        inventory = (struct rmonitor_filesys_info *) malloc(sizeof(struct rmonitor_filesys_info));
        inventory->path = xxstrdup(path);
        inventory->id   = dev_id;
        itable_insert(filesysms, dev_id, (void *) inventory);
        rmonitor_get_dsk_usage(inventory->path, &inventory->disk_initial);
    }

    inc_fs_count(inventory);

    return inventory;
}

struct rmonitor_wdir_info *lookup_or_create_wd(struct rmonitor_wdir_info *previous, char const *path)
{
    struct rmonitor_wdir_info *inventory;

    if(strlen(path) < 1 || access(path, F_OK) != 0)
        return previous;

    inventory = hash_table_lookup(wdirs, path);

    if(!inventory)
    {
        debug(D_RMON, "working directory '%s' added to monitor.\n", path);

        inventory = (struct rmonitor_wdir_info *) malloc(sizeof(struct rmonitor_wdir_info));
        inventory->path  = xxstrdup(path);
		inventory->state = NULL;
        hash_table_insert(wdirs, inventory->path, (void *) inventory);

        inventory->fs = lookup_or_create_fs(inventory->path);
    }

    if(inventory != previous)
    {
        inc_wd_count(inventory);
        if(previous)
            dec_wd_count(previous);
    }

    debug(D_RMON, "filesystem of %s is %d\n", inventory->path, inventory->fs->id);

    return inventory;
}

void rmonitor_add_file_watch(const char *filename, int is_output, int override_flags)
{
	struct rmonitor_file_info *finfo;
	struct stat fst;

	finfo = hash_table_lookup(files, filename);
	if (finfo)
	{
		(finfo->n_references)++;
		(finfo->n_opens)++;
		return;
	}

	finfo = calloc(1, sizeof(struct rmonitor_file_info));
	if (finfo != NULL)
	{
		finfo->n_opens       = 1;
		finfo->size_on_open  = -1;
		finfo->size_on_close = -1;
		finfo->is_output     = is_output;

		if (stat(filename, &fst) >= 0)
		{
			finfo->size_on_open  = fst.st_size;
			finfo->device        = fst.st_dev;
		}
	}

	hash_table_insert(files, filename, finfo);

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
	if (rmonitor_inotify_fd >= 0)
	{
		char **new_inotify_watches;
		int iwd;

		int inotify_flags = IN_CLOSE | IN_ACCESS | IN_MODIFY;
		if(override_flags) {
			inotify_flags = override_flags;
		}

		if ((iwd = inotify_add_watch(rmonitor_inotify_fd, filename, inotify_flags)) < 0)
		{
			debug(D_RMON, "inotify_add_watch for file %s fails: %s", filename, strerror(errno));
		} else {
			debug(D_RMON, "added watch (id: %d) for file %s", iwd, filename);
			if (iwd >= alloced_inotify_watches)
			{
				new_inotify_watches = (char **)realloc(inotify_watches, (iwd+50) * (sizeof(char *)));
				if (new_inotify_watches != NULL)
				{
					alloced_inotify_watches = iwd+50;
					inotify_watches = new_inotify_watches;
				} else {
					debug(D_RMON, "Out of memory trying to expand inotify_watches");
				}
			}
			if (iwd < alloced_inotify_watches)
			{
				inotify_watches[iwd] = xxstrdup(filename);
				if (finfo != NULL) finfo->n_references = 1;
			} else {
				debug(D_RMON, "Out of memory: Removing inotify watch for %s", filename);
				inotify_rm_watch(rmonitor_inotify_fd, iwd);
			}
		}
	}
#endif
}

int rmonitor_handle_inotify(void)
{
	int urgent = 0;

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
	struct inotify_event *evdata;
	struct rmonitor_file_info *finfo;
	struct stat fst;
	char *fname;
	int nbytes, evc, i;

	if (rmonitor_inotify_fd >= 0)
	{
		if (ioctl(rmonitor_inotify_fd, FIONREAD, &nbytes) >= 0)
		{
			evdata = (struct inotify_event *) malloc(nbytes);
			if (evdata == NULL) return urgent;
			if (read(rmonitor_inotify_fd, evdata, nbytes) != nbytes)
			{
				free(evdata);
				return urgent;
			}

			evc = nbytes/sizeof(*evdata);
			for(i = 0; i < evc; i++)
			{
				if (evdata[i].wd >= alloced_inotify_watches) {
					continue;
				}

				if ((fname = inotify_watches[evdata[i].wd]) == NULL) {
					continue;
				}

				if (evdata[i].mask & IN_CREATE) {
					if(snapshot_signal_file && strcmp(snapshot_signal_file, evdata[i].name) == 0) {
							debug(D_RMON, "found snapshot file '%s'", fname);
							rmonitor_add_file_watch(snapshot_signal_file, 0, IN_MODIFY | IN_OPEN | IN_CLOSE);
							urgent = 1;
					}
					continue;
				}

				if (finfo == NULL) {
					continue;
				}
				finfo = hash_table_lookup(files, fname);
				if (evdata[i].mask & IN_ACCESS) (finfo->n_reads)++;
				if (evdata[i].mask & IN_MODIFY) (finfo->n_writes)++;
				if (evdata[i].mask & IN_CLOSE) {
					(finfo->n_closes)++;
					if (stat(fname, &fst) >= 0)
					{
						finfo->size_on_close = fst.st_size;
					}

					/* Decrease reference count and remove watch of zero */
					(finfo->n_references)--;
					if (finfo->n_references == 0)
					{
						inotify_rm_watch(rmonitor_inotify_fd, evdata[i].wd);
						debug(D_RMON, "removed watch (id: %d) for file %s", evdata[i].wd, fname);
						free(fname);
						inotify_watches[evdata[i].wd] = NULL;
					}

					if(snapshot_signal_file && strcmp(fname, snapshot_signal_file) == 0) {
						urgent = 1;
					}
				}
			}
		}
		free(evdata);
	}
#endif

	return urgent;
}

void append_network_bw(struct rmonitor_msg *msg) {

	/* Avoid division by zero, negative bws */
	if(msg->end <= msg->start || msg->data.n < 1)
		return;

	struct rmonitor_bw_info *new_tail = malloc(sizeof(struct rmonitor_bw_info));

	new_tail->bit_count = 8*msg->data.n;
	new_tail->start     = msg->start;
	new_tail->end       = msg->end;

	/* we drop entries older than 60s, unless there are less than 4, so
	 * we can smooth some noise. */
	if(list_size(tx_rx_sizes) > 3) {
		struct rmonitor_bw_info *head;
		while((head = list_peek_head(tx_rx_sizes))) {
			if( head->end + 60*USECOND < new_tail->start) {
				list_pop_head(tx_rx_sizes);
				free(head);
			} else {
				break;
			}
		}
	}

	list_push_tail(tx_rx_sizes, new_tail);
}

int64_t average_bandwidth(int use_min_len) {
	if(list_size(tx_rx_sizes) == 0)
		return 0;

	int64_t sum = 0;
	struct rmonitor_bw_info *head, *tail;


	/* if last bit count occured more than a minute ago, report bw as 0 */
	tail = list_peek_tail(tx_rx_sizes);
	if(tail->end + 60*USECOND < timestamp_get())
		return 0;

	list_first_item(tx_rx_sizes);
	while((head = list_next_item(tx_rx_sizes))) {
		sum += head->bit_count;
	}

	head = list_peek_head(tx_rx_sizes);
	int64_t len_real = DIV_INT_ROUND_UP(tail->end - head->start, USECOND);

	/* divide at least by 10s, to smooth noise. */
	int n = use_min_len ? MAX(10, len_real) : len_real;

	return DIV_INT_ROUND_UP(sum, n);
}


/***
 * Logging functions. The process tree is summarized in struct
 * rmsummary's, computing current value, maximum, and minimums.
***/

void rmonitor_summary_header()
{
    if(log_series)
    {
	    fprintf(log_series, "# Units:\n");
	    fprintf(log_series, "# wall_clock and cpu_time in microseconds\n");
	    fprintf(log_series, "# virtual, resident and swap memory in megabytes.\n");
	    fprintf(log_series, "# disk in megabytes.\n");
	    fprintf(log_series, "# bandwidth in bits/s.\n");
	    fprintf(log_series, "# cpu_time, bytes_read, bytes_written, bytes_sent, and bytes_received show cummulative values.\n");
	    fprintf(log_series, "# wall_clock, max_concurrent_processes, virtual, resident, swap, files, and disk show values at the sample point.\n");

	    fprintf(log_series, "#");
	    fprintf(log_series,  "%s", "wall_clock");
	    fprintf(log_series, " %s", "cpu_time");
	    fprintf(log_series, " %s", "cores");
	    fprintf(log_series, " %s", "max_concurrent_processes");
	    fprintf(log_series, " %s", "virtual_memory");
	    fprintf(log_series, " %s", "memory");
	    fprintf(log_series, " %s", "swap_memory");
	    fprintf(log_series, " %s", "bytes_read");
	    fprintf(log_series, " %s", "bytes_written");
	    fprintf(log_series, " %s", "bytes_received");
	    fprintf(log_series, " %s", "bytes_sent");
	    fprintf(log_series, " %s", "bandwidth");

	    if(resources_flags->disk)
	    {
		    fprintf(log_series, " %25s", "total_files");
		    fprintf(log_series, " %25s", "disk");
	    }

	    fprintf(log_series, "\n");
    }
}

struct peak_cores_sample {
	int64_t wall_time;
	int64_t cpu_time;
};

int64_t peak_cores(int64_t wall_time, int64_t cpu_time) {
	static struct list *samples = NULL;

	int64_t max_separation = 60 + 2*interval; /* at least one minute and a complete interval */

	if(!samples) {
		samples = list_create();

		struct peak_cores_sample *zero = malloc(sizeof(struct peak_cores_sample));
		zero->wall_time = 0;
		zero->cpu_time  = 0;
		list_push_tail(samples, zero);
	}

	struct peak_cores_sample *tail = malloc(sizeof(struct peak_cores_sample));
	tail->wall_time = wall_time;
	tail->cpu_time  = cpu_time;
	list_push_tail(samples, tail);

	struct peak_cores_sample *head;

	/* Drop entries older than max_separation, unless we only have two samples. */
	while((head = list_peek_head(samples))) {
		if(list_size(samples) < 2) {
			break;
		}
		else if( head->wall_time + max_separation*USECOND < tail->wall_time) {
			list_pop_head(samples);
			free(head);
		} else {
			break;
		}
	}

	head = list_peek_head(samples);

	int64_t diff_wall = tail->wall_time - head->wall_time;
	int64_t diff_cpu  = tail->cpu_time  - head->cpu_time;

	/* hack to elimiate noise. if diff_wall < 60s, we return 1. If command runs
	 * for more than 60s, the average cpu/wall serves as a fallback in the
	 * final summary. */

	if(diff_wall < 60) {
		return 1;
	} else {
		return (int64_t) MAX(1, ceil( ((double) diff_cpu)/diff_wall));
	}
}

void rmonitor_collate_tree(struct rmsummary *tr, struct rmonitor_process_info *p, struct rmonitor_mem_info *m, struct rmonitor_wdir_info *d, struct rmonitor_filesys_info *f)
{
	tr->wall_time  = usecs_since_epoch() - summary->start;
	tr->cpu_time   = p->cpu.delta + tr->cpu_time;

	tr->cores      = peak_cores(tr->wall_time, tr->cpu_time);

	tr->cores_avg  = 0;
	if(tr->wall_time > 0) {
		tr->cores_avg = (tr->cpu_time * 1000) / tr->wall_time;
	}

	tr->max_concurrent_processes = (int64_t) itable_size(processes);
	tr->total_processes          = summary->total_processes;

	/* we use max here, as /proc/pid/smaps that fills *m is not always
	 * available. This causes /proc/pid/status to become a conservative
	 * fallback. */
	if(m->resident > 0) {
		tr->virtual_memory    = (int64_t) m->virtual;
		tr->memory   = (int64_t) m->resident;
		tr->swap_memory       = (int64_t) m->swap;
	}
	else {
		tr->virtual_memory    = (int64_t) p->mem.virtual;
		tr->memory   = (int64_t) p->mem.resident;
		tr->swap_memory       = (int64_t) p->mem.swap;
	}

	tr->bytes_read        = (int64_t) (p->io.delta_chars_read + tr->bytes_read);
	tr->bytes_read       += (int64_t)  p->io.delta_bytes_faulted;
	tr->bytes_written     = (int64_t) (p->io.delta_chars_written + tr->bytes_written);

	tr->bytes_received = total_bytes_rx;
	tr->bytes_sent     = total_bytes_tx;

	tr->bandwidth = average_bandwidth(1);

	tr->total_files = (int64_t) d->files;
	tr->disk = (int64_t) (d->byte_count + ONE_MEGABYTE - 1) / ONE_MEGABYTE;

	tr->fs_nodes          = (int64_t) f->disk.f_ffree;
}

void rmonitor_find_max_tree(struct rmsummary *result, struct rmsummary *tr)
{
    if(!tr)
        return;

    rmsummary_merge_max_w_time(result, tr);

	/* if we are running with the --sh option, we subtract one process (the sh process). */
	if(sh_cmd_line) {
		result->max_concurrent_processes--;
	}
}

void rmonitor_log_row(struct rmsummary *tr)
{
	if(log_series)
	{
		fprintf(log_series,  "%" PRId64, tr->wall_time + summary->start);
		fprintf(log_series, " %" PRId64, tr->cpu_time);
		fprintf(log_series, " %" PRId64, tr->cores);
		fprintf(log_series, " %" PRId64, tr->max_concurrent_processes);
		fprintf(log_series, " %" PRId64, tr->virtual_memory);
		fprintf(log_series, " %" PRId64, tr->memory);
		fprintf(log_series, " %" PRId64, tr->swap_memory);
		fprintf(log_series, " %" PRId64, tr->bytes_read);
		fprintf(log_series, " %" PRId64, tr->bytes_written);
		fprintf(log_series, " %" PRId64, tr->bytes_received);
		fprintf(log_series, " %" PRId64, tr->bytes_sent);
		fprintf(log_series, " %" PRId64, tr->bandwidth);

		if(resources_flags->disk)
		{
			fprintf(log_series, " %" PRId64, tr->total_files);
			fprintf(log_series, " %" PRId64, tr->disk);
		}

		fprintf(log_series, "\n");

		fflush(log_series);
		fsync(fileno(log_series));

		/* are we going to keep monitoring the whole filesystem? */
		// fprintf(log_series "%" PRId64 "\n", tr->fs_nodes);
	}

	debug(D_RMON, "resources: %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 " %" PRId64 "% " PRId64 "\n", tr->wall_time + summary->start, tr->cpu_time, tr->max_concurrent_processes, tr->virtual_memory, tr->memory, tr->swap_memory, tr->bytes_read, tr->bytes_written, tr->bytes_received, tr->bytes_sent, tr->total_files, tr->disk);

}

int record_snapshot(struct rmsummary *tr) {
	char label[1024];

	if(!snapshot_signal_file) {
		return 0;
	}

	FILE *snap_f = fopen(snapshot_signal_file, "r");
	if(!snap_f) {
		/* signal is unavailable, so no snapshot is taken. */
		return 0;
	}

	if(!snapshots) {
		snapshots = list_create(0);
	}

	label[0]='\0';
	fgets(label, 1024, snap_f);
	fclose(snap_f);
	unlink(snapshot_signal_file);

	string_chomp(label);

	if(label[0] == '\0') {
		snprintf(label, 1024, "snapshot %d", list_size(snapshots) + 1);
	}

	snapshot->end       = usecs_since_epoch();
	snapshot->wall_time = snapshot->end - snapshot->start;

	struct jx *j = rmsummary_to_json(tr, /* only resources */ 1);

	jx_insert_string(j, "snapshot_name", label);

	if(!j) {
		return 0;
	}

	char *output_file = string_format("%s.snapshot.%02d", template_path, list_size(snapshots));
	snap_f = fopen(output_file, "w");
	free(output_file);

	if(!snap_f) {
		return 0;
	}

	jx_print_stream(j, snap_f);
	fclose(snap_f);

	/* push to the front, since snapshots are writen in reverse order. */
	list_push_head(snapshots, j);

	debug(D_RMON, "Recoded snapshot: '%s'", label);

	return 1;
}

void decode_zombie_status(struct rmsummary *summary, int wait_status)
{
	/* update from any END_WAIT message received. */
	summary->exit_status = first_process_exit_status;

	if ( WIFSIGNALED(wait_status) || WIFSTOPPED(wait_status) )
	{
		debug(D_RMON, "process %d terminated: %s.\n",
		      first_process_pid,
		      strsignal(WIFSIGNALED(wait_status) ? WTERMSIG(wait_status) : WSTOPSIG(wait_status)));

		summary->exit_type   = xxstrdup("signal");

		if(WIFSIGNALED(wait_status))
			summary->signal    = WTERMSIG(wait_status);
		else
			summary->signal    = WSTOPSIG(wait_status);

		summary->exit_status   = 128 + summary->signal;
	} else {
		debug(D_RMON, "process %d finished: %d.\n", first_process_pid, WEXITSTATUS(wait_status));
		summary->exit_type = xxstrdup("normal");
		summary->exit_status = WEXITSTATUS(wait_status);
	}

	if(summary->limits_exceeded)
	{
		free(summary->exit_type);
		summary->exit_type   = xxstrdup("limits");
		summary->exit_status = 128 + SIGTERM;
	}
}

void rmonitor_find_files_final_sizes() {
		char *fname;
		struct stat buf;
		struct rmonitor_file_info *finfo;

		hash_table_firstkey(files);
		while(hash_table_nextkey(files, &fname, (void **) &finfo))
		{
			/* If size_on_close is unknwon, perform a stat on the file. */

			if(finfo->size_on_close < 0 && stat(fname, &buf) == 0) {
				finfo->size_on_close = buf.st_size;
			}
		}
}

void rmonitor_add_files_to_summary(char *field, int outputs) {
		char *fname;
		struct rmonitor_file_info *finfo;

		buffer_t b;

		buffer_init(&b);
		buffer_putfstring(&b,  "%-15s[\n", field);

		char *delimeter = "";

		hash_table_firstkey(files);
		while(hash_table_nextkey(files, &fname, (void **) &finfo))
		{
			if(finfo->is_output != outputs)
				continue;

			int64_t file_size = MAX(finfo->size_on_open, finfo->size_on_close);

			if(file_size < 0) {
				debug(D_NOTICE, "Could not find size of file %s\n", fname);
				continue;
			}

			buffer_putfstring(&b, "%s%20s\"%s\", %" PRId64 " ]", delimeter, "[ ", fname, (int64_t) ceil(1.0*file_size/ONE_MEGABYTE));
			delimeter = ",\n";
		}

		buffer_putfstring(&b,  "\n%16s", "]");

		add_verbatim_field(buffer_tostring(&b));

		buffer_free(&b);
}

int rmonitor_file_io_summaries()
{
#if defined(RESOURCE_MONITOR_USE_INOTIFY)
	if (rmonitor_inotify_fd >= 0)
	{
		char *fname;
		struct rmonitor_file_info *finfo;

		fprintf(log_inotify, "%-15s\n%-15s %6s %20s %20s %6s %6s %6s %6s\n",
			"#path", "#", "device", "size_initial(B)", "size_final(B)", "opens", "closes", "reads", "writes");

		hash_table_firstkey(files);
		while(hash_table_nextkey(files, &fname, (void **) &finfo))
		{
			fprintf(log_inotify, "%-15s\n%-15s ", fname, "");
			fprintf(log_inotify, "%6" PRId64 " %20lld %20lld",
				finfo->device,
				(long long int) finfo->size_on_open,
				(long long int) finfo->size_on_close);
			fprintf(log_inotify, " %6" PRId64 " %6" PRId64,
				finfo->n_opens,
				finfo->n_closes);
			fprintf(log_inotify, " %6" PRId64 " %6" PRId64 "\n",
				finfo->n_reads,
				finfo->n_writes);
		}
	}
#endif
	return 0;
}

int rmonitor_final_summary()
{
	decode_zombie_status(summary, first_process_sigchild_status);

	char *monitor_self_info = string_format("monitor_version:%9s %d.%d.%d.%.8s", "", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, CCTOOLS_COMMIT);
	add_verbatim_field(monitor_self_info);

	char hostname[DOMAIN_NAME_MAX];

	char *host_info = NULL;
	if(domain_name_cache_guess(hostname)) {
		host_info = string_format("host:%s", hostname);
		add_verbatim_field(host_info);
	}

	if(snapshots && list_size(snapshots) > 0) {
		add_snapshots();
	}

	if(log_inotify)
	{
		rmonitor_find_files_final_sizes();
		rmonitor_add_files_to_summary("input_files:",  0);
		rmonitor_add_files_to_summary("output_files:", 1);

        int nfds = rmonitor_inotify_fd + 1;
        int count = 0;

		struct timeval timeout;
		fd_set rset;
		do
		{
			timeout.tv_sec   = 0;
			timeout.tv_usec  = 0;

			FD_ZERO(&rset);
			if (rmonitor_inotify_fd > 0)   FD_SET(rmonitor_inotify_fd, &rset);

			count = select(nfds, &rset, NULL, NULL, &timeout);

			if(count > 0)
				if (FD_ISSET(rmonitor_inotify_fd, &rset)) {
					rmonitor_handle_inotify();
				}
		} while(count > 0);

		rmonitor_file_io_summaries();
	}

	rmsummary_print(log_summary, summary, pprint_summaries, verbatim_summary_fields);

	if(monitor_self_info)
		free(monitor_self_info);

	if(host_info)
		free(host_info);

	int status;

	if(summary->limits_exceeded) {
		status = RM_OVERFLOW;
	} else if(summary->exit_status != 0) {
		status = RM_TASK_ERROR;
	} else {
		status = RM_SUCCESS;
	}

	return status;
}

/***
 * Functions that modify the processes tracking table, and
 * cleanup of processes in the zombie state.
 ***/

int ping_process(pid_t pid)
{
    return (kill(pid, 0) == 0);
}

void rmonitor_track_process(pid_t pid)
{
	char *newpath;
	struct rmonitor_process_info *p;

	if(!ping_process(pid))
		return;

	p = itable_lookup(processes, pid);

	if(p)
		return;

	p = malloc(sizeof(struct rmonitor_process_info));
	bzero(p, sizeof(struct rmonitor_process_info));

	p->pid = pid;
	p->running = 0;

	if(follow_chdir) {
		newpath = getcwd(NULL, 0);
		p->wd   = lookup_or_create_wd(NULL, newpath);
		free(newpath);
	}

	itable_insert(processes, p->pid, (void *) p);

	p->running = 1;
	p->waiting = 0;

	summary->total_processes++;
}

void rmonitor_untrack_process(uint64_t pid)
{
	struct rmonitor_process_info *p = itable_lookup(processes, pid);

	if(p)
		p->running = 0;
}

void cleanup_zombie(struct rmonitor_process_info *p)
{
  debug(D_RMON, "cleaning process: %d\n", p->pid);

  if(follow_chdir && p->wd)
    dec_wd_count(p->wd);

  itable_remove(processes, p->pid);
  free(p);
}

void cleanup_zombies(void)
{
  uint64_t pid;
  struct rmonitor_process_info *p;

  itable_firstkey(processes);
  while(itable_nextkey(processes, &pid, (void **) &p))
    if(!p->running)
      cleanup_zombie(p);
}

void release_waiting_process(uint64_t pid)
{
	debug(D_RMON, "sending SIGCONT to %" PRIu64 ".", pid);
	kill((pid_t) pid, SIGCONT);
}

void release_waiting_processes(void)
{
	uint64_t pid;
	struct rmonitor_process_info *p;

	itable_firstkey(processes);
	while(itable_nextkey(processes, &pid, (void **) &p))
		if(p->waiting)
			release_waiting_process(pid);
}

void ping_processes(void)
{
    uint64_t pid;
    struct rmonitor_process_info *p;

    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
        if(!ping_process(pid))
        {
            debug(D_RMON, "cannot find %"PRId64" process.\n", pid);
            rmonitor_untrack_process(pid);
        }
}

struct rmsummary *rmonitor_final_usage_tree(void)
{
    struct rusage usg;
    struct rmsummary *tr_usg = rmsummary_create(-1);

    debug(D_RMON, "calling getrusage.\n");

    if(getrusage(RUSAGE_CHILDREN, &usg) != 0)
    {
        debug(D_RMON, "getrusage failed: %s\n", strerror(errno));
        return NULL;
    }

	if(usg.ru_majflt > 0) {
		/* Here we add the maximum recorded + the io from memory maps */
		tr_usg->bytes_read     =  summary->bytes_read + usg.ru_majflt * sysconf(_SC_PAGESIZE);
		debug(D_RMON, "page faults: %ld.\n", usg.ru_majflt);
	}

	tr_usg->cpu_time  = 0;
	tr_usg->cpu_time += usg.ru_utime.tv_sec*USECOND + usg.ru_utime.tv_usec;
	tr_usg->cpu_time += usg.ru_stime.tv_sec*USECOND + usg.ru_stime.tv_usec;
	tr_usg->end       = usecs_since_epoch();
	tr_usg->wall_time = tr_usg->end - summary->start;

	/* we do not use peak_cores here, as we may have missed some threads which
	 * make cpu_time quite jumpy. */
	tr_usg->cores     = MAX(1, ceil( ((double) tr_usg->cpu_time)/tr_usg->wall_time));

	tr_usg->bandwidth      = average_bandwidth(0);
	tr_usg->bytes_received = total_bytes_rx;
	tr_usg->bytes_sent     = total_bytes_tx;

    return tr_usg;
}

/* sigchild signal handler */
void rmonitor_check_child(const int signal)
{
    uint64_t pid = waitpid(first_process_pid, &first_process_sigchild_status, WNOHANG | WCONTINUED | WUNTRACED);

    if(pid != (uint64_t) first_process_pid)
	    return;

    debug(D_RMON, "SIGCHLD from %d : ", first_process_pid);

    if(WIFEXITED(first_process_sigchild_status))
    {
        debug(D_RMON, "exit\n");
    }
    else if(WIFSIGNALED(first_process_sigchild_status))
    {
      debug(D_RMON, "signal\n");
    }
    else if(WIFSTOPPED(first_process_sigchild_status))
    {
      debug(D_RMON, "stop\n");

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
      debug(D_RMON, "continue\n");
      return;
    }

    first_process_already_waited = 1;

    struct rmonitor_process_info *p;
    debug(D_RMON, "adding all processes to cleanup list.\n");
    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
      rmonitor_untrack_process(pid);

    /* get the peak values from getrusage, and others. */
    struct rmsummary *tr_usg = rmonitor_final_usage_tree();
    rmonitor_find_max_tree(summary, tr_usg);
    free(tr_usg);
}

void cleanup_library() {
	unlink(lib_helper_name);
}

//SIGINT, SIGQUIT, SIGTERM signal handler.
void rmonitor_final_cleanup(int signum)
{
    uint64_t pid;
    struct   rmonitor_process_info *p;
    int      status;

	static int handler_already_running = 0;

	if(handler_already_running)
		return;
	handler_already_running = 1;

    signal(SIGCHLD, rmonitor_check_child);

    //ask politely to quit
    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
    {
        debug(D_RMON, "sending %s(%d) to process %"PRId64".\n", strsignal(signum), signum, pid);

        kill(pid, signum);
    }

	/* wait for processes to cleanup. We wait 5 seconds, but no more than 0.2 seconds at a time. */
	int count = 25;
	do{
		usleep(200000);
		ping_processes();
		cleanup_zombies();
		count--;
	} while(itable_size(processes) > 0 && count > 0);

    if(!first_process_already_waited)
	    rmonitor_check_child(signum);

    signal(SIGCHLD, SIG_DFL);

    //we did ask...
    itable_firstkey(processes);
    while(itable_nextkey(processes, &pid, (void **) &p))
    {
        debug(D_RMON, "sending %s(%d) to process %"PRId64".\n", strsignal(SIGKILL), SIGKILL, pid);

        kill(pid, SIGKILL);

        rmonitor_untrack_process(pid);
    }

    cleanup_zombies();

    if(lib_helper_extracted) {
		cleanup_library();
		lib_helper_extracted = 0;
	}

    status = rmonitor_final_summary();

	fclose(log_summary);

    if(log_series)
	    fclose(log_series);
    if(log_inotify)
	    fclose(log_inotify);

    exit(status);
}

#define over_limit_check(tr, fld)\
	if(resources_limits->fld > -1 && (tr)->fld > 0 && resources_limits->fld - (tr)->fld < 0)\
	{\
		debug(D_RMON, "Limit " #fld " broken.\n");\
		if(!(tr)->limits_exceeded) { (tr)->limits_exceeded = rmsummary_create(-1); }\
		(tr)->limits_exceeded->fld = resources_limits->fld;\
	}

/* return 0 means above limit, 1 means limist ok */
int rmonitor_check_limits(struct rmsummary *tr)
{
	tr->limits_exceeded = NULL;

	/* Consider errors as resources exhausted. Used for ENOSPC, ENFILE, etc. */
	if(tr->last_error)
		return 0;

	if(!resources_limits)
		return 1;

	over_limit_check(tr, start);
	over_limit_check(tr, end);
	over_limit_check(tr, cores);
	over_limit_check(tr, wall_time);
	over_limit_check(tr, cpu_time);
	over_limit_check(tr, max_concurrent_processes);
	over_limit_check(tr, total_processes);
	over_limit_check(tr, virtual_memory);
	over_limit_check(tr, memory);
	over_limit_check(tr, swap_memory);
	over_limit_check(tr, bytes_read);
	over_limit_check(tr, bytes_written);
	over_limit_check(tr, bytes_received);
	over_limit_check(tr, bytes_sent);
	over_limit_check(tr, total_files);
	over_limit_check(tr, disk);

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

	atexit(cleanup_library);
}

/* return 1 if urgent message (wait, branch), 0 otherwise) */
int rmonitor_dispatch_msg(void)
{
	struct rmonitor_msg msg;
	struct rmonitor_process_info *p;

	int recv_status = recv_monitor_msg(rmonitor_queue_fd, &msg);

	if(recv_status < 0 || ((unsigned int) recv_status) < sizeof(msg)) {
		debug(D_RMON, "Malformed message from monitored processes. Ignoring.");
		return 1;
	}

	//Next line commented: Useful for detailed debugging, but too spammy for regular operations.
	//debug(D_RMON,"message '%s' (%d) from %d with status '%s' (%d)\n", str_msgtype(msg.type), msg.type, msg.origin, strerror(msg.error), msg.error);

	p = itable_lookup(processes, (uint64_t) msg.origin);

	if(!p)
	{
		/* We either got a malformed message, message from a
		process we are not tracking anymore, or a message from
		a newly created process.  */
		if( msg.type == END_WAIT )
        {
			release_waiting_process(msg.origin);
			return 1;
        }
		else if(msg.type != BRANCH)
			return 1;
	}

    switch(msg.type)
    {
        case BRANCH:
			msg.error = 0;
            rmonitor_track_process(msg.origin);
            if(summary->max_concurrent_processes < itable_size(processes))
                summary->max_concurrent_processes = itable_size(processes);
            break;
        case END_WAIT:
			msg.error = 0;
            p->waiting = 1;
			if(msg.origin == first_process_pid)
				first_process_exit_status = msg.data.n;
            break;
        case END:
			msg.error = 0;
            rmonitor_untrack_process(msg.origin);
            break;
        case CHDIR:
			msg.error = 0;
			if(follow_chdir)
				p->wd = lookup_or_create_wd(p->wd, msg.data.s);
            break;
		case OPEN_INPUT:
		case OPEN_OUTPUT:
			switch(msg.error) {
				case 0:
					debug(D_RMON, "File %s has been opened.\n", msg.data.s);
					if(log_inotify) {
						rmonitor_add_file_watch(msg.data.s, msg.type == OPEN_OUTPUT, 0);
					}
					break;
				case EMFILE:
					/* Eventually report that we ran out of file descriptors. */
					debug(D_RMON, "Process %d ran out of file descriptors.\n", msg.origin);
					break;
				default:
					/* Clear the error, as it is not related to resources. */
					msg.error = 0;
					break;
			}
			break;
		case RX:
			msg.error = 0;
			if(msg.data.n > 0) {
				total_bytes_rx += msg.data.n;
				append_network_bw(&msg);
			}
			break;
		case TX:
			msg.error = 0;
			if(msg.data.n > 0) {
				total_bytes_tx += msg.data.n;
				append_network_bw(&msg);
			}
			break;
        case READ:
			msg.error = 0;
			break;
        case WRITE:
			switch(msg.error) {
				case ENOSPC:
					/* Eventually report that we ran out of space. */
					debug(D_RMON, "Process %d ran out of disk space.\n", msg.origin);
					break;
				default:
					/* Clear the error, as it is not related to resources. */
					msg.error = 0;
					break;
			}
            break;
        default:
            break;
    };

	summary->last_error = msg.error;

	if(!rmonitor_check_limits(summary))
		rmonitor_final_cleanup(SIGTERM);

	if(msg.type == BRANCH || msg.type == END_WAIT || msg.type == END) {
		return 1;
	} else {
		return 0;
	}
}

int wait_for_messages(int interval)
{
	struct timeval timeout;
	timeout.tv_sec   = interval;
	timeout.tv_usec  = 0;

	debug(D_RMON, "sleeping for: %d seconds\n", interval);

	//If grandchildren processes cannot talk to us, simply wait.
	//Else, wait, and check socket for messages.
	if (rmonitor_queue_fd < 0)
	{
		/* wait for interval. */
		select(1, NULL, NULL, NULL, &timeout);
	}
	else
	{

		/* Figure out the number of file descriptors to pass to select */
		int nfds = 1 + MAX(rmonitor_queue_fd, rmonitor_inotify_fd);
		fd_set rset;

		int urgent = 0;
		int count  = 0;
		do
		{
			FD_ZERO(&rset);
			if (rmonitor_queue_fd > 0) {
				FD_SET(rmonitor_queue_fd,   &rset);
			}

			if (rmonitor_inotify_fd > 0) {
				FD_SET(rmonitor_inotify_fd, &rset);
			}

			count = select(nfds, &rset, NULL, NULL, &timeout);

			if (FD_ISSET(rmonitor_queue_fd, &rset)) {
				urgent = rmonitor_dispatch_msg();
			}

			if (FD_ISSET(rmonitor_inotify_fd, &rset)) {
				urgent = rmonitor_handle_inotify();
			}

			if(urgent) {
				timeout.tv_sec  = 0;
				timeout.tv_usec = 0;
			}
		} while(count > 0);
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

pid_t rmonitor_fork(void)
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
        debug(D_RMON, "fork %d -> %d\n", getpid(), pid);

        rmonitor_track_process(pid);

		/* if we are running with the --sh option, we subtract one process (the sh process). */
		if(sh_cmd_line) {
			summary->total_processes--;
		}

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

struct rmonitor_process_info *spawn_first_process(const char *executable, char *argv[], int child_in_foreground)
{
    pid_t pid;

    pid = rmonitor_fork();

    rmonitor_summary_header();

    if(pid > 0)
    {
        first_process_pid = pid;
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        setpgid(pid, 0);

        if (child_in_foreground)
        {
			signal(SIGTTOU, SIG_IGN);
            int fdtty, retc;
            fdtty = open("/dev/tty", O_RDWR);
            if (fdtty >= 0)
            {
                /* Try bringing the child process to the session foreground */
                retc = tcsetpgrp(fdtty, getpgid(pid));
                if (retc < 0)
				{
					debug(D_FATAL, "error bringing process to the session foreground (tcsetpgrp): %s\n", strerror(errno));
					exit(RM_MONITOR_ERROR);
				}
				close(fdtty);
			} else {
				debug(D_FATAL, "error accessing controlling terminal (/dev/tty): %s\n", strerror(errno));
				exit(RM_MONITOR_ERROR);
			}
        }

		char *executable_path = path_which(executable);
		if(executable_path) {
			rmonitor_add_file_watch(executable_path, /* is output? */ 0, 0);
			free(executable_path);
		}
    }
    else if(pid < 0) {
		debug(D_FATAL, "fork failed: %s\n", strerror(errno));
		exit(RM_MONITOR_ERROR);
	}
    else //child
    {
        setpgid(0, 0);

        debug(D_RMON, "executing: %s\n", executable);

		errno = 0;
        execvp(executable, argv);
        //We get here only if execlp fails.
		int exec_errno = errno;
        debug(D_RMON, "error executing %s: %s\n", executable, strerror(errno));

		exit(exec_errno);
    }

    return itable_lookup(processes, pid);

}


static void show_help(const char *cmd)
{
    fprintf(stdout, "\nUse: %s [options] -- command-line-and-options\n\n", cmd);
    fprintf(stdout, "%-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	fprintf(stdout, "%-30s Send debugging to this file. (can also be :stderr, :stdout, :syslog, or :journal)\n", "-o,--debug-file=<file>");
    fprintf(stdout, "%-30s Show this message.\n", "-h,--help");
    fprintf(stdout, "%-30s Show version string.\n", "-v,--version");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Interval between observations, in seconds. (default=%d)\n", "-i,--interval=<n>", DEFAULT_INTERVAL);
    fprintf(stdout, "%-30s Read command line from <str>, and execute as '/bin/sh -c <str>'\n", "-c,--sh=<str>");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Use maxfile with list of var: value pairs for resource limits.\n", "-l,--limits-file=<maxfile>");
    fprintf(stdout, "%-30s Use string of the form \"var: value, var: value\" to specify.\n", "-L,--limits=<string>");
    fprintf(stdout, "%-30s resource limits. Can be specified multiple times.\n", "");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Keep the monitored process in foreground (for interactive use).\n", "-f,--child-in-foreground");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Follow the size of processes' current working directories. \n", "--follow-chdir");
    fprintf(stdout, "%-30s Follow the size of <dir>. If not specified, follow the current directory.\n", "--measure-dir");
    fprintf(stdout, "%-30s Can be specified multiple times.\n", "");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Specify filename template for log files (default=resource-pid-<pid>)\n", "-O,--with-output-files=<file>");
    fprintf(stdout, "%-30s Write resource time series to <template>.series\n", "--with-time-series");
    fprintf(stdout, "%-30s Write inotify statistics of opened files to default=<template>.files\n", "--with-inotify");
    fprintf(stdout, "%-30s Include this string verbatim in a line in the summary. \n", "-V,--verbatim-to-summary=<str>");
    fprintf(stdout, "%-30s (Could be specified multiple times.)\n", "");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s Do not measure working directory footprint.\n", "--without-disk-footprint");
    fprintf(stdout, "%-30s Do not pretty-print summaries.\n", "--no-pprint");
    fprintf(stdout, "\n");
    fprintf(stdout, "%-30s If <file> exists at the end of a measurement interval, take a snapshot of\n", "--snapshot-file=<file>");
    fprintf(stdout, "%-30s current resources, and delete <file>. If <file> has a non-empty first\n", "");
    fprintf(stdout, "%-30s line, it is used as a label for the snapshot.\n", "");
}


int rmonitor_resources(long int interval /*in seconds */)
{
    uint64_t round;

    struct rmonitor_process_info *p_acc = calloc(1, sizeof(struct rmonitor_process_info)); //Automatic zeroed.
    struct rmonitor_wdir_info    *d_acc = calloc(1, sizeof(struct rmonitor_wdir_info));
    struct rmonitor_filesys_info *f_acc = calloc(1, sizeof(struct rmonitor_filesys_info));
    struct rmonitor_mem_info     *m_acc = calloc(1, sizeof(struct rmonitor_mem_info));

    struct rmsummary    *resources_now = calloc(1, sizeof(struct rmsummary));

    // Loop while there are processes to monitor, that is
    // itable_size(processes) > 0). The check is done again in a
    // if/break pair below to mitigate a race condition in which
	// the last process exits after the while(...) is tested, but
	// before we reach select.
	round = 1;
	while(itable_size(processes) > 0)
	{
		debug(D_RMON, "Round %" PRId64, round);
		activate_debug_log_if_file();

		resources_now->last_error = 0;

		ping_processes();

		rmonitor_poll_all_processes_once(processes, p_acc);
		rmonitor_poll_maps_once(processes, m_acc);

		if(resources_flags->disk)
			rmonitor_poll_all_wds_once(wdirs, d_acc, MAX(1, interval/(MAX(1, hash_table_size(wdirs)))));

		// rmonitor_fss_once(f); disabled until statfs fs id makes sense.

		rmonitor_collate_tree(resources_now, p_acc, m_acc, d_acc, f_acc);
		rmonitor_find_max_tree(summary,  resources_now);
		rmonitor_find_max_tree(snapshot, resources_now);
		rmonitor_log_row(resources_now);

		if(!rmonitor_check_limits(summary))
			rmonitor_final_cleanup(SIGTERM);

		release_waiting_processes();

		cleanup_zombies();

		//process snapshot
		if(record_snapshot(snapshot)) {
			rmsummary_delete(snapshot);
			snapshot = calloc(1, sizeof(*snapshot));
			snapshot->start = usecs_since_epoch();
		}

		//If no more process are alive, break out of loop.
		if(itable_size(processes) < 1)
			break;

		wait_for_messages(interval);

		//cleanup processes which by terminating may have awaken
		//select.
		cleanup_zombies();

		round++;
	}

    rmsummary_delete(resources_now);
    free(p_acc);
    free(m_acc);
    free(d_acc);
    free(f_acc);

    return 0;
}

int main(int argc, char **argv) {
    int i;
    char *command_line;
    char *executable;
    int64_t c;

    char *summary_path = NULL;
    char *series_path  = NULL;
    char *opened_path  = NULL;

	char *sh_cmd_line = NULL;

    int use_series   = 0;
    int use_inotify  = 0;
    int child_in_foreground = 0;

    debug_config(argv[0]);

    signal(SIGCHLD, rmonitor_check_child);
    signal(SIGINT,  rmonitor_final_cleanup);
    signal(SIGQUIT, rmonitor_final_cleanup);
    signal(SIGTERM, rmonitor_final_cleanup);

    summary  = calloc(1, sizeof(struct rmsummary));
    snapshot = calloc(1, sizeof(struct rmsummary));

    summary->peak_times = rmsummary_create(-1);
    resources_limits = rmsummary_create(-1);
    resources_flags  = rmsummary_create(0);

	total_bytes_rx = 0;
	total_bytes_tx = 0;
	tx_rx_sizes    = list_create();

    rmsummary_read_env_vars(resources_limits);

    processes = itable_create(0);
    wdirs     = hash_table_create(0,0);
    filesysms = itable_create(0);
    files     = hash_table_create(0,0);

    wdirs_rc   = itable_create(0);
    filesys_rc = itable_create(0);

	char *cwd = getcwd(NULL, 0);

	enum {
		LONG_OPT_TIME_SERIES = UCHAR_MAX+1,
		LONG_OPT_OPENED_FILES,
		LONG_OPT_DISK_FOOTPRINT,
		LONG_OPT_NO_DISK_FOOTPRINT,
		LONG_OPT_SH_CMDLINE,
		LONG_OPT_WORKING_DIRECTORY,
		LONG_OPT_FOLLOW_CHDIR,
		LONG_OPT_MEASURE_DIR,
		LONG_OPT_NO_PPRINT,
		LONG_OPT_SNAPSHOT_FILE
	};

    static const struct option long_options[] =
	    {
		    /* Regular Options */
		    {"debug",      required_argument, 0, 'd'},
		    {"debug-file", required_argument, 0, 'o'},
		    {"help",       required_argument, 0, 'h'},
		    {"version",    no_argument,       0, 'v'},
		    {"interval",   required_argument, 0, 'i'},
		    {"limits",     required_argument, 0, 'L'},
		    {"limits-file",required_argument, 0, 'l'},
		    {"sh",         required_argument, 0, 'c'},

		    {"verbatim-to-summary",required_argument, 0, 'V'},

		    {"follow-chdir", no_argument,       0,  LONG_OPT_FOLLOW_CHDIR},
		    {"measure-dir",  required_argument, 0,  LONG_OPT_MEASURE_DIR},
		    {"no-pprint",    no_argument,       0,  LONG_OPT_NO_PPRINT},

		    {"with-output-files",      required_argument, 0,  'O'},
		    {"with-time-series",       no_argument, 0, LONG_OPT_TIME_SERIES},
		    {"with-inotify",           no_argument, 0, LONG_OPT_OPENED_FILES},
		    {"without-disk-footprint", no_argument, 0, LONG_OPT_NO_DISK_FOOTPRINT},

		    {"snapshot-file", required_argument, 0, LONG_OPT_SNAPSHOT_FILE},

		    {0, 0, 0, 0}
	    };


	/* By default, measure working directory. */
	resources_flags->disk = 1;

	/* Used in LONG_OPT_MEASURE_DIR */
	char measure_dir_name[PATH_MAX];

    while((c = getopt_long(argc, argv, "c:d:fhi:L:l:o:O:vV:", long_options, NULL)) >= 0)
    {
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'o':
				debug_config_file(optarg);
				debug_config_file_size(0);
				break;
			case 'h':
				show_help(argv[0]);
				return 0;
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			case 'c':
				sh_cmd_line = xxstrdup(optarg);
				break;
			case 'i':
				interval = strtoll(optarg, NULL, 10);
				if(interval < 1) {
					debug(D_FATAL, "interval cannot be set to less than one second.");
					exit(RM_MONITOR_ERROR);
				}
				break;
			case 'l':
				parse_limits_file(resources_limits, optarg);
				break;
			case 'L':
				parse_limit_string(resources_limits, optarg);
				break;
			case 'V':
				add_verbatim_field(optarg);
				break;
			case 'f':
				child_in_foreground = 1;
				break;
			case 'O':
				if(template_path)
					free(template_path);
				template_path = xxstrdup(optarg);
				break;
			case  LONG_OPT_TIME_SERIES:
				use_series  = 1;
				break;
			case  LONG_OPT_OPENED_FILES:
				use_inotify = 1;
				break;
			case LONG_OPT_NO_DISK_FOOTPRINT:
				resources_flags->disk = 0;
				break;
			case LONG_OPT_FOLLOW_CHDIR:
				follow_chdir = 1;
				break;
			case LONG_OPT_MEASURE_DIR:
				path_absolute(optarg, measure_dir_name, 0);
				if(!lookup_or_create_wd(NULL, measure_dir_name)) {
					debug(D_FATAL, "Directory '%s' does not exist.", optarg);
					exit(RM_MONITOR_ERROR);
				}
				break;
			case LONG_OPT_NO_PPRINT:
				pprint_summaries = 0;
				break;
			case LONG_OPT_SNAPSHOT_FILE:
				snapshot_signal_file = xxstrdup(optarg);
				break;
			default:
				show_help(argv[0]);
				return 1;
				break;
		}
	}

	if( follow_chdir && hash_table_size(wdirs) > 0) {
		debug(D_FATAL, "Options --follow-chdir and --measure-dir as mutually exclusive.");
		exit(RM_MONITOR_ERROR);
	}

    rmsummary_debug_report(resources_limits);

	//this is ugly. if -c given, we should not accept any more arguments.
	// if not given, we should get the arguments that represent the command line.
	if((optind < argc && sh_cmd_line) || (optind >= argc && !sh_cmd_line)) {
		show_help(argv[0]);
		return 1;
	}

	if(sh_cmd_line) {
		argc = 3;
		optind = 0;

		char *argv_sh[] = { "/bin/sh", "-c", sh_cmd_line, 0 };
		argv = argv_sh;

		/* for pretty printing in the summary. */
		command_line = sh_cmd_line;

		char *sh_cmd_line_exec_escaped = string_escape_shell(sh_cmd_line);
		debug(D_RMON, "command line: /bin/sh -c %s\n", sh_cmd_line_exec_escaped);
		free(sh_cmd_line_exec_escaped);
	}
	else {
		buffer_t b;
		buffer_init(&b);

		char *sep = "";
		for(i = optind; i < argc; i++)
		{
			buffer_printf(&b, "%s%s", sep, argv[i]);
			sep = " ";
		}

		command_line = xxstrdup(buffer_tostring(&b));
		buffer_free(&b);

		debug(D_RMON, "command line: %s\n", command_line);
	}



    if(getenv(RESOURCE_MONITOR_INFO_ENV_VAR))
    {
        debug(D_NOTICE, "using upstream monitor. executing: %s\n", command_line);
        execlp("/bin/sh", "sh", "-c", command_line, (char *) NULL);
        //We get here only if execlp fails.
        fatal("error executing %s: %s\n", command_line, strerror(errno));
    }

    write_helper_lib();
    rmonitor_helper_init(lib_helper_name, &rmonitor_queue_fd);

	summary_path = default_summary_name(template_path);

    if(use_series)
        series_path = default_series_name(template_path);

    if(use_inotify)
        opened_path = default_opened_name(template_path);

    log_summary = open_log_file(summary_path);
    log_series  = open_log_file(series_path);
    log_inotify = open_log_file(opened_path);

    summary->command = xxstrdup(command_line);
    summary->start   = usecs_since_epoch();
    snapshot->start  = summary->start;

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
    if(log_inotify || snapshot_signal_file)
    {
	    rmonitor_inotify_fd     = inotify_init();
	    alloced_inotify_watches = 100;
	    inotify_watches         = (char **) calloc(alloced_inotify_watches, sizeof(char *));
	}

	if(snapshot_signal_file) {
		char *full_path = malloc(PATH_MAX);

		path_absolute(snapshot_signal_file, full_path, 0);

		char *dir  = malloc(PATH_MAX);
		path_dirname(full_path, dir);

		free(snapshot_signal_file);
		snapshot_signal_file = xxstrdup(path_basename(full_path));

		rmonitor_add_file_watch(dir, 0, IN_CREATE);

		free(full_path);
    }
#endif

	/* if we are not following changes in directory, and no directory was manually added, we follow the current working directory. */
	if(!follow_chdir || hash_table_size(wdirs) == 0) {
		lookup_or_create_wd(NULL, cwd);
	}

	executable = xxstrdup(argv[optind]);

	if( rmonitor_determine_exec_type(executable) ) {
		debug(D_FATAL, "Error reading %s.", executable);
		exit(RM_MONITOR_ERROR);
	}

	spawn_first_process(executable, argv + optind, child_in_foreground);
    rmonitor_resources(interval);
    rmonitor_final_cleanup(SIGTERM);

	/* rmonitor_final_cleanup exits */
    return 0;
}


/* vim: set noexpandtab tabstop=4: */
