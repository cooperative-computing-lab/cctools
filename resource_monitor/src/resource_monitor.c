/*
Copyright (C) 2022 The University of Notre Dame
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
 * wall:          wall time (in secs).
 * no.proc:       number of processes
 * cpu-time:      user-mode time + kernel-mode time.
 * vmem:          current total memory size (virtual).
 * rss:           current total resident size.
 * swap:          current total swap usage.
 * bytes_read:    read chars count using *read system calls from disk. (in MB)
 * bytes_written: writen char count using *write system calls to disk. (in MB)
 * bytes_received:total bytes received (recv family) (in MB)
 * bytes_sent:    total bytes sent     (send family) (in MB)
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

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <time.h>
#include <unistd.h>

#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/wait.h>

#include <inttypes.h>
#include <sys/types.h>

#include "buffer.h"
#include "catalog_query.h"
#include "cctools.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "domain_name_cache.h"
#include "elfheader.h"
#include <getopt.h>
#include "hash_table.h"
#include "itable.h"
#include "jx.h"
#include "jx_parse.h"
#include "jx_pretty_print.h"
#include "jx_print.h"
#include "list.h"
#include "macros.h"
#include "path.h"
#include "random.h"
#include "stringtools.h"
#include "uuid.h"
#include "xxmalloc.h"

#include "rmonitor.h"
#include "rmonitor_file_watch.h"
#include "rmonitor_poll_internal.h"

#define RESOURCE_MONITOR_USE_INOTIFY 1
#if defined(RESOURCE_MONITOR_USE_INOTIFY)
#include <sys/inotify.h>
#include <sys/ioctl.h>
#endif

#include "rmonitor_helper_comm.h"
#include "rmonitor_piggyback.h"

#define DEFAULT_INTERVAL 5		   /* in seconds */
#define DEFAULT_LOG_NAME "resource-pid-%d" /* %d is used for the value of getpid() */

#define ACTIVATE_DEBUG_FILE ".cctools_resource_monitor_debug"

uint64_t interval = DEFAULT_INTERVAL;

char *summary_path = NULL; /* name of the summary file */
FILE *log_summary = NULL;  /* Final statistics are written to this file (FILE * to summary_path). */
FILE *log_series = NULL;   /* Resource events and samples are written to this file. */
FILE *log_inotify = NULL;  /* List of opened files is written to this file. */

char *template_path = NULL; /* Prefix of all output files names */

int debug_active = 0;	/* 1 if ACTIVATE_DEBUG_FILE exists. If 1, debug info goes to ACTIVATE_DEBUG_FILE ".log" */
int enforce_limits = 1; /* 0 if monitor should only measure, 1 if enforcing resources limits. */

char hostname[DOMAIN_NAME_MAX]; /* current hostname */

struct jx *verbatim_summary_fields; /* fields added to the summary without change */

int rmonitor_queue_fd = -1; /* File descriptor of a datagram socket to which (great)
			      grandchildren processes report to the monitor. */
static int rmonitor_inotify_fd = -1;

pid_t first_process_pid;	      /* pid of the process given at the command line. */
int first_process_sigchild_status;    /* exit status flags of the process given at the command line */
int first_process_already_waited = 0; /* exit status flags of the process given at the command line */
int first_process_exit_status = 0;
int first_pid_manually_set = 0; /* whether --pid was used */

struct itable *processes; /* Maps the pid of a process to a unique struct rmonitor_process_info. */
struct hash_table *wdirs; /* Maps paths to working directory structures. */
struct itable *filesysms; /* Maps st_dev ids (from stat syscall) to filesystem structures. */
struct hash_table *files; /* Keeps track of which files have been opened. */

static int follow_chdir = 0;	 /* Keep track of all the working directories per process. */
static int pprint_summaries = 1; /* Pretty-print json summaries. */

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
static char **inotify_watches; /* Keeps track of created inotify watches. */
static int alloced_inotify_watches = 0;
#endif

static int stop_short_running = 0; /* Stop to analyze process that run for less than RESOURCE_MONITOR_SHORT_TIME
				      seconds. By default such processes are not stopped. */

struct itable *wdirs_rc;   /* Counts how many rmonitor_process_info use a rmonitor_wdir_info. */
struct itable *filesys_rc; /* Counts how many rmonitor_wdir_info use a rmonitor_filesys_info. */

char *lib_helper_name = NULL; /* Name of the helper library that is
				 automatically extracted */

int lib_helper_extracted; /* Boolean flag to indicate whether the bundled
			     helper library was automatically extracted
			     */

struct rmsummary *summary;  /* final summary */
struct rmsummary *snapshot; /* current snapshot */
struct rmsummary *resources_limits;
struct rmsummary *resources_flags;

struct list *tx_rx_sizes; /* list of network byte counts with a timestamp, to compute bandwidth. */
int64_t total_bytes_rx;	  /* total bytes received */
int64_t total_bytes_tx;	  /* total bytes sent */

const char *sh_cmd_line = NULL; /* command line passed with the --sh option. */

char *snapshot_watch_events_file = NULL; /* name of the file with a jx document that looks like:
											{ "FILENAME" : [ { "pattern":"REGEXP", "label":"my
					    snapshot", "max_count":1}, "FILENAME2" : ... ] } A snapshot is generated when pattern matches a
					    line in the file FILENAME. */

size_t snapshots_allocated = 0; /* dynamic number of snapshot slots allocated */

struct list *snapshot_labels = NULL; /* list of labels for current snapshot. */

struct itable *snapshot_watch_pids; /* pid of all processes watching files for snapshots. */

static timestamp_t last_termination_signal_time = 0; /* Records the time a termination signal is received. */
static int fast_terminate_from_signal = 0;	     /* Whether to stop monitoring loop before main process
														terminates. (e.g.,
							if receiving two terminating signals	   in less than a second. */

double max_peak_cores_interval = 180; /* wall time in seconds for the peak cores window. Processes that run for
										 less than this time will report
					 cores_avg as peak cores. */

char *catalog_task_readable_name = NULL;
char *catalog_uuid = NULL;
char *catalog_hosts = NULL;
char *catalog_project = NULL;
char *catalog_owner = NULL;

uint64_t catalog_interval = 0;
uint64_t catalog_last_update_time = 0;

int64_t catalog_interval_default = 30;

timestamp_t last_summary_write = 0;
int update_summary_file = 0;

/***
 * Utility functions (open log files, proc files, measure time)
 ***/

char *default_summary_name(char *template_path)
{
	if (template_path)
		return string_format("%s.summary", template_path);
	else
		return string_format(DEFAULT_LOG_NAME ".summary", getpid());
}

char *default_series_name(char *template_path)
{
	if (template_path)
		return string_format("%s.series", template_path);
	else
		return string_format(DEFAULT_LOG_NAME ".series", getpid());
}

char *default_opened_name(char *template_path)
{
	if (template_path)
		return string_format("%s.files", template_path);
	else
		return string_format(DEFAULT_LOG_NAME ".files", getpid());
}

FILE *open_log_file(const char *log_path)
{
	FILE *log_file;
	char *dirname;

	if (log_path) {
		dirname = xxstrdup(log_path);
		path_dirname(log_path, dirname);
		if (!create_dir(dirname, 0755)) {
			debug(D_FATAL, "could not create directory %s : %s\n", dirname, strerror(errno));
			exit(RM_MONITOR_ERROR);
		}

		if ((log_file = fopen(log_path, "w")) == NULL) {
			debug(D_FATAL, "could not open log file %s : %s\n", log_path, strerror(errno));
			exit(RM_MONITOR_ERROR);
		}

		free(dirname);
	} else
		return NULL;

	return log_file;
}

void activate_debug_log_if_file()
{
	static timestamp_t last_time = 0;

	timestamp_t current = timestamp_get();

	if ((current - last_time) < 30 * USECOND) {
		return;
	}

	struct stat s;
	int status = stat(ACTIVATE_DEBUG_FILE, &s);

	if (status == 0) {
		if (!debug_active) {
			debug_active = 1;
			debug_flags_set("all");
			debug_config_file(ACTIVATE_DEBUG_FILE ".log");
			debug_config_file_size(0);
		}
	} else if (debug_active) {
		debug_active = 0;
		debug_flags_set("clear");
	}

	last_time = current;
}

void parse_limit_string(struct rmsummary *limits, char *str)
{
	char *pair = xxstrdup(str);
	char *delim = strchr(pair, ':');

	if (!delim) {
		debug(D_FATAL, "Missing ':' in '%s'\n", str);
		exit(RM_MONITOR_ERROR);
	}

	*delim = '\0';

	char *field = string_trim_spaces(pair);
	char *value = string_trim_spaces(delim + 1);

	int status = 0;

	double d;
	status = string_is_float(value, &d);

	if (!status) {
		debug(D_FATAL, "Invalid limit field '%s' or value '%s'\n", field, value);
		exit(RM_MONITOR_ERROR);
	}

	rmsummary_set(limits, field, d);
	free(pair);
}

void parse_limits_file(struct rmsummary *limits, char *path)
{
	struct rmsummary *s;
	s = rmsummary_parse_file_single(path);

	rmsummary_merge_override(limits, s);

	rmsummary_delete(s);
}

void add_verbatim_field(const char *str)
{
	char *pair = xxstrdup(str);
	char *delim = strchr(pair, ':');

	if (!delim) {
		debug(D_FATAL, "Missing ':' in '%s'\n", str);
		exit(RM_MONITOR_ERROR);
	}

	*delim = '\0';

	char *field = string_trim_spaces(pair);
	char *value = string_trim_spaces(delim + 1);

	if (!verbatim_summary_fields)
		verbatim_summary_fields = jx_object(NULL);

	jx_insert_string(verbatim_summary_fields, field, value);
	debug(D_RMON, "%s", str);

	free(pair);
}

void find_hostname()
{
	char *host_info = NULL;
	if (domain_name_cache_guess(hostname)) {
		host_info = string_format("host:%s", hostname);
		add_verbatim_field(host_info);
		free(host_info);
	}
}

void find_version()
{
	char *monitor_self_info = string_format("monitor_version:%9s %d.%d.%d.%.8s", "", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, CCTOOLS_COMMIT);
	add_verbatim_field(monitor_self_info);
	free(monitor_self_info);
}

void add_snapshot(struct rmsummary *s)
{
	summary->snapshots_count++;

	if (summary->snapshots_count > snapshots_allocated) {
		while (summary->snapshots_count > snapshots_allocated) {
			snapshots_allocated = MAX(4, snapshots_allocated * 2);
		}

		summary->snapshots = realloc(summary->snapshots, snapshots_allocated * sizeof(struct rmsummary *));
	}

	summary->snapshots[summary->snapshots_count - 1] = s;
}

int rmonitor_determine_exec_type(const char *executable)
{
	char *absolute_exec = path_which(executable);
	char exec_type[PATH_MAX];

	if (!absolute_exec)
		return 1;

	int fd = open(absolute_exec, O_RDONLY, 0);
	if (fd < 0) {
		debug(D_RMON, "Could not open '%s' for reading.", absolute_exec);
		free(absolute_exec);
		return 1;
	}

	bzero(exec_type, PATH_MAX);
	size_t n = read(fd, exec_type, sizeof(exec_type) - 1);

	if (n < 1 || lseek(fd, 0, SEEK_SET) < 0) {
		debug(D_RMON, "Could not read header of '%s'.", absolute_exec);
		strcpy(exec_type, "unknown");
	} else if (strncmp(exec_type, "#!", 2) == 0) {
		char *newline = strchr(exec_type, '\n');
		if (newline)
			*newline = '\0';
	} else {
		errno = 0;
		int rc = elf_get_interp(fd, exec_type);

		if (rc < 0) {
			if (errno == EINVAL) {
				strcpy(exec_type, "static");
			} else {
				strcpy(exec_type, "unknown");
			}
		} else {
			strcpy(exec_type, "dynamic");
		}
	}

	close(fd);

	if (strcmp(exec_type, "dynamic") != 0) {
		debug(D_NOTICE, "Executable is not dynamically linked. Some resources may be undercounted, and children processes may not be tracked.");
	}

	char *type_field = string_format("executable_type: %s", exec_type);
	add_verbatim_field(type_field);

	free(type_field);
	free(absolute_exec);

	return 0;
}

int send_catalog_update(struct rmsummary *s, int force)
{

	if (!catalog_task_readable_name) {
		return 1;
	}

	if (!force && (timestamp_get() < catalog_last_update_time + catalog_interval * USECOND)) {
		return 1;
	}

	struct jx *j = rmsummary_to_json(s, /* all, not only resources */ 0);

	jx_insert_string(j, "type", "task");
	jx_insert_string(j, "uuid", catalog_uuid);
	jx_insert_string(j, "owner", catalog_owner);
	jx_insert_string(j, "task", catalog_task_readable_name);
	jx_insert_string(j, "project", catalog_project);

	char *str = jx_print_string(j);

	debug(D_RMON, "Sending resources snapshot to catalog server(s) at %s ...", catalog_hosts);
	int status = catalog_query_send_update(catalog_hosts, str, CATALOG_UPDATE_BACKGROUND | CATALOG_UPDATE_CONDITIONAL);

	free(str);
	jx_delete(j);

	catalog_last_update_time = timestamp_get();

	return status;
}

/***
 * Reference count for filesystems and working directories auxiliary functions.
 ***/

int itable_addto_count(struct itable *table, void *key, int value)
{
	uintptr_t count = (uintptr_t)itable_lookup(table, (uintptr_t)key);
	count += value; // we get 0 if lookup fails, so that's ok.

	if (count > 0)
		itable_insert(table, (uintptr_t)key, (void *)count);
	else
		itable_remove(table, (uintptr_t)key);

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

	if (count < 1) {
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

	if (count < 1) {
		debug(D_RMON, "working directory '%s' is not monitored anymore.\n", d->path);

		path_disk_size_info_delete_state(d->state);
		hash_table_remove(wdirs, d->path);

		dec_fs_count((void *)d->fs);
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

	if (stat(path, &dinfo) != 0) {
		debug(D_RMON, "stat call on '%s' failed : %s\n", path, strerror(errno));
		return -1;
	}

	return dinfo.st_dev;
}

struct rmonitor_filesys_info *lookup_or_create_fs(char *path)
{
	uint64_t dev_id = get_device_id(path);
	struct rmonitor_filesys_info *inventory = itable_lookup(filesysms, dev_id);

	if (!inventory) {
		debug(D_RMON, "filesystem %" PRId64 " added to monitor.\n", dev_id);

		inventory = (struct rmonitor_filesys_info *)malloc(sizeof(struct rmonitor_filesys_info));
		inventory->path = xxstrdup(path);
		inventory->id = dev_id;
		itable_insert(filesysms, dev_id, (void *)inventory);
		rmonitor_get_dsk_usage(inventory->path, &inventory->disk_initial);
	}

	inc_fs_count(inventory);

	return inventory;
}

struct rmonitor_wdir_info *lookup_or_create_wd(struct rmonitor_wdir_info *previous, char const *path)
{
	struct rmonitor_wdir_info *inventory;

	if (strlen(path) < 1 || access(path, F_OK) != 0)
		return previous;

	inventory = hash_table_lookup(wdirs, path);

	if (!inventory) {
		debug(D_RMON, "working directory '%s' added to monitor.\n", path);

		inventory = (struct rmonitor_wdir_info *)malloc(sizeof(struct rmonitor_wdir_info));
		inventory->path = xxstrdup(path);
		inventory->state = NULL;
		hash_table_insert(wdirs, inventory->path, (void *)inventory);

		inventory->fs = lookup_or_create_fs(inventory->path);
	}

	if (inventory != previous) {
		inc_wd_count(inventory);
		if (previous)
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
	if (finfo) {
		(finfo->n_references)++;
		(finfo->n_opens)++;
		return;
	}

	finfo = calloc(1, sizeof(struct rmonitor_file_info));
	if (finfo != NULL) {
		finfo->n_opens = 1;
		finfo->size_on_open = -1;
		finfo->size_on_close = -1;
		finfo->is_output = is_output;

		if (stat(filename, &fst) >= 0) {
			finfo->size_on_open = fst.st_size;
			finfo->device = fst.st_dev;
		}
	}

	hash_table_insert(files, filename, finfo);

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
	if (rmonitor_inotify_fd >= 0) {
		char **new_inotify_watches;
		int iwd;

		int inotify_flags = IN_CLOSE | IN_ACCESS | IN_MODIFY;
		if (override_flags) {
			inotify_flags = override_flags;
		}

		if ((iwd = inotify_add_watch(rmonitor_inotify_fd, filename, inotify_flags)) < 0) {
			debug(D_RMON, "inotify_add_watch for file %s fails: %s", filename, strerror(errno));
		} else {
			debug(D_RMON, "added watch (id: %d) for file %s", iwd, filename);
			if (iwd >= alloced_inotify_watches) {
				new_inotify_watches = (char **)realloc(inotify_watches, (iwd + 50) * (sizeof(char *)));
				if (new_inotify_watches != NULL) {
					alloced_inotify_watches = iwd + 50;
					inotify_watches = new_inotify_watches;
				} else {
					debug(D_RMON, "Out of memory trying to expand inotify_watches");
				}
			}
			if (iwd < alloced_inotify_watches) {
				inotify_watches[iwd] = xxstrdup(filename);
				if (finfo != NULL)
					finfo->n_references = 1;
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
	struct inotify_event *evdata = NULL;
	struct rmonitor_file_info *finfo = NULL;
	struct stat fst;
	char *fname;
	int nbytes, evc, i;

	if (rmonitor_inotify_fd >= 0) {
		if (ioctl(rmonitor_inotify_fd, FIONREAD, &nbytes) >= 0) {
			evdata = (struct inotify_event *)malloc(nbytes);
			if (evdata == NULL)
				return urgent;
			if (read(rmonitor_inotify_fd, evdata, nbytes) != nbytes) {
				free(evdata);
				return urgent;
			}

			evc = nbytes / sizeof(*evdata);
			for (i = 0; i < evc; i++) {
				if (evdata[i].wd >= alloced_inotify_watches) {
					continue;
				}

				if ((fname = inotify_watches[evdata[i].wd]) == NULL) {
					continue;
				}

				if (finfo == NULL) {
					continue;
				}
				finfo = hash_table_lookup(files, fname);
				if (evdata[i].mask & IN_ACCESS)
					(finfo->n_reads)++;
				if (evdata[i].mask & IN_MODIFY)
					(finfo->n_writes)++;
				if (evdata[i].mask & IN_CLOSE) {
					(finfo->n_closes)++;
					if (stat(fname, &fst) >= 0) {
						finfo->size_on_close = fst.st_size;
					}

					/* Decrease reference count and remove watch of zero */
					(finfo->n_references)--;
					if (finfo->n_references == 0) {
						inotify_rm_watch(rmonitor_inotify_fd, evdata[i].wd);
						debug(D_RMON, "removed watch (id: %d) for file %s", evdata[i].wd, fname);
						free(fname);
						inotify_watches[evdata[i].wd] = NULL;
					}
				}
			}
		}
		free(evdata);
	}
#endif

	return urgent;
}

void append_network_bw(struct rmonitor_msg *msg)
{

	/* Avoid division by zero, negative bws */
	if (msg->end <= msg->start || msg->data.n < 1)
		return;

	struct rmonitor_bw_info *new_tail = malloc(sizeof(struct rmonitor_bw_info));

	new_tail->bit_count = 8 * msg->data.n;
	new_tail->start = msg->start; // start and end of messages in usecs
	new_tail->end = msg->end;

	/* we drop entries older than 60s, unless there are less than 4, so
	 * we can smooth some noise. */
	if (list_size(tx_rx_sizes) > 3) {
		struct rmonitor_bw_info *head;
		while ((head = list_peek_head(tx_rx_sizes))) {
			if (head->end + 60 * ONE_SECOND < new_tail->start) {
				list_pop_head(tx_rx_sizes);
				free(head);
			} else {
				break;
			}
		}
	}

	list_push_tail(tx_rx_sizes, new_tail);
}

int64_t average_bandwidth(int use_min_len)
{
	if (list_size(tx_rx_sizes) == 0)
		return 0;

	int64_t sum = 0;
	struct rmonitor_bw_info *head, *tail;

	/* if last bit count occured more than a minute ago, report bw as 0 */
	tail = list_peek_tail(tx_rx_sizes);
	if (tail->end + 60 * ONE_SECOND < timestamp_get())
		return 0;

	list_first_item(tx_rx_sizes);
	while ((head = list_next_item(tx_rx_sizes))) {
		sum += head->bit_count;
	}

	head = list_peek_head(tx_rx_sizes);
	double len_real = (tail->end - head->start) / ONE_SECOND;

	/* divide at least by 10s, to smooth noise. */
	double n = use_min_len ? MAX(10, len_real) : len_real;

	n *= 1e6; // to Mbps

	return sum / n;
}

/***
 * Logging functions. The process tree is summarized in struct
 * rmsummary's, computing current value, maximum, and minimums.
 ***/

void rmonitor_summary_header()
{
	if (log_series) {
		fprintf(log_series, "# Units:\n");
		fprintf(log_series, "# wall_clock and cpu_time in seconds\n");
		fprintf(log_series, "# virtual, resident and swap memory in megabytes.\n");
		fprintf(log_series, "# disk in megabytes.\n");
		fprintf(log_series, "# bandwidth in Mbps.\n");
		fprintf(log_series, "# cpu_time, bytes_read, bytes_written, bytes_sent, and bytes_received show cummulative values.\n");
		fprintf(log_series, "# wall_clock, max_concurrent_processes, virtual, resident, swap, files, and disk show values at the sample point.\n");

		fprintf(log_series, "#");
		fprintf(log_series, "%s", "wall_clock");
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
		fprintf(log_series, " %s", "machine_load");

		if (resources_flags->disk) {
			fprintf(log_series, " %25s", "total_files");
			fprintf(log_series, " %25s", "disk");
		}

		fprintf(log_series, "\n");
	}
}

struct peak_cores_sample {
	double wall_time;
	double cpu_time;
};

double peak_cores(double wall_time, double cpu_time)
{
	static struct list *samples = NULL;

	if (!samples) {
		samples = list_create();

		struct peak_cores_sample *zero = malloc(sizeof(struct peak_cores_sample));
		zero->wall_time = 0;
		zero->cpu_time = 0;
		list_push_tail(samples, zero);
	}

	struct peak_cores_sample *tail = malloc(sizeof(struct peak_cores_sample));
	tail->wall_time = wall_time;
	tail->cpu_time = cpu_time;
	list_push_tail(samples, tail);

	struct peak_cores_sample *head;

	/* Drop entries older than max_peak_cores_interval, unless we only have two samples. */
	while ((head = list_peek_head(samples))) {
		if (list_size(samples) < 2) {
			break;
		} else if (head->wall_time + max_peak_cores_interval < tail->wall_time) {
			list_pop_head(samples);
			free(head);
		} else {
			break;
		}
	}

	head = list_peek_head(samples);

	double diff_wall = MAX(0, tail->wall_time - head->wall_time);
	double diff_cpu = MAX(0, tail->cpu_time - head->cpu_time);

	if (tail->wall_time - summary->start < max_peak_cores_interval) {
		/* hack to elimiate noise. if we have not collected enough samples,
		 * use max_peak_cores_interval as the wall_time. This eliminates short noisy
		 * burst at the beginning of the execution, but also triggers limits
		 * checks for extreme offenders. */
		diff_wall = max_peak_cores_interval;
	}

	return ((double)diff_cpu) / diff_wall;
}

void rmonitor_collate_tree(struct rmsummary *tr, struct rmonitor_process_info *p, struct rmonitor_mem_info *m, struct rmonitor_wdir_info *d, struct rmonitor_filesys_info *f)
{
	tr->start = summary->start;
	tr->end = ((double)usecs_since_epoch()) / ONE_SECOND;

	tr->wall_time = tr->end - tr->start;

	/* using .delta here because if we use .accumulated, then we lose information of processes that already
	 * terminated. */
	tr->cpu_time += ((double)p->cpu.delta) / ONE_SECOND;
	tr->context_switches += p->ctx.delta;

	tr->cores = 0;
	tr->cores_avg = 0;

	if (tr->wall_time > 0) {
		tr->cores = peak_cores(tr->wall_time, tr->cpu_time);
		tr->cores_avg = tr->cpu_time / tr->wall_time;
	}

	tr->max_concurrent_processes = (double)itable_size(processes);
	tr->total_processes = (double)summary->total_processes;

	/* we use max here, as /proc/pid/smaps that fills *m is not always
	 * available. This causes /proc/pid/status to become a conservative
	 * fallback. */
	if (m->resident > 0) {
		tr->virtual_memory = (double)m->virtual;
		tr->memory = (double)m->resident;
		tr->swap_memory = (double)m->swap;
	} else {
		tr->virtual_memory = (double)p->mem.virtual;
		tr->memory = (double)p->mem.resident;
		tr->swap_memory = (double)p->mem.swap;
	}

	tr->bytes_read = ((double)(p->io.delta_chars_read + tr->bytes_read + p->io.delta_bytes_faulted)) / ONE_MEGABYTE;
	tr->bytes_written = ((double)(p->io.delta_chars_written + tr->bytes_written)) / ONE_MEGABYTE;

	tr->bytes_received = ((double)total_bytes_rx) / ONE_MEGABYTE;
	tr->bytes_sent = ((double)total_bytes_tx) / ONE_MEGABYTE;

	tr->bandwidth = average_bandwidth(1);

	tr->total_files = (int64_t)d->files;
	tr->disk = (int64_t)(d->byte_count + ONE_MEGABYTE - 1) / ONE_MEGABYTE;

	tr->fs_nodes = (int64_t)f->disk.f_ffree;

	tr->machine_load = p->load.last_minute;
	tr->machine_cpus = p->load.cpus;

	// hack: set gpu limit as the measured gpus:
	if (resources_limits->gpus > 0) {
		tr->gpus = resources_limits->gpus;
	}
}

void rmonitor_find_max_tree(struct rmsummary *result, struct rmsummary *tr)
{
	if (!tr)
		return;

	rmsummary_merge_max_w_time(result, tr);
	if (result->wall_time > 0) {
		result->cores_avg = result->cpu_time / result->wall_time;
	}

	/* if we are running with the --sh option, we subtract one process (the sh process). */
	if (sh_cmd_line) {
		result->max_concurrent_processes--;
	}
}

void rmonitor_log_row(struct rmsummary *tr)
{
	if (log_series) {
		fprintf(log_series, "%s", rmsummary_resource_to_str("start", tr->wall_time + summary->start, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("cpu_time", tr->cpu_time, 0));

		if (tr->wall_time > max_peak_cores_interval) {
			fprintf(log_series, " %s", rmsummary_resource_to_str("cores", tr->cores, 0));
		} else {
			fprintf(log_series, " %s", rmsummary_resource_to_str("cores", tr->cores_avg, 0));
		}

		fprintf(log_series, " %s", rmsummary_resource_to_str("max_concurrent_processes", tr->max_concurrent_processes, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("virtual_memory", tr->virtual_memory, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("memory", tr->memory, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("swap_memory", tr->swap_memory, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("bytes_read", tr->bytes_read, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("bytes_written", tr->bytes_written, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("bytes_received", tr->bytes_received, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("bytes_sent", tr->bytes_sent, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("bandwidth", tr->bandwidth, 0));
		fprintf(log_series, " %s", rmsummary_resource_to_str("machine_load", tr->machine_load, 0));

		if (resources_flags->disk) {
			fprintf(log_series, " %s", rmsummary_resource_to_str("total_files", tr->total_files, 0));
			fprintf(log_series, " %s", rmsummary_resource_to_str("disk", tr->disk, 0));
		}

		fprintf(log_series, "\n");

		fflush(log_series);
		fsync(fileno(log_series));

		/* are we going to keep monitoring the whole filesystem? */
		// fprintf(log_series "%" PRId64 "\n", tr->fs_nodes);
	}
}

int record_snapshot(struct rmsummary *tr)
{
	if (list_size(snapshot_labels) < 1) {
		return 0;
	}

	buffer_t b;
	buffer_init(&b);

	const char *sep = "";

	char *str;
	while ((str = list_pop_head(snapshot_labels))) {
		string_chomp(str);
		buffer_printf(&b, "%s%s", sep, str);
		sep = ",";
		free(str);
	}

	struct rmsummary *freeze = rmsummary_copy(snapshot, 0);

	freeze->end = ((double)usecs_since_epoch() / ONE_SECOND);
	freeze->wall_time = snapshot->end - snapshot->start;
	freeze->snapshot_name = xxstrdup(buffer_tostring(&b));

	char *output_file = string_format("%s.snapshot.%02ld", template_path, summary->snapshots_count);
	FILE *snap_f = fopen(output_file, "w");

	if (!snap_f) {
		warn(D_RMON, "Could not save snapshots file '%s': %s", output_file, strerror(errno));
	} else {
		rmsummary_print(snap_f, freeze, 1, NULL);
		fclose(snap_f);
	}

	add_snapshot(freeze);

	free(output_file);

	debug(D_RMON, "Recoded snapshot: '%s'", buffer_tostring(&b));

	buffer_free(&b);

	return 1;
}

void decode_zombie_status(struct rmsummary *summary, int wait_status)
{
	/* update from any END_WAIT message received. */
	summary->exit_status = first_process_exit_status;

	if (WIFSIGNALED(wait_status) || WIFSTOPPED(wait_status)) {
		debug(D_RMON, "process %d terminated: %s.\n", first_process_pid, strsignal(WIFSIGNALED(wait_status) ? WTERMSIG(wait_status) : WSTOPSIG(wait_status)));

		summary->exit_type = xxstrdup("signal");

		if (WIFSIGNALED(wait_status))
			summary->signal = WTERMSIG(wait_status);
		else
			summary->signal = WSTOPSIG(wait_status);

		summary->exit_status = 128 + summary->signal;
	} else {
		debug(D_RMON, "process %d finished: %d.\n", first_process_pid, WEXITSTATUS(wait_status));
		summary->exit_type = xxstrdup("normal");
		summary->exit_status = WEXITSTATUS(wait_status);
	}

	if (summary->limits_exceeded) {
		/* record that limits were exceeded in the summary, but only change the
		 * exit_status when enforcing limits. */
		free(summary->exit_type);
		summary->exit_type = xxstrdup("limits");

		if (enforce_limits) {
			summary->exit_status = 128 + SIGTERM;
		}
	}
}

void rmonitor_find_files_final_sizes()
{
	char *fname;
	struct stat buf;
	struct rmonitor_file_info *finfo;

	hash_table_firstkey(files);
	while (hash_table_nextkey(files, &fname, (void **)&finfo)) {
		/* If size_on_close is unknwon, perform a stat on the file. */

		if (finfo->size_on_close < 0 && stat(fname, &buf) == 0) {
			finfo->size_on_close = buf.st_size;
		}
	}
}

void rmonitor_add_files_to_summary(char *field, int outputs)
{
	char *fname;
	struct rmonitor_file_info *finfo;

	buffer_t b;

	buffer_init(&b);
	buffer_putfstring(&b, "%-15s[\n", field);

	char *delimeter = "";

	hash_table_firstkey(files);
	while (hash_table_nextkey(files, &fname, (void **)&finfo)) {
		if (finfo->is_output != outputs)
			continue;

		int64_t file_size = MAX(finfo->size_on_open, finfo->size_on_close);

		if (file_size < 0) {
			debug(D_NOTICE, "Could not find size of file %s\n", fname);
			continue;
		}

		buffer_putfstring(&b, "%s%20s\"%s\", %" PRId64 " ]", delimeter, "[ ", fname, (int64_t)ceil(1.0 * file_size / ONE_MEGABYTE));
		delimeter = ",\n";
	}

	buffer_putfstring(&b, "\n%16s", "]");

	add_verbatim_field(buffer_tostring(&b));

	buffer_free(&b);
}

int rmonitor_file_io_summaries()
{
#if defined(RESOURCE_MONITOR_USE_INOTIFY)
	if (rmonitor_inotify_fd >= 0) {
		char *fname;
		struct rmonitor_file_info *finfo;

		fprintf(log_inotify,
				"%-15s\n%-15s %6s %20s %20s %6s %6s %6s %6s\n",
				"#path",
				"#",
				"device",
				"size_initial(B)",
				"size_final(B)",
				"opens",
				"closes",
				"reads",
				"writes");

		hash_table_firstkey(files);
		while (hash_table_nextkey(files, &fname, (void **)&finfo)) {
			fprintf(log_inotify, "%-15s\n%-15s ", fname, "");
			fprintf(log_inotify, "%6" PRId64 " %20lld %20lld", finfo->device, (long long int)finfo->size_on_open, (long long int)finfo->size_on_close);
			fprintf(log_inotify, " %6" PRId64 " %6" PRId64, finfo->n_opens, finfo->n_closes);
			fprintf(log_inotify, " %6" PRId64 " %6" PRId64 "\n", finfo->n_reads, finfo->n_writes);
		}
	}
#endif
	return 0;
}

void write_summary(int exited)
{
	if (!exited && last_summary_write + interval * ONE_SECOND > timestamp_get()) {
		return;
	}

	if (!exited) {
		summary->exit_type = xxstrdup("running");
	}

	log_summary = open_log_file(summary_path);
	rmsummary_print(log_summary, summary, pprint_summaries, verbatim_summary_fields);
	fclose(log_summary);

	if (!exited) {
		free(summary->exit_type);
		summary->exit_type = NULL;
	}

	last_summary_write = timestamp_get();
}

int rmonitor_final_summary()
{
	decode_zombie_status(summary, first_process_sigchild_status);

	if (summary->wall_time > 0) {
		summary->cores_avg = summary->cpu_time / summary->wall_time;
	}

	if (log_inotify) {
		rmonitor_find_files_final_sizes();
		rmonitor_add_files_to_summary("input_files:", 0);
		rmonitor_add_files_to_summary("output_files:", 1);

		int nfds = rmonitor_inotify_fd + 1;
		int count = 0;

		struct timeval timeout;
		fd_set rset;
		do {
			timeout.tv_sec = 0;
			timeout.tv_usec = 0;

			FD_ZERO(&rset);
			if (rmonitor_inotify_fd > 0)
				FD_SET(rmonitor_inotify_fd, &rset);

			count = select(nfds, &rset, NULL, NULL, &timeout);

			if (count > 0)
				if (FD_ISSET(rmonitor_inotify_fd, &rset)) {
					rmonitor_handle_inotify();
				}
		} while (count > 0);

		rmonitor_file_io_summaries();
	}

	write_summary(1);

	int status;
	if (summary->limits_exceeded && enforce_limits) {
		status = RM_OVERFLOW;
	} else if (summary->exit_status != 0) {
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

// if 1 pid was added anew to the tracking table, 0 otherwise (was already there, or could not be added).
int rmonitor_track_process(pid_t pid)
{
	char *newpath;
	struct rmonitor_process_info *p;

	if (!pid) {
		return 0;
	}

	if (!ping_process(pid)) {
		return 0;
	}

	p = itable_lookup(processes, pid);

	if (p) {
		return 0;
	}

	p = malloc(sizeof(struct rmonitor_process_info));
	bzero(p, sizeof(struct rmonitor_process_info));

	p->pid = pid;
	p->running = 0;

	if (follow_chdir) {
		newpath = getcwd(NULL, 0);
		p->wd = lookup_or_create_wd(NULL, newpath);
		free(newpath);
	}

	itable_insert(processes, p->pid, (void *)p);

	p->running = 1;
	p->waiting = 0;

	rmonitor_poll_process_once(p);
	summary->total_processes++;

	return 1;
}

void rmonitor_untrack_process(uint64_t pid)
{
	struct rmonitor_process_info *p = itable_lookup(processes, pid);

	if (p)
		p->running = 0;
}

void rmonitor_add_children_by_polling()
{

	uint64_t pid;
	struct rmonitor_process_info *p;
	uint64_t *children = NULL;

	itable_firstkey(processes);
	while (itable_nextkey(processes, &pid, (void **)&p)) {
		if (!p->running) {
			continue;
		}

		int n = rmonitor_get_children(pid, &children);

		if (n < 1) {
			continue;
		}

		int i;
		for (i = 0; i < n; i++) {
			if (rmonitor_track_process(children[i])) {
				debug(D_RMON, "added by polling pid %" PRIu64, children[i]);
			}
		}

		free(children);
	}
}

void cleanup_zombie(struct rmonitor_process_info *p)
{
	debug(D_RMON, "cleaning process: %d\n", p->pid);

	if (follow_chdir && p->wd)
		dec_wd_count(p->wd);

	itable_remove(processes, p->pid);
	free(p);
}

void cleanup_zombies(void)
{
	uint64_t pid;
	struct rmonitor_process_info *p;

	itable_firstkey(processes);
	while (itable_nextkey(processes, &pid, (void **)&p))
		if (!p->running)
			cleanup_zombie(p);
}

void release_waiting_process(uint64_t pid)
{
	debug(D_RMON, "sending SIGCONT to %" PRIu64 ".", pid);
	kill((pid_t)pid, SIGCONT);
}

void release_waiting_processes(void)
{
	uint64_t pid;
	struct rmonitor_process_info *p;

	itable_firstkey(processes);
	while (itable_nextkey(processes, &pid, (void **)&p))
		if (p->waiting)
			release_waiting_process(pid);
}

void ping_processes(void)
{
	uint64_t pid;
	struct rmonitor_process_info *p;

	itable_firstkey(processes);
	while (itable_nextkey(processes, &pid, (void **)&p))
		if (!ping_process(pid)) {
			debug(D_RMON, "cannot find %" PRId64 " process.\n", pid);
			rmonitor_untrack_process(pid);
		}
}

void set_snapshot_watch_events()
{

	if (!snapshot_watch_events_file) {
		return;
	}

	struct jx *j = jx_parse_file(snapshot_watch_events_file);

	if (!j) {
		debug(D_FATAL, "Could not process '%s' snapshots file.", snapshot_watch_events_file);
		exit(RM_MONITOR_ERROR);
	}

	void *i = NULL;
	const char *fname;
	while ((fname = jx_iterate_keys(j, &i))) {
		struct jx *array = jx_lookup(j, fname);

		if (!jx_istype(array, JX_OBJECT)) {
			debug(D_FATAL, "Error processing snapshot configurations for %s. Not of the form {\"FILENAME\" : { \"events\" : [ { \"label\": ..., }, ... ]", fname);
			exit(RM_MONITOR_ERROR);
		}

		pid_t pid = rmonitor_watch_file(fname, array);

		itable_insert(snapshot_watch_pids, (uintptr_t)pid, (void *)snapshot_watch_pids);
	}

	jx_delete(j);
}

void terminate_snapshot_watch_events()
{
	uint64_t pid;
	void *dummy;

	itable_firstkey(snapshot_watch_pids);
	while (itable_nextkey(snapshot_watch_pids, &pid, &dummy)) {
		kill(pid, SIGKILL);
	}
}

struct rmsummary *rmonitor_final_usage_tree(void)
{
	struct rusage usg;
	struct rmsummary *tr_usg = rmsummary_create(-1);

	debug(D_RMON, "calling getrusage.\n");

	if (getrusage(RUSAGE_CHILDREN, &usg) != 0) {
		debug(D_RMON, "getrusage failed: %s\n", strerror(errno));
		return NULL;
	}

	if (usg.ru_majflt > 0) {
		/* Here we add the maximum recorded + the io from memory maps */
		tr_usg->bytes_read = summary->bytes_read + (((double)usg.ru_majflt * sysconf(_SC_PAGESIZE)) / ONE_MEGABYTE);
		debug(D_RMON, "page faults: %ld.\n", usg.ru_majflt);
	}

	tr_usg->cpu_time = 0;
	tr_usg->cpu_time += usg.ru_utime.tv_sec + (((double)usg.ru_utime.tv_usec) / ONE_SECOND);
	tr_usg->cpu_time += usg.ru_stime.tv_sec + (((double)usg.ru_stime.tv_usec) / ONE_SECOND);
	tr_usg->start = summary->start;
	tr_usg->end = ((double)usecs_since_epoch() / ONE_SECOND);
	tr_usg->wall_time = tr_usg->end - tr_usg->start;

	/* we do not use peak_cores here, as we may have missed some threads which
	 * make cpu_time quite jumpy. */
	if (tr_usg->wall_time > 0) {
		tr_usg->cores = tr_usg->cpu_time / tr_usg->wall_time;
		tr_usg->cores_avg = tr_usg->cores;
	}

	tr_usg->bandwidth = average_bandwidth(0);
	tr_usg->bytes_received = ((double)total_bytes_rx) / ONE_MEGABYTE;
	tr_usg->bytes_sent = ((double)total_bytes_tx) / ONE_MEGABYTE;

	return tr_usg;
}

/* signal handler forward to process */
void rmonitor_forward_signal(const int signal, siginfo_t *info, void *data)
{
	timestamp_t current_time = timestamp_get();
	switch (signal) {
	case SIGINT:
	case SIGQUIT:
	case SIGTERM:
		if (current_time - last_termination_signal_time < USECOND) {
			fast_terminate_from_signal = 1;
		}
		last_termination_signal_time = current_time;
		if (first_pid_manually_set) {
			/* do not forward termination signal if monitor attached to
			 * already running process. */
			break;
		}
		/* fall through */
	default:
		notice(D_RMON, "forwarding signal %s(%d)", strsignal(signal), signal);
		kill(first_process_pid, signal);
		break;
	}
}

/* sigchild signal handler */
void rmonitor_check_child(const int signal)
{
	uint64_t pid = waitpid(first_process_pid, &first_process_sigchild_status, WNOHANG | WCONTINUED | WUNTRACED);

	if (pid != (uint64_t)first_process_pid)
		return;

	debug(D_RMON, "got SIGCHLD from %d", first_process_pid);

	if (WIFEXITED(first_process_sigchild_status)) {
		debug(D_RMON, "exit\n");
	} else if (WIFSIGNALED(first_process_sigchild_status)) {
		debug(D_RMON, "signal\n");
	} else if (WIFSTOPPED(first_process_sigchild_status)) {
		debug(D_RMON, "stop\n");

		switch (WSTOPSIG(first_process_sigchild_status)) {
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
	} else if (WIFCONTINUED(first_process_sigchild_status)) {
		debug(D_RMON, "continue\n");
		return;
	}

	first_process_already_waited = 1;

	struct rmonitor_process_info *p;
	debug(D_RMON, "adding all processes to cleanup list.\n");
	itable_firstkey(processes);
	while (itable_nextkey(processes, &pid, (void **)&p))
		rmonitor_untrack_process(pid);

	/* get the peak values from getrusage, and others. */
	struct rmsummary *tr_usg = rmonitor_final_usage_tree();
	rmonitor_find_max_tree(summary, tr_usg);
	free(tr_usg);
}

void cleanup_library()
{
	unlink(lib_helper_name);
}

void rmonitor_final_cleanup()
{
	uint64_t pid;
	struct rmonitor_process_info *p;
	int status;

	sigset_t block;
	sigfillset(&block);
	sigprocmask(SIG_BLOCK, &block, NULL);

	if (!first_pid_manually_set) {
		itable_firstkey(processes);
		while (itable_nextkey(processes, &pid, (void **)&p)) {
			notice(D_RMON, "sending kill signal to process %" PRId64 ".%d\n", pid);
			kill(pid, SIGKILL);
		}

		while (!first_process_already_waited) {
			usleep((int)(0.1 * USECOND)); // 0.2s

			ping_processes();
			cleanup_zombies();

			rmonitor_check_child(0);
		}
	}

	if (lib_helper_extracted) {
		cleanup_library();
		lib_helper_extracted = 0;
	}

	status = rmonitor_final_summary();

	send_catalog_update(summary, 1);

	if (log_series)
		fclose(log_series);
	if (log_inotify)
		fclose(log_inotify);

	terminate_snapshot_watch_events();

	exit(status);
}

/***
 * Functions that communicate with the helper library,
 * (un)tracking resources as messages arrive.
 ***/

void write_helper_lib(void)
{
	uint64_t n;

	lib_helper_name = xxstrdup("librmonitor_helper.so.XXXXXX");

	if (access(lib_helper_name, R_OK | X_OK) == 0) {
		lib_helper_extracted = 0;
		return;
	}

	int flib = mkstemp(lib_helper_name);
	if (flib == -1)
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

	if (recv_status < 0) {
		if (errno != EAGAIN) {
			debug(D_RMON, "Error receiving message: %s", strerror(errno));
			return 1;
		}
	}

	if (((unsigned int)recv_status) < sizeof(msg)) {
		debug(D_RMON, "Malformed message from monitored processes. Ignoring.");
		return 1;
	}

	// Next line commented: Useful for detailed debugging, but too spammy for regular operations.
	// debug(D_RMON,"message '%s' (%d) from %d with status '%s' (%d)\n", str_msgtype(msg.type), msg.type,
	// msg.origin, strerror(msg.error), msg.error);

	p = itable_lookup(processes, (uint64_t)msg.origin);

	if (!p) {
		/* We either got a malformed message, message from a
		process we are not tracking anymore, a message from
		a newly created process, or a message from a snapshot process.  */
		if (msg.type == END_WAIT) {
			release_waiting_process(msg.origin);
			return 1;
		} else if (msg.type != BRANCH && msg.type != SNAPSHOT) {
			return 1;
		}
	}

	switch (msg.type) {
	case BRANCH:
		msg.error = 0;
		rmonitor_track_process(msg.origin);
		if (summary->max_concurrent_processes < itable_size(processes)) {
			summary->max_concurrent_processes = itable_size(processes);
		}
		break;
	case END_WAIT:
		msg.error = 0;
		p->waiting = 1;
		if (msg.origin == first_process_pid) {
			first_process_exit_status = msg.data.n;
		}
		break;
	case END:
		msg.error = 0;
		rmonitor_untrack_process(msg.origin);
		break;
	case CHDIR:
		msg.error = 0;
		if (follow_chdir) {
			p->wd = lookup_or_create_wd(p->wd, msg.data.s);
		}
		break;
	case OPEN_INPUT:
	case OPEN_OUTPUT:
		switch (msg.error) {
		case 0:
			debug(D_RMON, "File %s has been opened.\n", msg.data.s);
			if (log_inotify) {
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
		if (msg.data.n > 0) {
			total_bytes_rx += msg.data.n;
			append_network_bw(&msg);
		}
		break;
	case TX:
		msg.error = 0;
		if (msg.data.n > 0) {
			total_bytes_tx += msg.data.n;
			append_network_bw(&msg);
		}
		break;
	case READ:
		msg.error = 0;
		break;
	case WRITE:
		switch (msg.error) {
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
	case SNAPSHOT:
		debug(D_RMON, "Snapshot msg label: '%s'\n", msg.data.s);
		list_push_tail(snapshot_labels, xxstrdup(msg.data.s));
		break;
	default:
		break;
	};

	summary->last_error = msg.error;

	if (!rmsummary_check_limits(summary, resources_limits) && enforce_limits) {
		rmonitor_final_cleanup();
	}

	// find out if messages are urgent:
	if (msg.type == SNAPSHOT) {
		// SNAPSHOTs are always urgent
		return 1;
	}

	if (msg.type == END_WAIT || msg.type == END) {
		if (msg.origin == first_process_pid) {
			// ENDs from the first process are always urgent.
			return 1;
		}

		if (stop_short_running) {
			// we are stopping all processes, so all ENDs are urgent.
			return 1;
		}

		if (msg.end < (msg.start + RESOURCE_MONITOR_SHORT_TIME)) {
			// for short running processes END_WAIT and END are not urgent.
			return 0;
		}

		// ENDs for long running processes are always urgent.
		return 1;
	}

	// Any other case is not urgent.
	return 0;
}

int wait_for_messages(int interval)
{
	struct timeval timeout;
	timeout.tv_sec = interval;
	timeout.tv_usec = 0;

	debug(D_RMON, "sleeping for: %d seconds\n", interval);

	// If grandchildren processes cannot talk to us, simply wait.
	// Else, wait, and check socket for messages.
	if (rmonitor_queue_fd < 0) {
		/* wait for interval. */
		select(1, NULL, NULL, NULL, &timeout);
	} else {

		/* Figure out the number of file descriptors to pass to select */
		int nfds = 1 + MAX(rmonitor_queue_fd, rmonitor_inotify_fd);
		fd_set rset;

		int urgent = 0;
		int count = 0;
		do {
			FD_ZERO(&rset);
			if (rmonitor_queue_fd > 0) {
				FD_SET(rmonitor_queue_fd, &rset);
			}

			if (rmonitor_inotify_fd > 0) {
				FD_SET(rmonitor_inotify_fd, &rset);
			}

			count = select(nfds, &rset, NULL, NULL, &timeout);

			if (FD_ISSET(rmonitor_queue_fd, &rset)) {
				urgent |= rmonitor_dispatch_msg();
			}

			if (FD_ISSET(rmonitor_inotify_fd, &rset)) {
				urgent |= rmonitor_handle_inotify();
			}

			if (urgent) {
				timeout.tv_sec = 0;
				timeout.tv_usec = 0;
			}
		} while (count > 0);
	}

	return 0;
}

/***
 * Functions to fork the very first process. This process is
 * created and suspended before execv, until a SIGCONT is sent
 * from the monitor.
 ***/

// Very first process signal handler.
void wakeup_after_fork(int signum)
{
	if (signum == SIGCONT)
		signal(SIGCONT, SIG_DFL);
}

pid_t rmonitor_fork(void)
{
	pid_t pid;
	sigset_t set;
	void (*prev_handler)(int signum);

	// make the monitor the leader of its own process group
	setpgid(0, 0);
	pid = fork();

	prev_handler = signal(SIGCONT, wakeup_after_fork);
	sigfillset(&set);
	sigdelset(&set, SIGCONT);

	if (pid > 0) {
		debug(D_RMON, "fork %d -> %d\n", getpid(), pid);

		rmonitor_track_process(pid);

		/* if we are running with the --sh option, we subtract one process (the sh process). */
		if (sh_cmd_line) {
			summary->total_processes--;
		}

		signal(SIGCONT, prev_handler);
		kill(pid, SIGCONT);
	} else {
		// sigsuspend(&set);
		signal(SIGCONT, prev_handler);
	}

	return pid;
}

struct rmonitor_process_info *spawn_first_process(const char *executable, char *argv[], int child_in_foreground)
{
	pid_t pid;

	pid = rmonitor_fork();

	rmonitor_summary_header();

	if (pid > 0) {
		first_process_pid = pid;
		close(STDIN_FILENO);
		close(STDOUT_FILENO);

		if (child_in_foreground) {
			signal(SIGTTOU, SIG_IGN);
			int fdtty, retc;
			fdtty = open("/dev/tty", O_RDWR);
			if (fdtty >= 0) {
				/* Try bringing the child process to the session foreground */
				retc = tcsetpgrp(fdtty, getpgid(pid));
				if (retc < 0) {
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
		if (executable_path) {
			rmonitor_add_file_watch(executable_path, /* is output? */ 0, 0);
			free(executable_path);
		}
	} else if (pid < 0) {
		debug(D_FATAL, "fork failed: %s\n", strerror(errno));
		exit(RM_MONITOR_ERROR);
	} else // child
	{
		debug(D_RMON, "executing: %s\n", executable);

		char *pid_s = string_format("%d", getpid());
		setenv(RESOURCE_MONITOR_ROOT_PROCESS, pid_s, 1);
		free(pid_s);

		prctl(PR_SET_PDEATHSIG, SIGKILL);

		errno = 0;
		execvp(executable, argv);
		// We get here only if execlp fails.
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
	fprintf(stdout, "%-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	fprintf(stdout, "%-30s Show this message.\n", "-h,--help");
	fprintf(stdout, "%-30s Show version string.\n", "-v,--version");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Maximum interval between observations, in seconds. (default=%d)\n", "-i,--interval=<n>", DEFAULT_INTERVAL);
	fprintf(stdout, "%-30s Track <pid> instead of executing a command line (warning: less precise measurements).\n", "--pid=<pid>");
	fprintf(stdout, "%-30s Accurately measure short running processes (adds overhead).\n", "--accurate-short-processes");
	fprintf(stdout, "%-30s Read command line from <str>, and execute as '/bin/sh -c <str>'\n", "-c,--sh=<str>");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Use maxfile with list of var: value pairs for resource limits.\n", "-l,--limits-file=<maxfile>");
	fprintf(stdout, "%-30s Use string of the form \"var: value, var: value\" to specify.\n", "-L,--limits=<string>");
	fprintf(stdout, "%-30s resource limits. Can be specified multiple times.\n", "");
	fprintf(stdout, "%-30s Do not enforce resource limits, only measure resources.\n", "--measure-only");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Keep the monitored process in foreground (for interactive use).\n", "-f,--child-in-foreground");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Specify filename template for log files (default=resource-pid-<pid>)\n", "-O,--with-output-files=<file>");
	fprintf(stdout, "%-30s Write resource time series to <template>.series\n", "--with-time-series");
	fprintf(stdout, "%-30s Write inotify statistics of opened files to default=<template>.files\n", "--with-inotify");
	fprintf(stdout, "%-30s Include this string verbatim in a line in the summary. \n", "-V,--verbatim-to-summary=<str>");
	fprintf(stdout, "%-30s (Could be specified multiple times.)\n", "");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Follow the size of <dir>. By default the directory at the start of\n", "--measure-dir=<dir>");
	fprintf(stdout, "%-30s execution is followed. Can be specified multiple times.\n", "");
	fprintf(stdout, "%-30s See --without-disk-footprint below.\n", "");
	fprintf(stdout, "%-30s Do not measure working directory footprint. Overrides --measure-dir and --follow-chdir.\n", "--without-disk-footprint");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Report measurements to catalog server with \"task\"=<task-name>.\n", "--catalog-task-name=<name>");
	fprintf(stdout, "%-30s Set project name of catalog update to <project> (default=<task-name>).\n", "--catalog-project=<project>");
	fprintf(stdout, "%-30s Use catalog server <catalog>. (default=catalog.cse.nd.edu:9094).\n", "--catalog=<catalog>");
	fprintf(stdout, "%-30s Send update to catalog every <interval> seconds. (default=%" PRId64 ").\n", "--catalog-interval=<interval>", catalog_interval_default);
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Update resource summary file every measurement interval.\n", "--update-summary");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Do not pretty-print summaries.\n", "--no-pprint");
	fprintf(stdout, "\n");
	fprintf(stdout, "%-30s Configuration file for snapshots on file patterns. (See man page.)\n", "--snapshot-events=<file>");
}

int rmonitor_resources(long int interval /*in seconds */)
{
	uint64_t round;

	struct rmonitor_process_info *p_acc = calloc(1, sizeof(struct rmonitor_process_info)); // Automatic zeroed.
	struct rmonitor_wdir_info *d_acc = calloc(1, sizeof(struct rmonitor_wdir_info));
	struct rmonitor_filesys_info *f_acc = calloc(1, sizeof(struct rmonitor_filesys_info));
	struct rmonitor_mem_info *m_acc = calloc(1, sizeof(struct rmonitor_mem_info));

	struct rmsummary *resources_now = calloc(1, sizeof(struct rmsummary));

	// Loop while there are processes to monitor, that is
	// itable_size(processes) > 0). The check is done again in a
	// if/break pair below to mitigate a race condition in which
	// the last process exits after the while(...) is tested, but
	// before we reach select.
	round = 1;
	while (itable_size(processes) > 0 && !fast_terminate_from_signal) {
		debug(D_RMON, "Round %" PRId64, round);
		activate_debug_log_if_file();

		resources_now->last_error = 0;

		ping_processes();

		rmonitor_poll_all_processes_once(processes, p_acc);
		rmonitor_poll_maps_once(processes, m_acc);

		if (resources_flags->disk) {
			rmonitor_poll_all_wds_once(wdirs, d_acc, MAX(1, interval / (MAX(1, hash_table_size(wdirs)))));
		}

		// rmonitor_fss_once(f); disabled until statfs fs id makes sense.

		rmonitor_collate_tree(resources_now, p_acc, m_acc, d_acc, f_acc);
		rmonitor_find_max_tree(summary, resources_now);
		rmonitor_find_max_tree(snapshot, resources_now);
		rmonitor_log_row(resources_now);

		if (!rmsummary_check_limits(summary, resources_limits) && enforce_limits) {
			rmonitor_final_cleanup();
		}

		release_waiting_processes();

		cleanup_zombies();

		if (record_snapshot(snapshot)) {
			rmsummary_delete(snapshot);
			snapshot = rmsummary_create(-1);
			snapshot->start = ((double)usecs_since_epoch()) / ONE_SECOND;
		}

		if (update_summary_file) {
			write_summary(0);
		}

		send_catalog_update(resources_now, 0);

		// If no more process are alive, break out of loop.
		if (itable_size(processes) < 1)
			break;

		wait_for_messages(interval);

		// if monitoring a static executable, this adds children missed by
		// BRANCH messages.
		rmonitor_add_children_by_polling();

		// cleanup processes which by terminating may have awaken
		// select.
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

int main(int argc, char **argv)
{
	int i;
	char *command_line;
	char *executable;
	int64_t c;

	char *series_path = NULL;
	char *opened_path = NULL;

	char *sh_cmd_line = NULL;
	char *argv_sh[] = {"/bin/sh", "-c", sh_cmd_line, 0}; // sh_cmd_line to be replace if given as arg

	int use_series = 0;
	int use_inotify = 0;
	int child_in_foreground = 0;

	debug_config(argv[0]);

	signal(SIGCHLD, rmonitor_check_child);

	struct sigaction sig_act_forward;
	sig_act_forward.sa_flags = 0;
	sig_act_forward.sa_sigaction = rmonitor_forward_signal;
	sigfillset(&sig_act_forward.sa_mask);

	sigaction(SIGINT, &sig_act_forward, NULL);
	sigaction(SIGQUIT, &sig_act_forward, NULL);
	sigaction(SIGTERM, &sig_act_forward, NULL);
	sigaction(SIGABRT, &sig_act_forward, NULL);
	sigaction(SIGALRM, &sig_act_forward, NULL);
	sigaction(SIGHUP, &sig_act_forward, NULL);
	sigaction(SIGUSR1, &sig_act_forward, NULL);
	sigaction(SIGUSR2, &sig_act_forward, NULL);

	summary = calloc(1, sizeof(struct rmsummary));
	snapshot = calloc(1, sizeof(struct rmsummary));

	summary->peak_times = rmsummary_create(-1);
	resources_limits = rmsummary_create(-1);
	resources_flags = rmsummary_create(0);

	total_bytes_rx = 0;
	total_bytes_tx = 0;
	tx_rx_sizes = list_create();

	snapshot_labels = list_create();
	snapshot_watch_pids = itable_create(0);

	processes = itable_create(0);
	wdirs = hash_table_create(0, 0);
	filesysms = itable_create(0);
	files = hash_table_create(0, 0);

	wdirs_rc = itable_create(0);
	filesys_rc = itable_create(0);

	char *cwd = getcwd(NULL, 0);

	enum {
		LONG_OPT_TIME_SERIES = UCHAR_MAX + 1,
		LONG_OPT_OPENED_FILES,
		LONG_OPT_DISK_FOOTPRINT,
		LONG_OPT_NO_DISK_FOOTPRINT,
		LONG_OPT_SH_CMDLINE,
		LONG_OPT_WORKING_DIRECTORY,
		LONG_OPT_FOLLOW_CHDIR,
		LONG_OPT_MEASURE_DIR,
		LONG_OPT_NO_PPRINT,
		LONG_OPT_SNAPSHOT_FILE,
		LONG_OPT_SNAPSHOT_WATCH_CONF,
		LONG_OPT_STOP_SHORT_RUNNING,
		LONG_OPT_CATALOG_TASK_READABLE_NAME,
		LONG_OPT_CATALOG_SERVER,
		LONG_OPT_CATALOG_PROJECT,
		LONG_OPT_CATALOG_INTERVAL,
		LONG_OPT_UPDATE_SUMMARY,
		LONG_OPT_PID,
		LONG_OPT_MEASURE_ONLY
	};

	static const struct option long_options[] = {/* Regular Options */
			{"debug", required_argument, 0, 'd'},
			{"debug-file", required_argument, 0, 'o'},
			{"help", required_argument, 0, 'h'},
			{"version", no_argument, 0, 'v'},
			{"interval", required_argument, 0, 'i'},
			{"limits", required_argument, 0, 'L'},
			{"limits-file", required_argument, 0, 'l'},
			{"sh", required_argument, 0, 'c'},
			{"pid", required_argument, 0, LONG_OPT_PID},
			{"measure-only", no_argument, 0, LONG_OPT_MEASURE_ONLY},

			{"verbatim-to-summary", required_argument, 0, 'V'},

			{"follow-chdir", no_argument, 0, LONG_OPT_FOLLOW_CHDIR},
			{"measure-dir", required_argument, 0, LONG_OPT_MEASURE_DIR},
			{"no-pprint", no_argument, 0, LONG_OPT_NO_PPRINT},

			{"accurate-short-processes", no_argument, 0, LONG_OPT_STOP_SHORT_RUNNING},

			{"with-output-files", required_argument, 0, 'O'},
			{"with-time-series", no_argument, 0, LONG_OPT_TIME_SERIES},
			{"with-inotify", no_argument, 0, LONG_OPT_OPENED_FILES},
			{"without-disk-footprint", no_argument, 0, LONG_OPT_NO_DISK_FOOTPRINT},

			{"snapshot-file", required_argument, 0, LONG_OPT_SNAPSHOT_FILE},
			{"snapshot-events", required_argument, 0, LONG_OPT_SNAPSHOT_WATCH_CONF},

			{"catalog-task-name", required_argument, 0, LONG_OPT_CATALOG_TASK_READABLE_NAME},
			{"catalog", required_argument, 0, LONG_OPT_CATALOG_SERVER},
			{"catalog-project", required_argument, 0, LONG_OPT_CATALOG_PROJECT},
			{"catalog-interval", required_argument, 0, LONG_OPT_CATALOG_INTERVAL},

			{"update-summary", no_argument, 0, LONG_OPT_UPDATE_SUMMARY},

			{0, 0, 0, 0}};

	/* By default, measure working directory. */
	resources_flags->disk = 1;

	/* Used in LONG_OPT_MEASURE_DIR */
	char measure_dir_name[PATH_MAX];

	while ((c = getopt_long(argc, argv, "c:d:fhi:L:l:o:O:vV:", long_options, NULL)) >= 0) {
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
			if (interval < 1) {
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
			if (template_path)
				free(template_path);
			template_path = xxstrdup(optarg);
			break;
		case LONG_OPT_TIME_SERIES:
			use_series = 1;
			break;
		case LONG_OPT_OPENED_FILES:
			use_inotify = 1;
			break;
		case LONG_OPT_NO_DISK_FOOTPRINT:
			resources_flags->disk = 0;
			follow_chdir = 0;
			break;
		case LONG_OPT_FOLLOW_CHDIR:
			follow_chdir = 1;
			break;
		case LONG_OPT_MEASURE_DIR:
			path_absolute(optarg, measure_dir_name, 0);
			if (!lookup_or_create_wd(NULL, measure_dir_name)) {
				debug(D_FATAL, "Directory '%s' does not exist.", optarg);
				exit(RM_MONITOR_ERROR);
			}
			break;
		case LONG_OPT_STOP_SHORT_RUNNING:
			stop_short_running = 1;
			break;
		case LONG_OPT_NO_PPRINT:
			pprint_summaries = 0;
			break;
		case LONG_OPT_SNAPSHOT_FILE:
			debug(D_FATAL, "This option has been replaced with --snapshot-events. Please consult the manual of resource_monitor.");
			exit(RM_MONITOR_ERROR);
			break;
		case LONG_OPT_SNAPSHOT_WATCH_CONF:
			snapshot_watch_events_file = xxstrdup(optarg);
			break;
		case LONG_OPT_PID: {
			int64_t p = atoi(xxstrdup(optarg));
			if (p < 1) {
				debug(D_FATAL, "Option --pid should be positive integer.");
				exit(RM_MONITOR_ERROR);
			}
			first_pid_manually_set = 1;
			first_process_pid = (pid_t)p;
		} break;
		case LONG_OPT_MEASURE_ONLY:
			enforce_limits = 0;
			break;
		case LONG_OPT_CATALOG_TASK_READABLE_NAME:
			catalog_task_readable_name = xxstrdup(optarg);
			break;
		case LONG_OPT_CATALOG_SERVER:
			catalog_hosts = xxstrdup(optarg);
			break;
		case LONG_OPT_CATALOG_PROJECT:
			catalog_project = xxstrdup(optarg);
			break;
		case LONG_OPT_CATALOG_INTERVAL:
			catalog_interval = atoi(optarg);
			if (catalog_interval < 1) {
				debug(D_FATAL, "--catalog-interval cannot be less than 1.");
			}
			break;
		case LONG_OPT_UPDATE_SUMMARY:
			update_summary_file = 1;
			break;
		default:
			show_help(argv[0]);
			return 1;
			break;
		}
	}

	if (follow_chdir && hash_table_size(wdirs) > 0) {
		debug(D_FATAL, "Options --follow-chdir and --measure-dir as mutually exclusive.");
		exit(RM_MONITOR_ERROR);
	}

	if (first_pid_manually_set) {
		if (follow_chdir || hash_table_size(wdirs) > 0 || child_in_foreground) {
			debug(D_FATAL, "Options --follow-chdir, --measure-dir, and --child-in-foreground cannot be used with --pid.");
			exit(RM_MONITOR_ERROR);
		}

		if (optind < argc || sh_cmd_line) {
			debug(D_FATAL, "A command line cannot be used with --pid.");
			exit(RM_MONITOR_ERROR);
		}
	}

	if (catalog_task_readable_name) {

		random_init();
		cctools_uuid_t *uuid = malloc(sizeof(*uuid));
		cctools_uuid_create(uuid);
		catalog_uuid = xxstrdup(uuid->str);
		free(uuid);

		char *tmp = getenv("USER");
		if (tmp) {
			catalog_owner = xxstrdup(tmp);
		} else {
			catalog_owner = "unknown";
		}

		if (!catalog_hosts) {
			catalog_hosts = xxstrdup(CATALOG_HOST);
		}

		if (!catalog_project) {
			catalog_project = xxstrdup(catalog_task_readable_name);
		}

		if (catalog_interval < 1) {
			catalog_interval = catalog_interval_default;
		}

		if (catalog_interval < interval) {
			warn(D_RMON, "catalog update interval (%" PRId64 ") is less than measurements interval (%" PRId64 "). Using the latter.");
			catalog_interval = interval;
		}

	} else if (catalog_hosts || catalog_project || catalog_interval) {
		debug(D_FATAL, "Options --catalog, --catalog-project, and --catalog-interval cannot be used without --catalog-task-name.");
		exit(RM_MONITOR_ERROR);
	}

	// this is ugly. if -c given, we should not accept any more arguments.
	//  if not given, we should get the arguments that represent the command line.
	if ((optind < argc && sh_cmd_line) || (optind >= argc && !sh_cmd_line && !first_pid_manually_set)) {
		show_help(argv[0]);
		return 1;
	}

	find_hostname();
	find_version();

	if (first_pid_manually_set) {
		if (!ping_process(first_process_pid)) {
			debug(D_FATAL, "Process with pid %d could not be found.", first_process_pid);
			exit(RM_MONITOR_ERROR);
		}

		command_line = "# following pid with --pid";

	} else if (sh_cmd_line) {
		argc = 3;
		optind = 0;

		argv_sh[2] = sh_cmd_line;
		argv = argv_sh;

		/* for pretty printing in the summary. */
		command_line = sh_cmd_line;

		char *sh_cmd_line_exec_escaped = string_escape_shell(sh_cmd_line);
		debug(D_RMON, "command line: /bin/sh -c %s\n", sh_cmd_line_exec_escaped);
		free(sh_cmd_line_exec_escaped);
	} else {
		buffer_t b;
		buffer_init(&b);

		char *sep = "";
		for (i = optind; i < argc; i++) {
			buffer_printf(&b, "%s%s", sep, argv[i]);
			sep = " ";
		}

		command_line = xxstrdup(buffer_tostring(&b));
		buffer_free(&b);

		debug(D_RMON, "command line: %s\n", command_line);
	}

	rmsummary_debug_report(resources_limits);

	if (getenv(RESOURCE_MONITOR_INFO_ENV_VAR)) {
		debug(D_NOTICE, "using upstream monitor. executing: %s\n", command_line);
		execlp("/bin/sh", "sh", "-c", command_line, (char *)NULL);
		// We get here only if execlp fails.
		debug(D_FATAL, "error executing %s: %s\n", command_line, strerror(errno));
		exit(RM_MONITOR_ERROR);
	}

	write_helper_lib();

	rmonitor_helper_init(lib_helper_name, &rmonitor_queue_fd, stop_short_running);

	summary_path = default_summary_name(template_path);

	if (use_series)
		series_path = default_series_name(template_path);

	if (use_inotify)
		opened_path = default_opened_name(template_path);

	log_series = open_log_file(series_path);
	log_inotify = open_log_file(opened_path);

	summary->command = xxstrdup(command_line);
	summary->start = ((double)usecs_since_epoch()) / ONE_SECOND;
	snapshot->start = summary->start;

#if defined(RESOURCE_MONITOR_USE_INOTIFY)
	if (log_inotify) {
		rmonitor_inotify_fd = inotify_init();
		alloced_inotify_watches = 100;
		inotify_watches = (char **)calloc(alloced_inotify_watches, sizeof(char *));
	}
#endif

	/* if we are not following changes in directory, and no directory was manually added, we follow the current
	 * working directory. */
	if (!follow_chdir || hash_table_size(wdirs) == 0) {
		lookup_or_create_wd(NULL, cwd);
	}

	set_snapshot_watch_events();

	if (first_pid_manually_set > 0) {
		rmonitor_track_process(first_process_pid);
	} else {
		executable = xxstrdup(argv[optind]);

		if (rmonitor_determine_exec_type(executable)) {
			debug(D_FATAL, "Error reading %s.", executable);
			exit(RM_MONITOR_ERROR);
		}

		spawn_first_process(executable, argv + optind, child_in_foreground);
	}

	rmonitor_resources(interval);
	rmonitor_final_cleanup();

	/* rmonitor_final_cleanup exits */
	return 0;
}

/* vim: set noexpandtab tabstop=4: */
