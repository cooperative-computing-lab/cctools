/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/


/* Monitors a set of programs for CPU load average, memory and
 * disk utilization. The monitor works 'indirectly', that is, by
 * observing how the environment changed while a process was
 * running, therefore all the information reported should be
 * considered just as an estimate (this is in contrast with
 * direct methods, such as ptrace).
 *
 * Each monitor target has three functions: get_TARGET_usage,
 * hdr_TARGET_usage, and log_TARGET_usage. For example, for
 * memory we have get_mem_usage, hdr_mem_usage, and
 * log_mem_usage. In general, all functions return 0 on success,
 * or some other integer on failure. The exception are function
 * that open files, which return NULL on failure, or a file
 * pointer on success.
 *
 * The get_TARGET_usage functions are called at intervals.
 * Between each interval, the monitor does nothing. For each
 * process monitored, a log text file is written, called
 * log-PID-XXXXXX, in which PID is the pid of the process, and
 * XXXXXX a random set of six digits.
 *
 * log_TARGET_usage writes the corresponding information to the
 * log. Each field is separated by \t. hdr_TARGET_usage is called
 * only once, and it writes the title of the corresponding column
 * to the log file.
 *
 * Currently, the columns are:
 *
 * wall:   Wall time (in clicks).
 * user:   Time the process has spent in user mode (in clicks).
 * kernel: Time the process has spent in kernel mode (in clicks).
 * load:   (user + kernel)/wall
 * vmem:   Current total virtual memory size.
 * rssmem  Current total resident memory size.
 * shmem   Amount of shared memory.
 * frBlks  Free blocks of the working directory's filesystem. 
 * AvBlks  Available blocks of the working directory's filesystem. 
 * frNodes Free nodes of the working directory's filesystem. 
 * files   File count of the working directory.
 * dirs    Directory count of the working directory.
 * bytes   Total byte count of files in the working directory.
 * blks    Block count (512 bytes) in the working directory.
 * rchars  Read char count using *read system calls.
 * wchars  Writen char count using *write system calls.
 *
 * The log files are written to the home directory of the monitor
 * process. A flag will be added later to indicate a prefered
 * output directory.
 *
 * While all the logic supports the monitoring of several
 * processes by the same monitor, only one monitor can
 * be specified at the command line. This is because we plan to
 * wrap the calls to fork and clone in the monitor such that we
 * can also monitor the process children.
 *
 * Each monitored process gets a 'struct monitor_info', itself
 * composed of 'struct mem_info', 'struct load_info', etc. There
 * is a global variable that keeps a table relating pids to
 * the corresponding struct monitor_info.
 *
 * The monitor program handles SIGCHLD, by either retrieving the
 * last usage of the child (getrusage through waitpid) and
 * removing it from the table above described, or logging SIGSTOP
 * and SIGCONT.
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
 * We sleep one second waiting for the child process to be
 * created, which is not very good form.
 *
 * If the process writes something outside the working directory,
 * right now we are out of luck.
 *
 */

#include "itable.h"
#include "debug.h"

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

#ifdef __FREEBSD
	#include <sys/param.h>
    #include <sys/mount.h>
#else
	#include  <sys/vfs.h>
#endif


#define DEFAULT_INTERVAL 60 /* in seconds */

struct itable *children; /* Maps the pid of a process to a unique struct monitor. */

struct mem_info
{
	uint64_t virtual; 
	uint64_t resident;
	uint64_t shared;
	uint64_t text;
	uint64_t data;
};

//time in clicks, no seconds:
struct load_info
{
	double            cpu_wall_ratio;
	unsigned long int wall_time;
	unsigned long int user_time;
	unsigned long int kernel_time;
};

struct file_info
{
	int      files;
	int      directories;
	off_t    byte_count;
	blkcnt_t block_count;
};

struct io_info
{
	unsigned long int chars_read;
	unsigned long int chars_written;
};

struct monitor_info
{
	pid_t       pid;
	const char *cmd;
	int         running;
	FILE       *log_file;
	struct timeval time_initial;

	struct mem_info  mem;
	struct load_info load;
	struct file_info file;
	struct io_info   io;

	struct statfs    disk;
	struct statfs    disk_initial;
};

FILE *open_log_file(pid_t pid, const char *prefix)
{
	FILE *log;
	char flog_path[PATH_MAX];	
	sprintf(flog_path, "%s-%d-XXXXXX", prefix, pid);
	mkstemp(flog_path);

	if((log = fopen(flog_path, "w")) == NULL)
	{
		debug(D_DEBUG, "monitor: could not open log file %s : %s\n", flog_path, strerror(errno));
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
				debug(D_DEBUG, "monitor: could not process file %s : %s\n", fproc_path, strerror(errno));
				return NULL;
		}

		return fproc;
}

int get_disk_usage(struct statfs *disk)
{
	char cwd[PATH_MAX];

	if(statfs(getcwd(cwd, PATH_MAX - 1), disk) > 0)
	{
		debug(D_DEBUG, "monitor: could statfs on %s : %s\n", cwd, strerror(errno));
		return 1;
	}

	return 0;
}

void log_disk_usage(FILE *log_file, struct statfs *disk, struct statfs *disk_initial)
{
	/* Free blocks . Available blocks . Free nodes */

	fprintf(log_file, "%ld\t", disk->f_bfree - disk_initial->f_bfree);
	fprintf(log_file, "%ld\t", disk->f_bavail - disk_initial->f_bavail);
	fprintf(log_file, "%ld", disk->f_ffree - disk_initial->f_ffree);
}

void hdr_disk_usage(FILE *log_file)
{
	fprintf(log_file, "frBlks\tavBlks\tfrNodes");
}

int get_file_usage(struct file_info *file)
{
	char *argv[] = {".", NULL};
	FTS *hierarchy;
	FTSENT *entry;
	memset(file, 0, sizeof(struct file_info));

	hierarchy = fts_open(argv, FTS_PHYSICAL, NULL);

	if(!hierarchy)
	{
		debug(D_DEBUG, "monitor: fts_open error: %s\n", strerror(errno));
		return 1;
	}

	while( (entry = fts_read(hierarchy)) )
	{
		switch(entry->fts_info)
		{
			case FTS_D:
				file->directories++;
				break;
			case FTS_DC:
			case FTS_DP:
				break;
			case FTS_SL:
			case FTS_DEFAULT:
				file->files++;
				break;
			case FTS_F:
				file->files++;
				file->byte_count  += entry->fts_statp->st_size;
				file->block_count += entry->fts_statp->st_blocks;
				break;
			case FTS_ERR:
				debug(D_DEBUG, "monitor: fts_read error %s: %s\n", entry->fts_name, strerror(errno));
				break;
			default:
				break;
		}
	}

	fts_close(hierarchy);

	return 0;
}

void log_file_usage(FILE *log_file, struct file_info *file)
{
	/* files . dirs . bytes . blocks */
	fprintf(log_file, "%d\t%d\t%d\t%d", file->files, file->directories, (int) file->byte_count, (int) file->block_count);

}

void hdr_file_usage(FILE *log_file)
{
	fprintf(log_file, "files\tdirs\tbytes\tblks");
}


double timeval_to_double(struct timeval *time, struct timeval *origin)
{
	double  secs = time->tv_sec  - origin->tv_sec;
	double usecs = time->tv_usec - origin->tv_usec;

	return(secs + usecs/1000000.0);

}

int get_load_usage(pid_t pid, struct timeval *time_initial, struct load_info *load)
{
	/* /dev/proc/[pid]/stat */
	
	struct timeval time;
	
	FILE *fcpu = open_proc_file(pid, "stat");
	if(!fcpu)
	{
		return 1;
	}

	gettimeofday(&time, NULL);

	fscanf(fcpu,
			"%*s" /* pid */ "%*s" /* cmd line */ "%*s" /* state */ "%*s" /* pid of parent */
			"%*s" /* group ID */ "%*s" /* session id */ "%*s" /* tty pid */ "%*s" /* tty group ID */
			"%*s" /* linux/sched.h flags */ "%*s %*s %*s %*s" /* faults */
			"%lu" /* user mode time (in clock ticks) */
			"%lu" /* kernel mode time (in clock ticks) */
			/* .... */,
			&load->user_time, &load->kernel_time);

	load->wall_time = (time.tv_sec - time_initial->tv_sec) * sysconf(_SC_CLK_TCK); 

	if(load->wall_time > 0)
		load->cpu_wall_ratio = (load->user_time + load->kernel_time)/(1.0 * load->wall_time);
	else
		load->cpu_wall_ratio = 0;

	return 0;
}

void log_load_usage(FILE *log_file, struct load_info *load)
{
	/* wall . user . kernel . load */
	fprintf(log_file, "%ld\t",  load->wall_time);
	fprintf(log_file, "%ld\t%ld\t%4.4lf", load->user_time, load->kernel_time, load->cpu_wall_ratio);
}

void hdr_load_usage(FILE *log_file)
{
	fprintf(log_file, "wall\tuser\tkernel\tload");
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

void log_mem_usage(FILE *log_file, struct mem_info *mem)
{
	/* total virtual . resident . shared */
	fprintf(log_file, "%" PRIu64 "\t%" PRIu64 "\t%" PRIu64,
				mem->virtual, mem->resident, mem->shared);
}

void hdr_mem_usage(FILE *log_file)
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

void log_io_usage(FILE *log_file, struct io_info *io)
{
	/* total chars read . total chars written */
	fprintf(log_file, "%" PRIu64 "\t%" PRIu64, io->chars_read, io->chars_written);
}
		
void hdr_io_usage(FILE *log_file)
{
	fprintf(log_file, "rchars\twchars");
}


#define log_order load mem disk file io

void monitor_log_hdr(struct monitor_info *m)
{
	hdr_load_usage(m->log_file);
	fprintf(m->log_file, "\t");

	hdr_mem_usage(m->log_file);
	fprintf(m->log_file, "\t");

	hdr_disk_usage(m->log_file);
	fprintf(m->log_file, "\t");

	hdr_file_usage(m->log_file);
	fprintf(m->log_file, "\t");

	hdr_io_usage(m->log_file);
	fprintf(m->log_file, "\n");
}

void monitor_log(struct monitor_info *m)
{
	log_load_usage(m->log_file, &m->load);
	fprintf(m->log_file, "\t");

	log_mem_usage(m->log_file,  &m->mem);
	fprintf(m->log_file, "\t");

	log_disk_usage(m->log_file, &m->disk, &m->disk_initial);
	fprintf(m->log_file, "\t");

	log_file_usage(m->log_file, &m->file);
	fprintf(m->log_file, "\t");

	log_io_usage(m->log_file, &m->io);
	fprintf(m->log_file, "\n");

	fflush(m->log_file);
}


int monitor_once(struct monitor_info *m, int counter)
{
	//check memory every memt, load every loadt, and disk every
	//dist intervals.
	int memt = 1, loadt = 1, diskt = 1;

	int change = 0;
	if( counter % memt == 0)
	{
		get_mem_usage(m->pid, &m->mem);
		change = 1;
	}
	if( counter % loadt == 0)
	{
		get_load_usage(m->pid, &m->time_initial, &m->load);
		change = 1;
	}
	if( counter % diskt == 0)
	{
		get_disk_usage(&m->disk);
		get_file_usage(&m->file);
		get_io_usage(m->pid, &m->io);
		change = 1;
	}

	counter++; //if counter overflows, doing mod arithmetic, so that's ok.

	if(change)
		monitor_log(m);

	return change;
}


int monitor_children(long int interval /*in seconds */)
{
	pid_t pid;
	struct monitor_info *mon_info;
	struct timeval timeout;

	uint64_t i = 0;
	do
	{ 
		itable_firstkey(children);
		while(itable_nextkey(children, (uint64_t *) &pid, (void **) &mon_info))
			monitor_once(mon_info, i);

		/* wait for interval seconds. */
		timeout.tv_sec  = interval;
		timeout.tv_usec = 0;
		select(0, NULL, NULL, NULL, &timeout);
	} while(itable_size(children) > 0);

	return 0;
}

	
/* this is some ugly code... need to prettify */
struct monitor_info *spawn_child(const char *cmd)
{
	pid_t pid;

	struct statfs disk_initial;

	get_disk_usage(&disk_initial);
	
	pid = fork();

	if(pid > 0)
	{
		struct monitor_info *m = malloc(sizeof(struct monitor_info));
		memset(m, 0, sizeof(struct monitor_info));

        close(STDIN_FILENO);
        close(STDOUT_FILENO);

		setpgid(pid, 0);
		m->pid     = pid;
		m->running = 1;

		gettimeofday(&m->time_initial, NULL);
		memcpy(&m->disk_initial, &disk_initial, sizeof(struct statfs));
		
		m->log_file   = open_log_file(pid, "log");

		fprintf(m->log_file, "command:\t%s\n", cmd);
		monitor_log_hdr(m);

		return m;
	}
	else if(pid < 0)
	{
		fatal("monitor: fork failed: %s\n", strerror(errno));
		return NULL;
	}
	else //child
	{
		sleep(1); //hack so we get initial disk and time. must find a better solution!
		execlp("sh", "sh", "-c", cmd, (char *) NULL);
		//We get here only if execlp fails.
		fatal("monitor: error executing %s:\n", cmd, strerror(errno));
		return NULL;
	}

}

void check_child(const int signal)
{
	int status;
	siginfo_t cinfo;

	//which children we got a signal about?
	if(waitid(P_ALL, 0, &cinfo, WEXITED | WSTOPPED | WCONTINUED | WNOWAIT) != 0)
		return;

	//monitor that process once more, maybe for the last time if
	//WEXITED above.
	struct monitor_info *m = itable_lookup(children, cinfo.si_pid);
	monitor_once(m, 0);

	//die zombie die
	waitpid(cinfo.si_pid, &status, WNOHANG);

	if( WIFEXITED(status) )
	{
			fprintf(m->log_file, "\nProcess %d finished normally: %d.\n", m->pid, WEXITSTATUS(status) );
			itable_remove(children, m->pid);
			free(m);
	} 
	else if ( WIFSIGNALED(status) )
	{
			fprintf(m->log_file, "\nProcess %d terminated with signal: %s.\n", m->pid, strsignal(WTERMSIG(status)) );
			itable_remove(children, m->pid);
			free(m);
	} 
	else if ( WIFSTOPPED(status) )
	{
			fprintf(m->log_file, "\nProcess %d on hold with signal: %s.\n", m->pid, strsignal(WIFSTOPPED(status)) );
			m->running = 0;
	} 
	else if ( WIFCONTINUED(status) )
	{
			fprintf(m->log_file, "\nProcess %d received SIGCONT.\n", m->pid );
			m->running = 1;
	}

}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options] <dagfile>\n", cmd);
	fprintf(stdout, "-i <n>			Interval bewteen observations, in seconds. (default=%d)\n", DEFAULT_INTERVAL);
	fprintf(stdout, "-d <subsystem>		Enable debugging for this subsystem.\n");
	fprintf(stdout, "-o <directory>		Write logs to this directory. NOT IMPLEMENTED (default=.)\n");
}



int main(int argc, char **argv) {
	int i;
	struct monitor_info *m;
	char cmd[1024] = {'\0'};
	char c;
	uint64_t interval = DEFAULT_INTERVAL;

	debug_config(argv[0]);
	signal(SIGCHLD, check_child);


	while((c = getopt(argc, argv, "d:i:")) > 0)
	{
		switch (c) {
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'i':
				interval = strtoll(optarg, NULL, 10);
				if(interval < 1)
					fatal("monitor: interval cannot be set to less than one second.");
					break;
			default:
				show_help(argv[0]);
				return 1;
				break;
		}
	}
		

	children = itable_create(0);

	//this is ugly, concatenating command and arguments
	for(i = optind; i < argc; i++)
	{
		strcat(cmd, argv[i]);
		strcat(cmd, " ");
	}

	m = spawn_child(cmd);

	itable_insert(children, m->pid, (void *) m);

	monitor_children(interval);

	fclose(m->log_file);

	return 0;

}
