/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "stringtools.h"
#include "cctools.h"
#include "debug.h"
#include "random.h"

#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <signal.h>

typedef enum {
	STATE_READY,
	STATE_STARTED,
	STATE_RUNNING,
	STATE_STOP_WAIT,
	STATE_KILL_WAIT,
	STATE_STOPPED
} state_t;

static const char *state_name[] = {
	"READY",
	"STARTED",
	"RUNNING",
	"STOP_WAIT",
	"KILL_WAIT",
	"STOPPED"
};

static state_t state = STATE_READY;
static time_t state_start;
static int state_changes = 0;
static int want_to_exit = 0;
static int start_failures = 0;
static pid_t pid;
static char **program_argv;
static time_t program_mtime;
static time_t program_ctime;
static unsigned int min_wait_time = 10;
static unsigned int max_wait_time = 600;
static int start_interval = 60;
static int stop_interval = 60;
static int check_interval = 3600;
static time_t last_check_time = 0;

static void change_state(int newstate)
{
	debug(D_DEBUG, "%s -> %s", state_name[state], state_name[newstate]);
	state = newstate;
	state_start = time(0);
	state_changes++;
}

static int program_changed()
{
	struct stat info;
	int result;
	time_t current = time(0);

	if(!last_check_time) {
		last_check_time = current - check_interval * (rand() / (double) RAND_MAX);
	}

	if((last_check_time + check_interval) > current) {
		/* not time to check yet */
		return 0;
	}

	last_check_time = current;

	result = stat(program_argv[0], &info);
	if(result != 0) {
		debug(D_DEBUG, "couldn't stat %s: %s", program_argv[0], strerror(errno));
		return 0;
	}

	if(info.st_mtime != program_mtime || info.st_ctime != program_ctime) {
		debug(D_DEBUG, "%s has changed since it was started.", program_argv[0]);
		return 1;
	} else {
		debug(D_DEBUG, "%s is unchanged.", program_argv[0]);
		return 0;
	}
}

static int start_program()
{
	struct stat info;
	int result;

	result = stat(program_argv[0], &info);
	if(result != 0) {
		debug(D_DEBUG, "couldn't stat %s: %s\n", program_argv[0], strerror(errno));
		return 0;
	}

	program_mtime = info.st_mtime;
	program_ctime = info.st_ctime;

	pid = fork();
	if(pid == 0) {
		setpgid(getpid(), getpid());
		execv(program_argv[0], program_argv);
		exit(1);
	} else if(pid > 0) {
		debug(D_DEBUG, "%s started as pid %d", program_argv[0], pid);
		return 1;
	} else {
		debug(D_DEBUG, "unable to fork: %s\n", strerror(errno));
		return 0;
	}
}

static int program_exited()
{
	int status;
	pid_t checkpid;
	checkpid = waitpid(pid, &status, WNOHANG);
	if(checkpid == pid) {
		if(WIFEXITED(status)) {
			int exitcode = WEXITSTATUS(status);
			debug(D_DEBUG, "%s pid %d exited normally with code %d", program_argv[0], pid, exitcode);
		} else {
			int signum = WTERMSIG(status);
			debug(D_DEBUG, "%s pid %d exited abnormally with signal %d", program_argv[0], pid, signum);
		}
		return 1;
	} else {
		return 0;
	}
}

static void send_stop_signal()
{
	kill(pid, SIGTERM);
}

static void send_kill_signal()
{
	kill(-pid, SIGKILL);
}

static void handle_signal(int sig)
{
	debug(D_DEBUG, "received signal %d: %s", sig, string_signal(sig));
	switch (sig) {
	case SIGINT:
	case SIGTERM:
	case SIGQUIT:
		want_to_exit = 1;
		break;
	case SIGHUP:
		change_state(STATE_STOP_WAIT);
		break;
	}
}

static void install_handler(int sig, void (*handler) (int sig))
{
	struct sigaction s;
	s.sa_handler = handler;
	sigfillset(&s.sa_mask);
	s.sa_flags = 0;
	sigaction(sig, &s, 0);
}

void show_help(const char *cmd)
{
	printf("use: %s [options] <program> <program-args>\n", cmd);
	printf("Where options are:\n");
	printf("   -d <subsys>  Enable debugging for this subsystem.  (try -d all)\n");
	printf("   -o <file>    Send debugging output to this file.\n");
	printf("   -O <size>    Rotate debug files after this size.\n");
	printf("   -c <time>    Time between checks for program change.  (default: %ds)\n", check_interval);
	printf("   -m <time>    Minimum time to wait before restarting program. (default: %ds)\n", min_wait_time);
	printf("   -M <time>    Maximum time to wait before restarting program. (default: %ds)\n", max_wait_time);
	printf("   -s <time>    Minimum time program must run to be considered successful. (default: %ds)\n", start_interval);
	printf("   -S <time>    Time to wait between soft-kill and hard-kill. (default: %ds)\n", stop_interval);
	printf("   -v           Show version string.\n");
	printf("   -h           Show help screen.\n");
	printf("Note: Time values may be specified in seconds, or with an optional\n");
	printf("letter (s,m,h,d) to indicate seconds, minutes, hours or days.\n");
	printf("Examples: 5s is five seconds; 10m is ten minutes; 15h is fifteen hours.\n");
	exit(1);
}

int main(int argc, char *argv[])
{
	signed char c;

	random_init();

	install_handler(SIGINT, handle_signal);
	install_handler(SIGTERM, handle_signal);
	install_handler(SIGQUIT, handle_signal);
	install_handler(SIGCHLD, handle_signal);
	install_handler(SIGHUP, handle_signal);

	debug_config(argv[0]);

	while(((c = getopt(argc, argv, "+d:o:O:m:M:s:S:vh")) > -1)) {
		switch (c) {
		case 'c':
			check_interval = string_time_parse(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'm':
			min_wait_time = string_time_parse(optarg);
			break;
		case 'M':
			max_wait_time = string_time_parse(optarg);
			break;
		case 's':
			start_interval = string_time_parse(optarg);
			break;
		case 'S':
			stop_interval = string_time_parse(optarg);
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(0);
			break;
		case 'h':
		default:
			show_help(argv[0]);
			break;
		}
	}

	cctools_version_debug(D_DEBUG, argv[0]);

	if(optind >= argc) {
		show_help(argv[0]);
		return 1;
	}

	program_argv = &argv[optind];
	state_start = time(0);
	state = STATE_READY;

	if(program_argv[0][0] != '/') {
		fprintf(stderr, "watchdog: please give me the full path to %s\n", program_argv[0]);
		return 1;
	}

	while(1) {
		time_t time_in_state = time(0) - state_start;

		switch (state) {
		case STATE_READY:
			if(start_program()) {
				change_state(STATE_STARTED);
			} else {
				change_state(STATE_STOPPED);
				start_failures++;
			}
			break;
		case STATE_STARTED:
			if(program_exited()) {
				change_state(STATE_STOPPED);
				start_failures++;
			} else if(time_in_state > start_interval) {
				change_state(STATE_RUNNING);
			} else if(want_to_exit) {
				change_state(STATE_STOP_WAIT);
			}
			break;
		case STATE_RUNNING:
			start_failures = 0;
			if(program_exited()) {
				change_state(STATE_STOPPED);
			} else if(program_changed()) {
				change_state(STATE_STOP_WAIT);
			} else if(want_to_exit) {
				change_state(STATE_STOP_WAIT);
			}
			break;
		case STATE_STOP_WAIT:
			send_stop_signal();
			if(program_exited()) {
				change_state(STATE_STOPPED);
			} else if(time_in_state > stop_interval) {
				change_state(STATE_KILL_WAIT);
			}
			break;
		case STATE_KILL_WAIT:
			send_kill_signal();
			if(program_exited()) {
				change_state(STATE_STOPPED);
			}
			break;
		case STATE_STOPPED:
			if(want_to_exit) {
				debug(D_DEBUG, "all done");
				exit(0);
			} else {
				unsigned int wait_time;
				int i;

				wait_time = min_wait_time;
				for(i = 0; i < start_failures; i++) {
					wait_time *= 2;
				}
				if(wait_time > max_wait_time || wait_time < min_wait_time) {
					wait_time = max_wait_time;
				}
				if(time_in_state >= (int) wait_time) {
					change_state(STATE_READY);
				}
			}
			break;
		default:
			fatal("invalid state %d!\n", state);
			break;
		}
		if(state_changes == 0) {
			sleep(5);
		} else {
			state_changes = 0;
		}
	}
	return 1;
}

/* vim: set noexpandtab tabstop=8: */
