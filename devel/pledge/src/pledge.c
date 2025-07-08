#include <fcntl.h>
#include <stdbool.h>

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <getopt.h>

#include "list.h"
#include "util.h"

#include "libenforcer.h"

// Maximum number of arguments we are going to accept
#define ARGC_MAX 1024

// TODO: Linux devices are files (allegedly) but we should still find a way to test em...

// TODO: add unfinished path handling

// TODO: Add a flag in the contract that indicates if the file was created at runtime

//------------------------------------------
// Interesting thing to think about, im pretty sure that the parser is catching failed
// calls as successful, in the sense that it's saving AT_FDCWD as the directory
// We need a more clever parser
// New parser for tracer should:
// 1. Get a line
// 2. We parse for paths, be it in the form of </a/b/c> or "/a/b/c" "d/e"
// 3. Return all the paths it finds in an array of buffers
// 4. Then depending on what happened we can pick and choose
//
// Some functions call the AT_FDCWD file descriptor even when
// they dont use it
// so that gets resovled by strace and we end up adding cwd to our path list
// even though its not precise
//
// Another thought I have is the closing of files, I think something worth letting the
// user know about is if they keep opening or closing a file, that's a pattern that
// maybe the user doesn't want and could benefit from keeping the file open
// ---------------------------------

/// Angle bracket pattern: </a/b/c>
void ab_pattern(char *line,
		uint8_t access_fl,
		char *buf,
		size_t bufsiz,
		struct path_list **r)
{
	// TODO: we do per pattern, so this is for successful calls
	// but we should also add for the ENOENT calls, for now lets aim to
	// reproduce the original tracer
	for (size_t i = 0; line[i] != '\0'; i++) {
		if (line[i] == '<' && line[i + 1] == '/') {
			size_t j = 0;
			i++;
			while (1) {
				// that checking for nul is just in case theres an error with
				// the string
				if (line[i] == '>' || line[i] == '\0')
					break;
				if (j >= bufsiz)
					break;
				buf[j] = line[i];
				j++;
				i++;
			}
			buf[j] = '\0';
			add_path_to_list(r, buf, access_fl);
			// We save the pattern
			// and just extract paths, should be straightforward
		}
	}
	// TODO: Unfinished testing
	if (strstr(line, "unfinished") != NULL && strstr(line, "openat(") != NULL) {
		for (size_t i = 0; line[i] != '\0'; i++) {
			if (line[i] == '\"') {
				size_t j = 0;
				i++;
				while (1) {
					// Just safety checks lol
					if (line[i] == '\"' || line[i] == '\0')
						break;
					// bad bad bad
					if (j >= bufsiz)
						break;
					buf[j] = line[i];
					j++;
					i++;
				}
				buf[j] = '\0';
				size_t buf_len = strlen(buf);

				char abs_path[MAXPATHLEN] = {0};
				if (buf_len > 1 && buf[0] != '/') {
					rel2abspath(abs_path, buf, MAXPATHLEN);
					// TODO: this function needs to be reworked to return the
					// current, not the root
					add_path_to_list(r, abs_path, access_fl);
				}
				return;
			}
		}
	}
}

// TODO: This, along with the other might need to DIE, so we can make
// 1 single function that return ALL the strings in a line... :)
void quote_pattern(char *line,
		uint8_t access_fl,
		char *buf,
		size_t bufsiz,
		struct path_list **r)
{

	for (size_t i = 0; line[i] != '\0'; i++) {
		if (line[i] == '\"') {
			// rel2abspath
			size_t j = 0;
			i++;
			while (1) {
				// Just safety checks lol
				if (line[i] == '\"' || line[i] == '\0')
					break;
				// bad bad bad
				if (j >= bufsiz)
					break;
				buf[j] = line[i];
				j++;
				i++;
			}
			buf[j] = '\0';
			/* size_t buf_len              = strlen(buf); */
			char abs_path[MAXPATHLEN] = {0};
			rel2abspath(abs_path, buf, MAXPATHLEN);
			// this function needs to be reworked to return the current, not
			// the root
			add_path_to_list(r, abs_path, access_fl);
			// we return right away because we only want the first one
			return;
		}
	}
}

void pledge_help()
{
	fprintf(stderr, "PLEDGE: Tracing and enforcing\n");
	fprintf(stderr, "USAGE: pledge --[trace/enforce] command arg1 arg2 ...\n");
}

/// @param enf_cmd_idx: Enforcer command index, this is the index where the command for
/// the enforcer starts
void enforcer(int argc,
		char **argv,
		int enf_cmd_idx)
{
	char *prog[ARGC_MAX];
	// I would add another check here for the argument count but I don't think its wise
	// to have multiple places where we validate the same things (points of failure
	// should be different no?)
	// XXX: This can be turned into a function and we can pass the arguments array for
	// execvp to the tracer and the enforcer but lets see
	size_t j = 0;
	for (size_t i = enf_cmd_idx; (i < (size_t)argc && j < ARGC_MAX); i++) {
		// Save the program into the array that will call execvp
		prog[j] = argv[i];
		j++;
	}
	// arrays for execvp always end in NULL;
	prog[j] = NULL;
	pid_t pid;
	pid = fork();
	if (pid == -1) {
		fprintf(stderr, "Fork failed\n");
	} else if (pid == 0) {
		// ld_preload evnrionment variable
		char *ld_pl_env = getenv("LD_PRELOAD");

		// LD_PRELOAD was not set so we set it
		if (ld_pl_env == NULL) {
			if (setenv("LD_PRELOAD", "./minienforcer.so", 1)) {
				fprintf(stderr, "Failed to set LD_PRELOAD environment variable.\n");
			}
		} else // LD_PRELOAD is set so we append to it
		{
			size_t ld_len = strlen(ld_pl_env);
			char ld_pl[MAXPATHLEN + ld_len];
			ld_pl[0] = '\0';
			strncat(ld_pl, ld_pl_env, MAXPATHLEN);
			strncat(ld_pl, ":./minienforcer.so", MAXPATHLEN);
			if (setenv("LD_PRELOAD", ld_pl, 1)) {
				fprintf(stderr,
						"Failed to append to LD_PRELOAD environment variable.\n");
			}
		}

		if (execvp(prog[0], prog) == -1) {
			fprintf(stderr, "Could not execute program under enforcer...\n");
			exit(EXIT_FAILURE);
		}
	} else if (pid > 0) {
		int wait_status;
		wait(&wait_status);
		if (WIFEXITED(wait_status)) {
			if (WEXITSTATUS(wait_status)) {
				fprintf(stderr, "Error with enforcer...\n");
			}
		}
	}
}

/// @param tr_cmd_idx: Tracer command index, its the index in the argv array of where
/// the commands for the tracer start
void tracer(int argc,
		char **argv,
		int tr_cmd_idx)
{
	// we could do a large array and just place
	// In theory it should be fine because the pointers
	// in argv live for the entirety of the program
	char *prog[ARGC_MAX];
	prog[0] = "strace";
	prog[1] = "-f";
	prog[2] = "-y";
	prog[3] = "--trace=file,read,write,mmap";

#define MAX_LOG_LEN 127
	// TODO: For this log name stuff we need something to catch long names
	// and just assign a default name thats seeded by time or sumn
	char log_name[MAX_LOG_LEN] = {0};
	char contract_name[MAX_LOG_LEN] = {0}; // \0 == 0 lol
	int tr_cmd_count = argc - tr_cmd_idx;
	if (tr_cmd_count < 1) {
		fprintf(stderr, "Error obtaining count of arguments\n");
		return;
	}
	size_t j = 4;
	// we set it to tr_cmd_idx because that's where we want to start saving
	// commands
	for (size_t i = tr_cmd_idx, l = 0; i < (size_t)argc; i++, l++) {
		// this makes the array for execvp
		prog[j] = argv[i];
		size_t arg_len = strlen(argv[i]) + strlen(log_name); // looooool

		// we just want to append the command and its first argument
		// but we only do it if theres more than 1 argc
		if ((arg_len < MAX_LOG_LEN) && (l < 2) && argc > 1) {
			strcat(log_name, argv[i]);
			strcat(log_name, ".");
			strcat(contract_name, argv[i]);
			strcat(contract_name, ".");
		}
		j++;
	}
	// we ran the command "naked" of sorts
	// so if we run the program like:
	// pledge trace ls
	// then we only need to do ls
	if (tr_cmd_count == 1) {
		strcat(log_name, argv[tr_cmd_idx]);
		strcat(log_name, ".");
		strcat(contract_name, argv[tr_cmd_idx]);
		strcat(contract_name, ".");
	}
	strcat(log_name, "strace.log");
	strcat(contract_name, "contract");
	// We doing this with NULL at the end lol #execvp
	prog[j] = NULL;
	pid_t pid;
	pid = fork();
	if (pid == -1) {
		fprintf(stderr, "Fork failed\n");
	} else if (pid == 0) // child process
	{

		freopen(log_name, "w", stderr);
		if (execvp(prog[0], prog) == -1) {
			fprintf(stderr, "Could not start the process for the tracee.\n");
			exit(EXIT_FAILURE);
		}
		/* /1* fflush(stderr); *1/ */
		/* /1* freopen("/dev/tty", "w", stderr); *1/ */
		/* fflush(stderr); */
	} else if (pid > 0) // parent process
	{
		int wait_status;
		wait(&wait_status);
		if (WIFEXITED(wait_status)) {
			if (WEXITSTATUS(wait_status)) {
				fprintf(stderr, "Error with strace...\n");
			}
			fprintf(stderr, "Tracer: Strace log generated -> %s\n", log_name);
		}
		FILE *contract_s = fopen(log_name, "r");
		char *str;
		size_t str_siz = LINE_MAX * sizeof(char);
		str = malloc(str_siz);
		char pattern[MAXPATHLEN];
		struct path_list *root = NULL;
		// SECTION: Parsing
		while (getline(&str, &str_siz, contract_s) != -1) {
			// SECTION: openat
			// uhhhh what if the read command has an execve
			// O_RDONLY O_WRONLY O_RDWR
			// Reading the openat syscall man page andddd
			// we need to literally ignore the first argument,
			// because if the path given in 2nd parameter is relative
			// then it is used but the second path we parse is the actual file
			// if the 2nd parameter is absolute, first parameter is
			// ignored and at the end of the day:
			// the second path we parse is the actual file!!!!
			if (strstr(str, "openat(") != NULL) {
				if (strstr(str, "O_RDONLY") != NULL)
					ab_pattern(str, READ_ACCESS, pattern, MAXPATHLEN, &root);
				if (strstr(str, "O_WRONLY") != NULL)
					ab_pattern(str, WRITE_ACCESS, pattern, MAXPATHLEN, &root);
				if (strstr(str, "O_RDWR") != NULL)
					ab_pattern(str, READ_ACCESS | WRITE_ACCESS, pattern, MAXPATHLEN, &root);
				// TODO: unfinished operations check
			}
			// SECTION: Stat
			// We are going to treat stat as a read operation,
			// but stat wil probably eventually become a unique operation
			// im thinking something that can be upgraded so
			// say a path is marked with permission S (stat)
			// and when we are enforcing, if there tries to be an operation on
			// that path then we can upgrade said operation from stat to the
			// new operation but there have to be restrictions, a write
			// operation is more aggresive than a read
			else if (strstr(str, "newfstatat(") != NULL) {
				ab_pattern(str, STAT_ACCESS, pattern, MAXPATHLEN, &root);
				quote_pattern(str, STAT_ACCESS, pattern, MAXPATHLEN, &root);
			}
			// SECTION: read
			// The syscall itself tells u the operation lol
			// probably going to separate them
			else if (strstr(str, "read(") != NULL) {
				ab_pattern(str, READ_ACCESS, pattern, MAXPATHLEN, &root);
			} else if (strstr(str, "write(") != NULL) {
				ab_pattern(str, WRITE_ACCESS, pattern, MAXPATHLEN, &root);
			}
			// SECTION: execve
			// execve at the bottom because what if text containing
			// execve is a parameter of one of the functions (like read)
			else if (strstr(str, "execve(") != NULL) {
				// XXX: Is there any reason we should care if it's a
				// subprocess aside from the fact that we can find out if the
				// user is calling more programs What about subprocess of
				// subprocesses? Should we represent that too? elif "strace:
				// Process" in line:
				quote_pattern(str, READ_ACCESS, pattern, MAXPATHLEN, &root);
			}
			// SECTION: mmap
			// PROT_READ
			// PROT_WRITE
			// but heres question, if the file is MAP_PRIVATE,
			// do we want to care about it or no?
			// im guessing somewhere in between
			else if (strstr(str, "mmap(") != NULL) {
				/* if (strstr(str, "MAP_PRIVATE")) */
				if (strstr(str, "PROT_READ|PROT_WRITE") != NULL)
					ab_pattern(str, READ_ACCESS | WRITE_ACCESS, pattern, MAXPATHLEN, &root);
				if (strstr(str, "PROT_READ") != NULL)
					ab_pattern(str, READ_ACCESS, pattern, MAXPATHLEN, &root);
				if (strstr(str, "PROT_WRITE") != NULL)
					ab_pattern(str, WRITE_ACCESS, pattern, MAXPATHLEN, &root);
			}
		}
		free(str);
		/* dump_path_list(root); */

		FILE *ctr = fopen(contract_name, "w");
		generate_contract_from_list(ctr, root);
		fprintf(stderr, "Tracer: Contract generated   -> %s\n", contract_name);
		free_path_list(root);
		fclose(contract_s);
		fclose(ctr);
	}
}

int main(int argc,
		char **argv)
{
	int enforce_f = 0;
	int trace_f = 0;
	int c;
	int optidx = 0;
	struct option pl_args[] = {
			{
					"enforce",
					no_argument,
					&enforce_f,
					1,
			},
			{
					"trace",
					no_argument,
					&trace_f,
					1,
			},
			{NULL, 0, NULL, 0},
	};

	// We have 2 routes here:
	// 1. We can let the user place the flag anywhere and we just obtain the
	// program to execute through case 1: with getopt or
	// 2. We make the user use the verb before passing the program to execute
	// I think imma go with 2

	// Index of the command to execute under tracer
	int cmd_idx = 0;
	while ((c = getopt_long(argc, argv, "-", pl_args, &optidx)) != -1) {
		switch (c) {
		case 0:
			cmd_idx = optind;
			break;
			/* break; */
		}
		if ((enforce_f || trace_f)) {
			break;
		}
	}
	if (cmd_idx == argc) {
		fprintf(stderr, "No command provided after [%s]\n", pl_args[optidx].name);
		exit(EXIT_FAILURE);
	}
	if (trace_f) {
		fprintf(stderr, "Tracing started...\n");
		tracer(argc, argv, cmd_idx);
	} else if (enforce_f) {
		FILE *fl = fopen("minienforcer.so", "wb");
		fwrite(minienforcer, 1, minienforcer_len, fl);
		fclose(fl);
		// 0755 permissions
		// -rwxr-xr-x
		chmod("minienforcer.so",
				S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
		enforcer(argc, argv, cmd_idx);
		// XXX: maybe dont remove everytime, maybe we can cache and let the user decide
		// if we delete
		remove("minienforcer.so");
	} else {
		fprintf(stderr, "ERROR: No action provided for PLEDGE...\n");
		pledge_help();
		exit(EXIT_FAILURE);
	}
}
