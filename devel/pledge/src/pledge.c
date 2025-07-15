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

#include "list_util.h"
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

/// Used to sanitize our paths.
/// This functions replaces every occurrence of char a with char b
void replace_in_str(char *src, char a, char b)
{
	size_t src_len = strlen(src);
	for (size_t i = 0; i < src_len; i++) {
		if (src[i] == a)
			src[i] = b;
	}
}

// FIXME: The ab_pattern and quote_pattern parsers gotta go,
// going to replace with a system that parses paths in various passes
// It should:
// 1. Do multiple passes extracting the paths in a line
// 2. Label them as unfinished or not
// 3. Return every path without doing rel2abspath
// 4. Then we inspect the paths and decide how to turn it absolute, doing everything
// in the parsing is WRONGGGGG
/// Angle bracket pattern: </a/b/c>
void ab_pattern(struct list **r,
		char *line,
		uint8_t access_fl,
		char *buf,
		size_t bufsiz)
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
			add_path_to_contract_list(r, buf, access_fl);
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
					add_path_to_contract_list(r, abs_path, access_fl);
				}
				return;
			}
		}
	}
}

// TODO: This, along with the other might need to DIE, so we can make
// 1 single function that return ALL the strings in a line... :)
void quote_pattern(struct list **r,
		char *line,
		uint8_t access_fl,
		char *buf,
		size_t bufsiz)
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
			char abs_path[MAXPATHLEN] = {0};
			rel2abspath(abs_path, buf, MAXPATHLEN);
			// this function needs to be reworked to return the current, not
			// the root
			add_path_to_contract_list(r, abs_path, access_fl);
			// we return right away because we only want the first one
			return;
		}
	}
}

void pledge_help(void)
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

	// TODO: For this log name stuff we need something to catch long names
	// and just assign a default name thats seeded by time or sumn
	char log_name[FILENAME_MAX] = {0};
	char contract_name[FILENAME_MAX] = {0}; // \0 == 0 lol

	// Tracer command count, the amount of arguments we have, that we are going to feed
	// strace
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
		if ((arg_len < FILENAME_MAX) && (l < 2) && tr_cmd_count > 1) {
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
	size_t log_name_len = strlen(log_name);
	size_t contract_name_len = strlen(contract_name);

	// We do not want these characters for the file names
	// we remove the relative naming with leading dot and slash
	// or we remove the leading slash
	// everything else is ok
	if (log_name[0] == '.') {
		if (log_name[1] == '/') {
			memmove(log_name, log_name + 2, log_name_len);
		} else
			memmove(log_name, log_name + 1, log_name_len);

	} else if (log_name[0] == '/') {
		memmove(log_name, log_name + 1, log_name_len);
	}

	if (contract_name[0] == '.') {
		if (contract_name[1] == '/') {
			memmove(contract_name, contract_name + 2, contract_name_len);
		} else
			memmove(contract_name, contract_name + 1, contract_name_len);

	} else if (contract_name[0] == '/') {
		memmove(contract_name, contract_name + 1, contract_name_len);
	}

	// Sanitize path, since we dont want those slashes in the name
	replace_in_str(log_name, '/', '_');
	replace_in_str(contract_name, '/', '_');
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
	} else if (pid > 0) // parent process
	{
		int wait_status;
		wait(&wait_status);
		if (WIFEXITED(wait_status)) {
			if (WEXITSTATUS(wait_status)) {
				fprintf(stderr, "Error with strace...\n");
			}
			fprintf(stderr, "[Tracer: Strace log generated -> %s]\n", log_name);
		}
		FILE *strace_raw = fopen(log_name, "r");
		if (strace_raw == NULL) {
			fprintf(stderr, "Failed to create the log file...\n");
			exit(EXIT_FAILURE);
		}
		char *strace_line;
		size_t str_siz = LINE_MAX * sizeof(char);
		strace_line = malloc(str_siz);
		char pattern[MAXPATHLEN];
		struct list *root = NULL;
		uint8_t temp_access_fl = 0x0;

		// SECTION: Parsing
		while (getline(&strace_line, &str_siz, strace_raw) != -1) {
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
			if (strstr(strace_line, "openat(") != NULL) {
				if (strstr(strace_line, "O_CREAT") != NULL)
					temp_access_fl |= CREATE_ACCESS;
				if (strstr(strace_line, "O_RDONLY") != NULL)
					temp_access_fl |= READ_ACCESS;
				if (strstr(strace_line, "O_WRONLY") != NULL)
					temp_access_fl |= WRITE_ACCESS;
				if (strstr(strace_line, "O_RDWR") != NULL)
					temp_access_fl |= (READ_ACCESS | WRITE_ACCESS);

				if (temp_access_fl) {
					ab_pattern(&root, strace_line, temp_access_fl, pattern, MAXPATHLEN);
				}
				temp_access_fl = 0x0;
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
			else if (strstr(strace_line, "newfstatat(") != NULL) {
				ab_pattern(&root, strace_line, STAT_ACCESS, pattern, MAXPATHLEN);
				quote_pattern(&root, strace_line, STAT_ACCESS, pattern, MAXPATHLEN);
			}
			// SECTION: read
			// The syscall itself tells u the operation lol
			// probably going to separate them
			else if (strstr(strace_line, "read(") != NULL) {
				ab_pattern(&root, strace_line, READ_ACCESS, pattern, MAXPATHLEN);
			} else if (strstr(strace_line, "write(") != NULL) {
				ab_pattern(&root, strace_line, WRITE_ACCESS, pattern, MAXPATHLEN);
			}
			// SECTION: execve
			// execve at the bottom because what if text containing
			// execve is a parameter of one of the functions (like read)
			else if (strstr(strace_line, "execve(") != NULL) {
				// XXX: Is there any reason we should care if it's a
				// subprocess aside from the fact that we can find out if the
				// user is calling more programs What about subprocess of
				// subprocesses? Should we represent that too? elif "strace:
				// Process" in line:
				quote_pattern(&root, strace_line, READ_ACCESS, pattern, MAXPATHLEN);
			}
			// SECTION: mmap
			// PROT_READ
			// PROT_WRITE
			// but heres question, if the file is MAP_PRIVATE,
			// do we want to care about it or no?
			// im guessing somewhere in between
			// THINK: Should MAP_PRIVATE be handled a certain way?
			else if (strstr(strace_line, "mmap(") != NULL) {
				if (strstr(strace_line, "PROT_READ|PROT_WRITE") != NULL)
					temp_access_fl = (READ_ACCESS | WRITE_ACCESS);
				if (strstr(strace_line, "PROT_READ") != NULL)
					temp_access_fl = READ_ACCESS;
				if (strstr(strace_line, "PROT_WRITE") != NULL)
					temp_access_fl = WRITE_ACCESS;

				if (temp_access_fl) {
					ab_pattern(&root, strace_line, temp_access_fl, pattern, MAXPATHLEN);
				}
				temp_access_fl = 0x0;
			}
			// Delete operation needs write access,
			// but we do not care about access rights, only what is actually done,
			// although... deleting, even if its metadata,
			// does write something to the disk kinda ish
			else if (strstr(strace_line, "unlinkat(") != NULL) {
				quote_pattern(&root, strace_line, DELETE_ACCESS | WRITE_ACCESS, pattern, MAXPATHLEN);
			}
		}
		free(strace_line);

		FILE *ctr = fopen(contract_name, "w");
		if (ctr == NULL) {
			fprintf(stderr, "Failed to open contract file for writing...\n");
		}
		generate_contract_from_list(ctr, root);
		fprintf(stderr, "[Tracer: Contract generated   -> %s]\n", contract_name);
		destroy_contract_list(root);
		list_delete(root);
		fclose(strace_raw);
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
		fprintf(stderr, "[Tracing started...]\n");
		tracer(argc, argv, cmd_idx);
	} else if (enforce_f) {
		FILE *mini_enf_f = fopen("minienforcer.so", "wb");
		if (mini_enf_f == NULL) {
			fprintf(stderr, "Failed to generate minienforcer.so...\n");
			exit(EXIT_FAILURE);
		}
		fwrite(minienforcer, 1, minienforcer_len, mini_enf_f);
		fclose(mini_enf_f);
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
