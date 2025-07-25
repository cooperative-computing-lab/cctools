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

// TODO: Add a flag in the contract that indicates if the file was created at runtime

//------------------------------------------
// Another thought I have is the closing of files, I think something worth letting the
// user know about is if they keep opening or closing a file, that's a pattern that
// maybe the user doesn't want and could benefit from keeping the file open
// -----------------------------------------

/// A path bundle is a struct containing all the paths parsed in a line,
/// grouped by the pattern they got extracted from and saved in an array for each pattern.
/// So the reason we use an array of predefined size strings
/// as opposed to an array of malloc'd strings is that
/// if we are obtaining all the paths in a line, and every line we have to
/// allocate at least 1 string and at most 3,
/// so if we go through a long file (think >2000 lines, then
/// this is a lot of UNNECESSARY allocations, and we COULD get clever,
/// allocate once and reuse buffers right?
/// but that at that point let's just use a fixed size buffer
struct path_bundle {
	/// Angle bracket patterns
	/// These are paths that take the form of </a/b/c>
	char ab_paths[4][MAXPATHLEN];
	/// How many angle bracket paths
	uint32_t ab_count;
	/// Quote patterns
	/// These are paths that take the form of "/a/b/c"
	char quote_paths[4][MAXPATHLEN];
	/// How many quote paths we got
	uint32_t quote_count;
	/// If it resulted in an ENOENT
	uint8_t noent;
	/// If the path contains unfinished
	uint8_t unfinished;
	/// If the path contains AT_FDCWD
	uint8_t fdcwd;
};

/// This function parses an strace line and extracts the paths in it
void paths_from_strace_line(struct path_bundle *pb, char *line)
{
	char delim_a = '<';
	char delim_b = '>';
	// We do know the length of each string
	size_t max_buff_count = sizeof(pb->ab_paths) / MAXPATHLEN;
	size_t curr_buff = 0;
	for (size_t i = 0; line[i] != '\0'; i++) {
		// '<'
		if (line[i] == delim_a) {
			size_t j = 0;
			i++;
			while (1) {
				// '>'
				if (line[i] == delim_b || line[i] == '\0') {
					pb->ab_count += 1;
					curr_buff++;
					break;
				}
				// We dont have space
				if (j >= MAXPATHLEN) {
					pb->ab_count += 1;
					curr_buff++;
					break;
				}
				pb->ab_paths[curr_buff][j] = line[i];
				j++;
				i++;
			}
		}
		if (curr_buff == max_buff_count) {
			fprintf(stderr, "No buffers left to save path...\n");
			fprintf(stderr, "Line: [%s]", line);
			break;
		}
	}
	delim_a = '\"';
	delim_b = '\"';
	curr_buff = 0;
	for (size_t i = 0; line[i] != '\0'; i++) {
		// '"'
		if (line[i] == delim_a) {
			size_t j = 0;
			i++;
			while (1) {
				// '"'
				// This is an empty path
				// NOTE: I do know delim_a and delim_b here are the same
				if (line[i] == delim_b && line[i - 1] == delim_a)
					break;
				// lookbehind to ensure theres no escape sequence
				// although, the escape sequence could be escaped? something to think about
				if ((line[i] == delim_b && line[i - 1] != '\\') || line[i] == '\0') {
					pb->quote_count += 1;
					curr_buff++;
					break;
				}
				// We dont have space
				if (j >= MAXPATHLEN) {
					pb->quote_count += 1;
					curr_buff++;
					break;
				}
				pb->quote_paths[curr_buff][j] = line[i];
				j++;
				i++;
			}
		}
		// In general only execve will have a lot of quote paths grouped with it
		if (curr_buff == max_buff_count) {
			// This is happening in a line that is not execve, so we want to be loud
			// about it
			if (strstr(line, "execve(") == NULL) {
				fprintf(stderr, "No buffers left to save path...\n");
				fprintf(stderr, "Line: %s", line);
			}
			break;
		}
	}
	// We should keep information whenever we can
	if (strstr(line, "AT_FDCWD") != NULL) {
		pb->fdcwd = 1;
	}
	if (strstr(line, "<unfinished ...>") != NULL) {
		pb->unfinished = 1;
	}
	if (strstr(line, "ENOENT") != NULL) {
		pb->fdcwd = 1;
	}
	// TODO: Pipes and new processes, this is info we need to categorize...!
	// this will take us one step closer to PLEDGE's second phase
}

/// This function inserts all the paths in a given line into a contract
/// @param r: This is the root of a contract
/// @param pb: This is the bundle containing all the paths in a line
/// @param fd_only: is basically a hint for us to know if we can just
/// grab the angle bracket pattern paths right away or if we have to do some sort of
/// filtering
void insert_paths_to_contract(struct list **r, struct path_bundle *pb, uint8_t access_fl, bool fd_only)
{
	if (pb == NULL) {
		fprintf(stderr, "Attempted to insert with an empty path bundle\n");
		return;
	}
	// We only care about file descriptor and those go inside angle bracket patterns
	// so this is very straightforward
	if (fd_only) {
		for (size_t i = 0; i < pb->ab_count; i++) {
			// Allows us to actually filter out wrong matches
			if (pb->ab_paths[i][0] != '/')
				continue;
			add_path_to_contract_list(r, pb->ab_paths[i], access_fl);
		}
		return;
	} else {
		// There are patterns in the way that the syscalls works.
		// This tells us we are at a xxxat() syscall
		if (pb->fdcwd) {
			// If the angle bracket pattern has more than 1 path,
			// then we want to insert the last one because that's the path
			// that was actually opened
			if (pb->ab_count > 1) {
				// insert last one
				size_t idx = pb->ab_count - 1;
				add_path_to_contract_list(r, pb->ab_paths[idx], access_fl);
			}
			// if both patterns only have 1 path and are of the at_fdcwd kind,
			// means that we just need to append them
			// OR it is unifnished
			else if ((pb->ab_count == 1 && pb->quote_count == 1) || pb->unfinished) {
				// append quote pattern to bracket pattern
				if (pb->ab_count < 1 || pb->quote_count < 1) {
					fprintf(stderr, "Not enough paths in path bundle to create full path\n");
					return;
				}
				size_t ab_len = strlen(pb->ab_paths[0]);
				size_t quote_len = strlen(pb->quote_paths[0]);
				if (ab_len < 1) {
					fprintf(stderr, "Path contained in angle brackets is empty.");
					return;
				}
				if (quote_len < 1) {
					fprintf(stderr, "Path contained in quote brackets is empty.");
					return;
				}
				// it is not absolute
				if (pb->quote_paths[0][0] != '/') {
					if (quote_len > 1) {
						if (pb->quote_paths[0][0] == '.' && pb->quote_paths[0][1] == '/') {
							// cat the first quote path to the fdcwd
							char full_path[MAXPATHLEN] = {0};
							strcat(full_path, pb->ab_paths[0]);
							// skip the dot
							strcat(full_path, pb->quote_paths[0] + 1);
							add_path_to_contract_list(r, full_path, access_fl);

						}
						// We only have the dot as the open, so we save the whole cwd
						else if (pb->quote_paths[0][0] == '.') {
							add_path_to_contract_list(r, pb->quote_paths[0], access_fl);
						}
						// relative paths of the form "file"
						else {
							char full_path[MAXPATHLEN] = {0};
							strcat(full_path, pb->ab_paths[0]);
							// We can do this because fd's in strace
							// never has a trailing fslash
							strcat(full_path, "/");
							strcat(full_path, pb->quote_paths[0]);
							add_path_to_contract_list(r, full_path, access_fl);
						}
					}
				}
				// The quote pattern path is absolute so we can add it right away
				else {
					add_path_to_contract_list(r, pb->quote_paths[0], access_fl);
					return;
				}
			}
		}
		// It is not a call of the form at( nor does it work with file descriptors directly
		// so we just grab the relative paths
		else {
			char path_buf[MAXPATHLEN] = {0};
			for (size_t i = 0; i < pb->quote_count; i++) {
				rel2abspath(path_buf, pb->quote_paths[i], MAXPATHLEN);
				add_path_to_contract_list(r, path_buf, access_fl);
			}
			return;
		}
	}
}

/// Used to sanitize our paths.
/// This function replaces every occurrence of char a with char b
void replace_in_str(char *src, char a, char b)
{
	size_t src_len = strlen(src);
	for (size_t i = 0; i < src_len; i++) {
		if (src[i] == a)
			src[i] = b;
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
		struct list *root = NULL;
		uint8_t temp_access_fl = 0x0;

		struct path_bundle *paths = calloc(1, sizeof(struct path_bundle));

		// SECTION: Parsing
		while (getline(&strace_line, &str_siz, strace_raw) != -1) {
			paths_from_strace_line(paths, strace_line);
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
					insert_paths_to_contract(&root, paths, temp_access_fl, false);
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
				insert_paths_to_contract(&root, paths, STAT_ACCESS, false);
			}
			// SECTION: read
			// The syscall itself tells u the operation lol
			// probably going to separate them
			else if (strstr(strace_line, "read(") != NULL) {
				insert_paths_to_contract(&root, paths, READ_ACCESS, true);
			} else if (strstr(strace_line, "write(") != NULL) {
				insert_paths_to_contract(&root, paths, WRITE_ACCESS, true);
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

				// execve is special in that we only really care about the first path
				// so we will go directly to the source here
				char cmd_abs[MAXPATHLEN] = {0};
				if (paths->quote_count > 0) {
					rel2abspath(cmd_abs, paths->quote_paths[0], MAXPATHLEN);
					add_path_to_contract_list(&root, cmd_abs, READ_ACCESS);
				}
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
					insert_paths_to_contract(&root, paths, temp_access_fl, true);
				}
				temp_access_fl = 0x0;
			}
			// Delete operation needs write access,
			// but we do not care about access rights, only what is actually done,
			// although... deleting, even if its metadata,
			// does write something to the disk kinda ish
			else if (strstr(strace_line, "unlinkat(") != NULL) {
				insert_paths_to_contract(&root, paths, DELETE_ACCESS | WRITE_ACCESS, true);
			}
			memset(paths, 0, sizeof(struct path_bundle));
		}
		free(paths);
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
