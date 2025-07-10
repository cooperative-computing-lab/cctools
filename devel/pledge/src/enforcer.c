#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <unistd.h>

#include "list_util.h"
#include "util.h"

#ifdef COLOR_ENFORCING
#define PINK "\033[38;5;198m"
#define PINKER "\033[38;5;217m"
#define GREEN "\033[38;5;113m"
#define YELLOW "\033[38;5;221m"
#define RESET_TERM_C "\033[0m"
#define PRINT_PINK() fprintf(stderr, PINK);
#define PRINT_PINKER() fprintf(stderr, PINKER)
#define PRINT_GREEN() fprintf(stderr, GREEN)
#define PRINT_YELLOW() fprintf(stderr, YELLOW)
#define PRINT_RESET_TERM_C() fprintf(stderr, RESET_TERM_C)
#else
#define PRINT_PINK()
#define PRINT_PINKER()
#define PRINT_GREEN()
#define PRINT_YELLOW()
#define PRINT_RESET_TERM_C()
#endif

// TODO: the enforcer should not stop wrong paths...?

// TODO: Enforcer should not write to stderr no more

// --------------------------------------------------------------------------------------
// Something interesting to note that hecil has brought to my attention is that
// Makeflow attempts to open the x.y.makeflowlog file on read but since it does not
// find it because it's a clean run, it opens it on write mode but our strace parser
// only catches paths of successful calls...
// So the RDONLY access never gets registered.
// 2 solutions to this could possibly be:
// 1. We modify the tracer so it catches failed calls and adds them to the contract
// anyways
// 2. We modify the tracer so on subsequent runs it parses the file and
// updates the permissions accordingly, but in a append type of way.
// I think option Number 1 is easier but somewhat half-assed
// But Number 2 implies writing the same parser in 2 different languages
// Perhaps this is a signal to wrap everything under 1 language...?
// --------------------------------------------------------------------------------------

char flag2letter(struct path_access *r)
{
	if (r->read && r->write)
		return '+';
	if (r->read)
		return 'R';
	if (r->write)
		return 'W';
	if (r->stat)
		return 'S';
	return '_';
}

/// These paths, and everything under them will be whitelisted
const char *WHITELIST[] = {"/dev/pts", "/dev/null", "/dev/tty", "/proc/self", "/proc", "pipe", NULL};

// TODO: Error handling

/// Our linked list containing the paths to enforce, the reason it's a global is pretty
/// obvious, this being an .so we LD_PRELOAD, so the only way to share things accross
/// independent functions is with globals, this is in theory only ever used as read only
/// after initialization in @ref init_enforce()
/// THINK: thread locality and also hwo to not reinitialize after fork
/// TODO: Read up on __attribute__((constructor)) in the context of forks and clones
struct list *contract_list_root = NULL;

/// This function obtains the contract path,
/// fopen's it, but using the real_fopen function because
/// otherwise it'd call the fopen in here and that one depends on the contract to be
/// initialized for it to work.
/// It parses the contract file we fopen'd and builds the linked list
/// and assigns it to root.
/// __attribute__((constructor)) is key here.
/// For now the way we obtain the contract path is with an environment variable
/// "CONTRACT"
__attribute__((constructor)) void
init_enforce()
{
	FILE *contract_f;
	/// The contract path is given with an environment variable
	char *contract_env = getenv("CONTRACT");
	/// Get our libc fopen
	FILE *(*real_fopen)(const char *restrict pathname, const char *restrict mode);
	real_fopen = dlsym(RTLD_NEXT, "fopen");
	// Empty environment variable
	if (contract_env == NULL) {
		fprintf(stderr, "No contract available.\n");
		fprintf(stderr,
				"Please set the environment variable $CONTRACT with the absolute path "
				"to the contract.");
		exit(EXIT_FAILURE);
	} else if (strlen(contract_env) == 0) {
		fprintf(stderr, "No contract available.\n");
		fprintf(stderr,
				"Please set the environment variable $CONTRACT with the absolute path "
				"to the contract.\n");
		exit(EXIT_FAILURE);
	}

	if (contract_env != NULL) {
		char abs_p[MAXPATHLEN] = {0};
		// allowed becasue contract_env already decayed to a pointer
		contract_env = rel2abspath(abs_p, contract_env, MAXPATHLEN);
		fprintf(stderr, "Enforcer path: %s\n", contract_env);
		contract_f = real_fopen(contract_env, "r");
		if (contract_f == NULL) {
			PRINT_PINK();
			fprintf(stderr,
					"NO CONTRACT: "
					"Couldn't open the contract file\n");
			PRINT_RESET_TERM_C();
			exit(EXIT_FAILURE);
		}
	}
	// Our char
	int c;
	// lookahead char
	int l;
	// We want to skip the first line
	// Contract files are line oriented and the first line is always the title of the
	// 'columns'
	while ((c = fgetc(contract_f)) != '\n')
		;
	// Variable to check that we extracted the permission from the line
	bool got_perm = false;
	// flag that will contain all of our flags
	uint8_t access_fl;
	// Position storing to make lookahead easy
	fpos_t pos;
	// index for our array that will store the path
	size_t i = 0;
	// NOTE: A common pattern right now is using MAXPATHLEN arrays for paths and
	// not thinking too much about optimizing the size, more convenient
	char path_val[MAXPATHLEN] = {0};
	struct list *list_root_init = list_create();
	// Start parsing
	while ((c = fgetc(contract_f)) != EOF) {
		// We ignoe characters that are not useful to us
		// and despite contracts being line orientes, we dont actually track lines
		if (c == ' ' || c == '\t' || c == '\n')
			continue;
		// Save position for lookahead
		fgetpos(contract_f, &pos);
		// get our char
		l = fgetc(contract_f);
		if (!got_perm) {
			if (c == 'S') {
				switch (l) {
				case 'R':
					access_fl |= READ_ACCESS;
					access_fl |= STAT_ACCESS;
					got_perm = true;
					continue;
				case 'W':
					access_fl |= WRITE_ACCESS;
					access_fl |= STAT_ACCESS;
					got_perm = true;
					continue;
				case '+':
					access_fl |= READ_ACCESS;
					access_fl |= WRITE_ACCESS;
					access_fl |= STAT_ACCESS;
					got_perm = true;
					continue;
				}
				access_fl |= STAT_ACCESS;
				got_perm = true;
			} else if (c == 'R') {
				access_fl |= READ_ACCESS;
				got_perm = true;
			} else if (c == 'W') {
				access_fl |= WRITE_ACCESS;
				got_perm = true;
			} else if (c == '+') {
				access_fl |= READ_ACCESS;
				access_fl |= WRITE_ACCESS;
				got_perm = true;
			} else {
				fprintf(stderr, "Unrecognized permission [%x][%c]...\n", c, c);
			}
		} else {
			// Since we already found our permission we need to store the path
			path_val[i] = c;
			i++;
			// Ok this could be a potential issue since sometimes path might contain
			// spaces... perhaps solution could be
			// if (l == '\n' || (l == ' ' && c != '\\'))
			// so we can check if the space character is escaped
			if (l == '\n' || l == ' ') {
				path_val[i] = '\0';
				i = 0;
				new_path_access_node(list_root_init, path_val, access_fl);
				access_fl = 0x0;
				got_perm = false;
				continue;
			}
		}
		// Reset to position before lookahead
		fsetpos(contract_f, &pos);
	}
	// The root of our global contract list,
	// This is never really modified throughout the program, since enforcing
	// only really validates things.
	// NOTE: It could, in the future,
	contract_list_root = list_root_init;
	fclose(contract_f);
}
/// Bye bye enforcer
__attribute__((destructor)) void
deinit_enforce()
{
	// Bye
	destroy_contract_list(contract_list_root);
	list_delete(contract_list_root);
}

/// This is where the actual enforcing is done.
/// @param pathname: This is the pathname to check for enforcing
/// @param perm: This is the permission that this path should have
bool enforce(const char *pathname,
		const char perm)
{
	// WHITELIST check
	for (size_t i = 0; WHITELIST[i] != NULL; i++) {
		// The whitelist is for the subdirectories
		size_t wl_member_len = strlen(WHITELIST[i]);
		if (strncmp(WHITELIST[i], pathname, wl_member_len) == 0) {
			fprintf(stderr,
					"[WHITELISTED]: "
					"Path [%s] is whitelisted internally.\n",
					pathname);
			return true;
		}
	}

	struct path_access *a = find_path_in_list(contract_list_root, pathname);
	// Is this good enough error handling? Probably not.
	// Mostly saying this due to the fact that a path, that's not registered,
	// was attempted to be used and this should tell us something about the workflow,
	// however, labelling that is where it gets complicated.
	// So it's not really about the error itself but about what it might imply
	// and this is somewhat related to my comment about allowing a 'NOT FOUND' call
	// to return true
	if (a == NULL) {
		PRINT_PINKER();
		fprintf(stderr,
				"[NOT FOUND]: "
				"Path [%s] is not part of the contract...\n",
				pathname);
		PRINT_RESET_TERM_C();
		// For now we should still allow the program to run even if something is not found
		// the idea, for now, is not to stop a workflow but to monitor it (in a sense)
		return true;
	} else if ((a->read && a->write) && (perm == 'R' || perm == 'W')) {
		char a_perm = flag2letter(a);
		PRINT_GREEN();
		fprintf(stderr,
				"[ALLOWED]: "
				"Path [%s] with permission [%c] is not in violation of the "
				"contract.\n",
				a->pathname,
				a_perm);
		PRINT_RESET_TERM_C();
		return true;
	} else if ((a->read && perm == 'R') || (a->write && perm == 'W')) {
		char a_perm = flag2letter(a);
		PRINT_GREEN();
		fprintf(stderr,
				"[ALLOWED]: "
				"Path [%s] with permission [%c] is not in violation of the "
				"contract.\n",
				a->pathname,
				a_perm);
		PRINT_RESET_TERM_C();
		return true;
	} else if (a->stat && perm == 'S') {
		char a_perm = flag2letter(a);
		PRINT_GREEN();
		fprintf(stderr,
				"[ALLOWED STAT]: "
				"Path [%s] with permission [%c] is not in violation of the "
				"contract.\n",
				a->pathname,
				a_perm);
		PRINT_RESET_TERM_C();
		return true;
	} else {
		char a_perm = flag2letter(a);
		PRINT_PINK();
		fprintf(stderr,
				"[BLOCKED]: "
				"Permission [%c] for path [%s] does not match contract, expected [%c]\n",
				perm,
				a->pathname,
				a_perm);
		PRINT_RESET_TERM_C();
		return false;
	}
	return false;
}

/// Open wrapper
int open(const char *pathname,
		int flags,
		... /* mode_t      mode*/)
{
	// NOTE: Since contract paths may be absolute but a function like open may
	// be given a relative path AND the syscall will deal with the path format
	// we need to pass the function the original `pathname` without changes
	// but for our contracts we need to check the pathname is absolute

	// XXX: Is it our responsiblity to ensure it's not empty? probably not!
	// The reason is that we are not here to ensure it's not empty,
	// or if the usage of open is correct, its to just verify against contracts
	// HOWEVER, this is a role that we COULD assume in the future.
	char full_path[MAXPATHLEN];
	if (rel2abspath(full_path, pathname, MAXPATHLEN) == NULL) {
		// Couldn't convert so we fallback to pathname
		strncpy(full_path, pathname, MAXPATHLEN);
	}

	PRINT_YELLOW();
	fprintf(stderr, "[OPEN]: caught open with path [%s]\n", pathname);
	fprintf(stderr, "with absolute [%s]\n", full_path);
	PRINT_RESET_TERM_C();

	mode_t mode = 0;
	/// If there is a flag in there, we need to extract it
	if (flags & (O_CREAT)) {
		va_list mode_va;
		va_start(mode_va, flags);
		mode = va_arg(mode_va, mode_t);
	}

	// SECTION: Enforce
	// Get which permission we need
	char path_perm;
	if ((flags & O_RDONLY) == O_RDONLY) // O_RDONLY flag is 0 under the hood
	{
		path_perm = 'R';
	} else if (flags & O_WRONLY) {
		path_perm = 'W';
	} else if (flags & O_RDWR) {
		path_perm = '+';
	}
	if (enforce(full_path, path_perm) != true) {
		return -1;
	}

	// The general pattern for this is just making a function pointer
	int (*real_open)(const char *pathname, int flags, ...);
	// and retrieving the original with dlsym
	// TIP: do RTLD_DEFAULT if u wanna see something fun
	real_open = dlsym(RTLD_NEXT, "open");
	return real_open(pathname, flags, mode);
}
/// Read wrapper
ssize_t
read(int fd,
		void *buf,
		size_t count)
{
	char fd_link[BUFSIZ];
	// I don't like this function...
	snprintf(fd_link, BUFSIZ, "/proc/self/fd/%d", fd);
	char solved_path[BUFSIZ];
	size_t solved_path_len = readlink(fd_link, solved_path, BUFSIZ);
	// readlink does not place the '\0' at the end
	solved_path[solved_path_len] = '\0';

	PRINT_YELLOW();
	fprintf(stderr, "[READ]: caught path [%s] with link to [%s]\n", fd_link, solved_path);
	PRINT_RESET_TERM_C();

	// SECTION: Enforce
	if (enforce(solved_path, 'R') != true) {
		return -1;
	}

	ssize_t (*real_read)(int fd, void *buf, size_t count);
	real_read = dlsym(RTLD_NEXT, "read");
	return real_read(fd, buf, count);
}

// One day I will find out why echo is not calling this
/// Write wrapper
ssize_t
write(int fd,
		const void *buf,
		size_t count)
{
	char fd_link[BUFSIZ];
	snprintf(fd_link, BUFSIZ, "/proc/self/fd/%d", fd);
	char solved_path[BUFSIZ];
	size_t solved_path_len = readlink(fd_link, solved_path, BUFSIZ);
	solved_path[solved_path_len] = '\0';

	PRINT_YELLOW();
	fprintf(stderr, "[WRITING]: caught path [%s] with link to [%s]\n", fd_link, solved_path);
	PRINT_RESET_TERM_C();
	// SECTION: Enforce
	if (enforce(solved_path, 'W') != true) {
		return -1;
	}

	ssize_t (*real_write)(int fd, const void *buf, size_t count);
	real_write = dlsym(RTLD_NEXT, "write");
	return real_write(fd, buf, count);
}

// It is true that fopen might internally call open but it might do so
// without using the libc wrapper and calling assembly directly
// so there is some value to wrapping fopen
FILE *
fopen(const char *restrict pathname,
		const char *restrict mode)
{
	PRINT_YELLOW();
	fprintf(stderr, "[FOPEN]: Caught path [%s] with mode [%s]\n", pathname, mode);
	PRINT_RESET_TERM_C();

	// SECTION: Enforcing
	char perm_val;
	char mode_len = strlen(mode);
	// We need to get the values out of the flags
	if (strcmp(mode, "r") == 0) {
		perm_val = 'R';
	} else if (strcmp(mode, "w") == 0) {
		perm_val = 'W';
	} else if (strcmp(mode, "a") == 0) {
		perm_val = 'W';
	} else if (mode_len > 1) {
		if (mode[1] == '+') {
			perm_val = '+';
		} else if (mode[1] == 'b') {
			// not a fan of this
			switch (mode[0]) {
			case 'r':
				perm_val = 'R';
				break;
			case 'w':
				perm_val = 'W';
				break;
			}
		}
	} else {
		fprintf(stderr, "FOPEN: Unkown permission [%s]\n", mode);
	}

	char full_path[MAXPATHLEN];
	if (rel2abspath(full_path, pathname, MAXPATHLEN) == NULL) {
		// Couldn't convert so we fallback to pathname
		strncpy(full_path, pathname, MAXPATHLEN);
	}
	enforce(full_path, perm_val);

	FILE *(*real_fopen)(const char *restrict pathname, const char *restrict mode);
	real_fopen = dlsym(RTLD_NEXT, "fopen");

	return real_fopen(pathname, mode);
}

int fstatat(int dirfd,
		const char *restrict pathname,
		struct stat *restrict statbuf,
		int flags)
{

	PRINT_YELLOW();
	fprintf(stderr, "[STAT]: Caught path [%s]\n", pathname);
	PRINT_RESET_TERM_C();

	// TODO: so the manpage for fstatat says
	// if the path is relative and dirfd is AT_FDCWD
	// if the path is relative and dirfd is actually set then its relative to that
	// if its absolute then dirfd is ignored
	// which I think is the same as openat?
	size_t path_len = strlen(pathname);
	if (path_len < 1) {
		// TODO: Better error handling
		return -1;
	}

	// 1. If second path is relative then we make it absolute basing ourselves
	// off of the path in the dirfd
	// 2. If the path is absolute, we just use that path to verify
	// In theory this is fine because rel2abspath leaves absolute paths as is
	char full_path[MAXPATHLEN];
	if (rel2abspath(full_path, pathname, MAXPATHLEN) == NULL) {
		// Couldn't convert so we fallback to pathname
		strncpy(full_path, pathname, MAXPATHLEN);
	}
	// TODO: This could be made to only have 1 call to enforce instead of 2 in separate
	// branches
	if (dirfd == AT_FDCWD) {
		enforce(full_path, 'S');
	} else {
		// Solve the dirfd and then glue it together with the absolute path
		char fd_link[MAXPATHLEN];
		snprintf(fd_link, MAXPATHLEN, "/proc/self/fd/%d", dirfd);
		char solved_path[MAXPATHLEN];
		size_t solved_path_len = readlink(fd_link, solved_path, MAXPATHLEN);
		solved_path[solved_path_len] = '\0';
		strcat(solved_path, pathname); // XXX: strncat()?
		enforce(solved_path, 'S');
	}

	int (*real_fstatat)(int dirfd, const char *restrict pathname, struct stat *restrict statbuf, int flags);
	real_fstatat = dlsym(RTLD_NEXT, "fstatat");

	return real_fstatat(dirfd, pathname, statbuf, flags);
}

// TODO: execve?
// I don't think this is as straightforward, since strace uses ptrace
// here we injecting into the process itself

// TODO: openat()
