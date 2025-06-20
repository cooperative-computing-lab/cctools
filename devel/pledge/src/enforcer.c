#include <dlfcn.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/types.h>
#include <unistd.h>

#define PINK "\033[38;5;198m"
#define PINKER "\033[38;5;217m"
#define GREEN "\033[38;5;113m"
#define YELLOW "\033[38;5;221m"
#define RESET_TERM_C "\033[0m"

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

/// These paths, and everything under them will be whitelisted
const char *WHITELIST[] = {"/dev/pts", "/dev/null", "/dev/tty", "/proc/self", "/proc", "pipe", NULL};

// TODO: Error handling

/// Turn a relative path into an absolute path
/// @param abs_p is the buffer where we will store the absolute path
/// @param rel_p is the user given string
/// @param size is the size/len of abs_p
/// Return If the path does not need to be turned into an absolute path then we just
/// copy to the abs_p buffer
/// TODO: Perhaps we do strlcpy/strlcat?
char *
rel2abspath(char *abs_p,
		char *rel_p,
		size_t size)
{
	if (rel_p == NULL) {
		fprintf(stderr, "Attempted to turn an empty string into an absolute path.\n");
		abs_p = NULL;
		return NULL;
	}
	// Strnlen??
	size_t rel_p_len = strlen(rel_p);
	if (rel_p_len >= 1) {
		if (rel_p[0] != '/') {
			if (rel_p_len >= 2) {
				if (rel_p[0] == '.' && rel_p[1] == '/') {
					rel_p = rel_p + 2;
				}
			}
			// get cwd
			if (getcwd(abs_p, size) == NULL) {
				fprintf(stderr, "Attempt to obtain cwd failed.\n");
				return rel_p;
			}
			size_t abs_p_len = strlen(abs_p);
			if (abs_p[abs_p_len - 1] != '/') {
				strncat(abs_p, "/", MAXPATHLEN);
			}
			strncat(abs_p, rel_p, MAXPATHLEN);
			return abs_p;
		}
		strncpy(abs_p, rel_p, MAXPATHLEN);
		return abs_p;
	}
	strncpy(abs_p, rel_p, MAXPATHLEN);
	return abs_p;
}

/// Singly linked list containing our paths with their permission
struct path_list {
	/// Read  -> R
	/// Write -> W
	/// RW    -> +
	char permission;
	/// Pathname in absolute form, ideally it should never be relative
	char *pathname;
	/// Pointer to the next member in the linked list
	struct path_list *next;
};

/// New linked list of paths and their permissions
struct path_list *
new_path_list(struct path_list *c,
		char perm,
		char *path)
{
	struct path_list *t = malloc(sizeof(struct path_list));
	t->permission = perm;
	/// This string gotta be manually removed
	t->pathname = strdup(path);
	t->next = NULL;
	if (c != NULL) {
		c->next = t;
	}
	return t;
}

/// Frees the linked list @param r
void free_path_list(struct path_list *r)
{
	struct path_list *c = r;
	while (c != NULL) {
		struct path_list *x = c->next;
		/// Free string first
		free(c->pathname);
		/// Free the path_list struct
		free(c);
		c = x;
	}
}
/// Dumps the path list which contains a chain of paths (alongside their permissions).
void dump_path_list(struct path_list *r)
{
	struct path_list *c = r;
	while (c != NULL) {
		fprintf(stderr, "[%c]", c->permission);
		fprintf(stderr, " ");
		fprintf(stderr, "Path: [%s]\n", c->pathname);
		c = c->next;
	}
}

/// Function to find a certain path in the linked list, starting from @param r
/// until the end of the linked list.
struct path_list *
find_path(const struct path_list *r,
		const char *p)
{
	if (p == NULL) {
		// Empty path given
		return NULL;
	}
	// current
	struct path_list *c = r;
	while (c != NULL) {
		if (strcmp(c->pathname, p) == 0) {
			return c;
		}
		c = c->next;
	}
	return NULL;
}

/// Our linked list containing the paths to enforce, the reason it's a global is pretty
/// obvious, this being an .so we LD_PRELOAD, so the only way to share things accross
/// independent functions is with globals, this is in theory only ever used as read only
/// after initialization in @ref init_enforce()
/// THINK: thread locality and also hwo to not reinitialize after fork
/// TODO: Read up on __attribute__((constructor)) in the context of forks and clones
struct path_list *ROOT = NULL;

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
			fprintf(stderr, PINK);
			fprintf(stderr,
					"NO CONTRACT: "
					"Couldn't open the contract file\n");
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
	// The value of the permission extracted
	char perm_val;
	// Position storing to make lookahead easy
	fpos_t pos;
	// index for our array that will store the path
	size_t i = 0;
	// NOTE: A common pattern right now is using MAXPATHLEN arrays for paths and
	// not thinking too much about optimizing the size, more convenient
	char path_val[MAXPATHLEN] = {0};
	// Self explanatory
	struct path_list *root = NULL;
	struct path_list *current = NULL;
	// Start parsing
	while ((c = fgetc(contract_f)) != EOF) {
		// We ignoe characters that are not useful to us
		// and despite contracts being line orientes, we dont actually track lines
		if (c == ' ' || c == '\n')
			continue;
		// Save position for lookahead
		fgetpos(contract_f, &pos);
		// get our char
		l = fgetc(contract_f);
		if (!got_perm) {
			if (c == 'R') {
				// Check if its RW permissions
				if (l == 'W') {
					perm_val = '+';
					got_perm = true;
					continue;
				}
				perm_val = 'R';
				got_perm = true;
			} else if (c == 'W') {
				perm_val = 'W';
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
				current = new_path_list(current, perm_val, path_val);
				if (root == NULL) {
					root = current;
				}
				got_perm = false;
				continue;
			}
		}
		// Reset to position before lookahead
		fsetpos(contract_f, &pos);
	}
	// Our global READ-ONLY (in theory lmao) variable with our values
	ROOT = root;
	fclose(contract_f);
}
/// Bye bye enforcer
__attribute__((destructor)) void
deinit_enforce()
{
	// Bye
	free_path_list(ROOT);
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
					"WHITELISTED: "
					"Path [%s] is whitelisted internally.\n",
					pathname);
			return true;
		}
	}

	const struct path_list *a = find_path(ROOT, pathname);
	// Is this good enough error handling? Probably not.
	if (a == NULL) {
		fprintf(stderr, PINKER);
		fprintf(stderr,
				"NOT FOUND: "
				"Path [%s] is not part of the contract...\n",
				pathname);
		fprintf(stderr, RESET_TERM_C);
		// we need to do some sort of blockage but perhaps that's got to be handled
		// locally
		return false;
	} else if (a->permission == '+' && (perm == 'R' || perm == 'W')) {
		fprintf(stderr, GREEN);
		fprintf(stderr,
				"ALLOWED: "
				"Path [%s] with permission [%c] is not in violation of the "
				"contract.\n",
				a->pathname,
				a->permission);
		fprintf(stderr, RESET_TERM_C);
		return true;
	} else if (a->permission == perm) {
		fprintf(stderr, GREEN);
		fprintf(stderr,
				"ALLOWED: "
				"Path [%s] with permission [%c] is not in violation of the "
				"contract.\n",
				a->pathname,
				a->permission);
		fprintf(stderr, RESET_TERM_C);
		return true;
	} else if (a->permission != perm) {
		fprintf(stderr, PINK);
		fprintf(stderr,
				"BLOCKED: "
				"Permission [%c] for path [%s] does not match contract, expected [%c]\n",
				perm,
				a->pathname,
				a->permission);
		fprintf(stderr, RESET_TERM_C);
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
	/* if (pathname != NULL) */
	/* { */
	/* } */
	char full_path[MAXPATHLEN];
	if (rel2abspath(full_path, pathname, MAXPATHLEN) == NULL) {
		// Couldn't convert so we fallback to pathname
		strncpy(full_path, pathname, MAXPATHLEN);
	}

	fprintf(stderr, YELLOW);
	fprintf(stderr, "OPEN: caught open with path [%s]\n", pathname);
	fprintf(stderr, "with absolute [%s]\n", full_path);
	fprintf(stderr, RESET_TERM_C);

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

	fprintf(stderr, YELLOW);
	fprintf(stderr, "READING: caught path [%s] with link to [%s]\n", fd_link, solved_path);
	fprintf(stderr, RESET_TERM_C);

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

	fprintf(stderr, YELLOW);
	fprintf(stderr, "WRITING: caught path [%s] with link to [%s]\n", fd_link, solved_path);
	fprintf(stderr, RESET_TERM_C);
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
	fprintf(stderr, YELLOW);
	fprintf(stderr, "FOPEN: Caught path [%s] with mode [%s]\n", pathname, mode);
	fprintf(stderr, RESET_TERM_C);

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

// TODO: execve?
// I don't think this is as straightforward, since strace uses ptrace
// here we injecting into the process itself
