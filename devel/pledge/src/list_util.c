#include "list_util.h"

/// Turn a relative path into an absolute path, based on CWD
/// @param abs_p is the buffer where we will store the absolute path
/// @param rel_p is the user given string
/// @param size is the size/len of abs_p
/// Return If the path does not need to be turned into an absolute path then we just
/// copy to the abs_p buffer and return that
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
	// current directory paths need to get expanded
	// TODO: Integrate with dttools/path functions,
	// no reason we can't have both
	if (rel_p_len == 1) {
		if (rel_p[0] == '.') {
			realpath(rel_p, abs_p);
			return abs_p;
		}
	}
	if (rel_p_len >= 1) {
		if (rel_p[0] != '/') {
			// Check if its relative of the form ./
			if (rel_p[0] == '.' && rel_p[1] == '/') {
				rel_p = rel_p + 2;
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

/// Add a path_access node to our cctools list
/// Perhaps this should return the temprary path_access var
void new_path_access_node(struct list *c,
		char *path,
		uint8_t access_fl)
{
	struct path_access *t = malloc(sizeof(struct path_access));
	t->read = (access_fl & READ_ACCESS) ? true : false;
	t->write = (access_fl & WRITE_ACCESS) ? true : false;
	t->metadata = (access_fl & METADATA_ACCESS) ? true : false;
	t->create = (access_fl & CREATE_ACCESS) ? true : false;
	t->delete = (access_fl & DELETE_ACCESS) ? true : false;
	t->list = (access_fl & LIST_ACCESS) ? true : false;
	t->error = (access_fl & ERROR_ACCESS) ? true : false;
	t->count = 1;

	/// This string gotta be manually removed
	t->pathname = strdup(path);

	/// XXX: Maybe remove this from here?
	list_push_tail(c, t);
}

/// This is the function we pass list_clear to cleanup our cctools_list
void destroy_path_node(void *x)
{
	struct path_access *a = (struct path_access *)x;
	free(a->pathname);
	free(a);
}

/// Function to call at the __destructor
void destroy_contract_list(struct list *c)
{
	list_clear(c, destroy_path_node);
}

/// Search our cctools_list for the pathname
struct path_access *
find_path_in_list(struct list *c,
		char *path)
{
	// FIXME: /etc/gnutls/config seems to be loading before the tree is built so we cant
	// even accept it, even though it is in the contract
	if (c == NULL) {
		// possible solution could be to build the tree ourselves in here
		// as a last recourse
		fprintf(stderr, "Unable to find path in list due to null contract list root\n");
		return NULL;
	}
	list_first_item(c);
	void *x;
	while ((x = list_next_item(c))) {
		struct path_access *a = x;
		// XXX: Something of value here might be to check the tail before looping
		if (strcmp(a->pathname, path) == 0) {
			return a;
		}
	}
	return NULL;
}

struct path_access *
update_path_perms(struct path_access *a,
		uint8_t access_fl)
{
	if (a == NULL) {
		// they gave us an empty node...
		return NULL;
	}

	// We want this to only be positive, because if its false, we dont want to
	// change one that was true to false
	if (access_fl & READ_ACCESS) {
		a->read = true;
	}
	if (access_fl & WRITE_ACCESS) {
		a->write = true;
	}
	if (access_fl & METADATA_ACCESS) {
		a->metadata = true;
	}
	if (access_fl & CREATE_ACCESS) {
		a->create = true;
	}
	if (access_fl & DELETE_ACCESS) {
		a->delete = true;
	}
	if (access_fl & LIST_ACCESS) {
		a->list = true;
	}
	if (access_fl & ERROR_ACCESS) {
		a->list = true;
	}
	a->count += 1;
	// USELESS?: Maybe remove this lol
	return a;
}
/// This function grabs a path and its access flags and adds it to a cctools list
/// structure built at runtime creates, if theres no list it creates one If the path
/// already exists, it updates the permissions
void add_path_to_contract_list(struct list **r,
		char *path,
		uint8_t access_fl)
{
	if (*r == NULL) {
		*r = list_create();
		new_path_access_node(*r, path, access_fl);
		return;
	}
	struct list *c = *r;
	struct path_access *a = find_path_in_list(c, path);
	if (a == NULL) {
		new_path_access_node(c, path, access_fl);
	} else
		update_path_perms(a, access_fl);
}

/// Dumps our contract into the contract file
/// if @param f is NULL, then the contract gets dumped to stderr
void generate_contract_from_list(FILE *f, struct list *r)
{
	list_first_item(r);
	struct path_access *a;
	char perms[16] = {0};
	FILE *o = f;
	if (f == NULL)
		o = stderr;
	char *col1_title = "Access";
	char *col2_title = "<Path>";
	char *col3_title = "Count";
	fprintf(o, "%-12s %-14s %s\n", col1_title, col2_title, col3_title);
	while ((a = list_next_item(r))) {
		// THINK: This might need to become a function
		if (a->metadata)
			strcat(perms, "M");
		if (a->create)
			strcat(perms, "C");
		if (a->delete)
			strcat(perms, "D");
		if (a->read)
			strcat(perms, "R");
		if (a->write)
			strcat(perms, "W");
		if (a->list)
			strcat(perms, "L");

		if (a->error)
			strcat(perms, "E");

		fprintf(o, "%-12s <%s> %d\n", perms, a->pathname, a->count);

		memset(perms, 0, sizeof(perms)); // reset
	}
	fflush(f);
}
