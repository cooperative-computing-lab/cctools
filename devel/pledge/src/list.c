#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "list.h"

/// New linked list of paths and their permissions
/// if @param c is NULL, then it just creates a new node and returns it
/// if @param c is not NULL, then it creates a new node, makes c->next point to it and
/// returns it
/// TODO: reorder those parameters lol
struct path_list *
new_path_node(struct path_list *c,
		char *path,
		uint8_t access_fl)
{
	struct path_list *t = malloc(sizeof(struct path_list));
	t->read = (access_fl & READ_ACCESS) ? true : false;
	t->write = (access_fl & WRITE_ACCESS) ? true : false;
	t->stat = (access_fl & STAT_ACCESS) ? true : false;
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
	char perms[3] = {0};
	while (c != NULL) {
		if (c->stat) {
			strcat(perms, "S");
		}
		if (c->read && c->write) {
			strcat(perms, "+");
		} else if (c->read) {
			strcat(perms, "R");
		} else if (c->write) {
			strcat(perms, "W");
		}
		fprintf(stderr, "[%s]", perms);
		fprintf(stderr, " ");
		fprintf(stderr, "Path: [%s]\n", c->pathname);
		c = c->next;
		memset(perms, 0, sizeof(perms)); // reset
						 // and reset string
	}
}

void generate_contract_from_list(FILE *f,
		struct path_list *r)
{
	struct path_list *c = r;
	char perms[3] = {0};
	fprintf(f, "%-13s %-30s\n", "action", "path");
	while (c != NULL) {

		if (c->stat) {
			strcat(perms, "S");
		}
		if (c->read && c->write) {
			strcat(perms, "+");
		} else if (c->read) {
			strcat(perms, "R");
		} else if (c->write) {
			strcat(perms, "W");
		}

		fprintf(f, "%-13s %-30s\n", perms, c->pathname);
		c = c->next;
		memset(perms, 0, sizeof(perms)); // reset
	}
	fflush(f);
}

/// Function to find a certain path in the linked list, starting from @param r
/// until the end of the linked list.
struct path_list *
find_path(struct path_list *r,
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

struct path_list *
update_path_perms(struct path_list *a,
		uint8_t access_fl)
{
	if (a == NULL) {
		// they gave us an empty node...
		return NULL;
	}

	// We want this to only be positive, because if its false, we dont want to
	// changeone that was true to false
	if (access_fl & READ_ACCESS) {
		a->read = true;
	}
	if (access_fl & WRITE_ACCESS) {
		a->write = true;
	}
	if (access_fl & STAT_ACCESS) {
		a->stat = true;
	}
	return a;
}

/// This function is a bit similar to find path but it always returns the root
/// But it does NEED to check that the path was not there before
void add_path_to_list(struct path_list **r,
		char *p,
		uint8_t access_fl)
{
	if (*r == NULL) {
		*r = new_path_node(NULL, p, access_fl);
		return;
	}

	struct path_list *c = *r;
	char perm;

	while (c != NULL) {
		if (strcmp(c->pathname, p) == 0) {
			update_path_perms(c, access_fl);
			break;
		}
		if (c->next == NULL) {
			struct path_list *t = new_path_node(c, p, access_fl);
			c = t;
		}
		c = c->next;
	}
}
