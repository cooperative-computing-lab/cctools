#ifndef PL_LIST_H
#define PL_LIST_H

#include <stdbool.h>
#include <stdint.h>

#define READ_ACCESS 0x1
#define WRITE_ACCESS 0x2
#define STAT_ACCESS 0x4
#define DELETE_ACCESS 0x8

/// Singly linked list containing our paths with their permission
/// In a text file Read and Write as true get dumped as +
struct path_list {
	/// Pathname in absolute form, ideally it should never be relative
	char *pathname;
	/// Pointer to the next member in the linked list
	struct path_list *next;
	/// Flag for read
	bool read;
	/// Flag for write
	bool write;
	/// Flag for stat
	bool stat;
};

/// New linked list of paths and their permissions
struct path_list *
new_path_node(struct path_list *c,
		char *path,
		uint8_t access_fl);

/// Frees the linked list @param r
void free_path_list(struct path_list *r);

/// Dumps the path list which contains a chain of paths (alongside their permissions).
void dump_path_list(struct path_list *r);

void generate_contract_from_list(FILE *f,
		struct path_list *r);

/// Function to find a certain path in the linked list, starting from @param r
/// until the end of the linked list.
struct path_list *
find_path(struct path_list *r,
		const char *p);

/// This function is a bit similar to find path and add but
struct path_list *
update_path_perms(struct path_list *a,
		uint8_t access_fl);

/// This function is a bit similar to find path but it always returns the root
/// But it does NEED to check that the path was not there before
void add_path_to_list(struct path_list **r,
		char *p,
		uint8_t access_fl);
#endif
