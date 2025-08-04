#ifndef PL_LIST_UTIL
#define PL_LIST_UTIL
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/param.h>

#include "list.h"

#define ACCESS_COUNT 7

#define UNKOWN_ACCESS 0
#define READ_ACCESS 1
#define WRITE_ACCESS 2
#define METADATA_ACCESS 4
#define CREATE_ACCESS 8
#define DELETE_ACCESS 16
#define LIST_ACCESS 32
#define ERROR_ACCESS 64

// In a text file Read and Write as true get dumped as +
// THOUGHT: If we were to add a metadata flag, would open be a metadata flag
// or something else? Like you can make the argument that open is a metadata operation
// (which I already have done plenty of times) but,
// in practice, it still asks for some sort of access,
// so it's not like we cant read or write, and limit it to metadata operations
// because otherwise we might not be able to do much
// Now the real question is, do we LABEL an open call as whatever flag it carries
// (aside from creat obviously)
/// Struct containing our paths with their keys
/// TODO: Change name
struct path_access {
	/// How many times the file has been accessed
	uint32_t count;
	/// XXX: For statistics and summarization, if there was an ENOENT on a certain path
	/// it should be saved
	/// Pathname in absolute form (ideally) it should never be relative
	char *pathname;
	/// Flag for read
	bool read;
	/// Flag for write
	bool write;
	/// Flag for metadata
	bool metadata;
	/// Flag for file creation
	bool create;
	/// Flag for file deletion
	bool delete;
	/// Flag for retrieving directory entities
	bool list;
	/// Flag for an error when accessing the path
	bool error;
};

char *rel2abspath(char *abs_p, char *rel_p, size_t size);

/// Add a path_access node to our cctools list
void new_path_access_node(struct list *c,
		char *path,
		uint8_t access_fl);

void destroy_path_node(void *x);
void destroy_contract_list(struct list *c);
struct path_access *
find_path_in_list(struct list *c,
		char *path);

/// Update the path access permissions for the path passed to the function
struct path_access *
update_path_perms(struct path_access *a,
		uint8_t access_fl);

/// This function grabs a path and its access flags and adds it to a cctools list
/// structure built at runtime creates, if theres no list it creates one If the path
/// already exists, it updates the permissions
void add_path_to_contract_list(struct list **r,
		char *path,
		uint8_t access_fl);

/// Dumps our contract into the contract file
void generate_contract_from_list(FILE *f, struct list *r);
#endif
