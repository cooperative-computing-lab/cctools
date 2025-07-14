#ifndef PL_LIST_UTIL
#define PL_LIST_UTIL
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "list.h"

#define READ_ACCESS 0x1
#define WRITE_ACCESS 0x2
#define STAT_ACCESS 0x4
#define CREATE_ACCESS 0x8
#define DELETE_ACCESS 0x16
#define LIST_ACCESS 0x32

/// Singly linked list containing our paths with their permission
/// In a text file Read and Write as true get dumped as +
/// TODO: Change name
struct path_access {
	/// Pathname in absolute form, ideally it should never be relative
	char *pathname;
	/// Flag for read
	bool read;
	/// Flag for write
	bool write;
	/// Flag for stat
	bool stat;
};

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
