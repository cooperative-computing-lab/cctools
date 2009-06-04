/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "list.h"
#include "hash_table.h"

struct list * hash_table_to_list( struct hash_table *table )
{
	struct list *list;
	char *key;
	void *value;

	list = list_create();
	if(!list) return 0;

	hash_table_firstkey(table);
	while(hash_table_nextkey(table,&key,&value)) {
		list_push_tail(list,value);
		hash_table_remove(table,key);
	}

	return list;
}

