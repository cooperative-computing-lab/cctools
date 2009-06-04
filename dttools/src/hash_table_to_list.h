/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#ifndef HASH_TABLE_TO_LIST_H
#define HASH_TABLE_TO_LIST_H

#include "list.h"
#include "hash_table.h"

struct list * hash_table_to_list( struct hash_table *h );

#endif
