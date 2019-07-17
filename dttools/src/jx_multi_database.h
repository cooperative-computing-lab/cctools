/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_MULTI_DATABASE_H
#define JX_MULTI_DATABASE_H

/** @file jx_multi_database.h

A jx_multi_database manages a large body of JX records.
This API allows the caller to treat a collection of databases
as a single database, using the same insert, lookup, and iterate
API as the plain jx_database.  The difference is that, internally,
the data is partitioned by the "type" field of the record,
so that each type will have its own directory structure on disk.
*/

#include "jx_database.h"

/** Create a new database, recovering state from disk if available.
@param path A directory to contain the database on disk.  If it does not exist, it will be created.  If null, no disk storage will be used.
@return A pointer to a newly created multi-database.
*/

struct jx_multi_database * jx_multi_database_create( const char *path );

/** Delete a multi database from memory, retaining state on disk.
@param db The database to close
*/

void jx_multi_database_delete( struct jx_multi_database *mdb );


/** Insert or update an object into the database.
@param db The database to access.
@param key The primary key to associate with the object.
@param j The object in the form of an jx expression.
@return True on success, false on failure.  (Could fail if unable to create a database for a new type.)
*/

int jx_multi_database_insert( struct jx_multi_database *mdb, const char *key, struct jx *j );

/** Look up an object in the database.
@param db The database to access.
@param key The primary key of the desired object.
@return A pointer to the matching object, which should not be modified or deleted. Returns null if no match found.
*/

struct jx * jx_multi_database_lookup( struct jx_multi_database *mdb, const char *key );

/** Remove an object from the database.
Causes a delete (D) record to be generated in the log.
@param db The database to access.
@param key The primary key of the desired object.
@return A pointer to the matching object, which should be discarded with @ref jx_delete when done Returns null if no match found.
*/

struct jx * jx_multi_database_remove( struct jx_multi_database *mdb, const char *key );

/** Begin iteration over all keys in the database.
This function begins a new iteration over the database.
allowing you to visit every primary key in the database.
Next, invoke @ref jx_multi_database_nextkey to retrieve each value in order.
@param db The database to access.
*/

void jx_multi_database_firstkey( struct jx_multi_database *mdb );

/** Continue iteration over the database.
This function returns the next primary key and object in the iteration.
@param db The database to access.
@param key A pointer to an unset char pointer, which will be made to point to the primary key.
@param j A pointer to an unset jx pointer, which will be made to point to the next object.
@return Zero if there are no more elements to visit, non-zero otherwise.
*/

int  jx_multi_database_nextkey( struct jx_multi_database *mdb, char **key, struct jx **j );

#endif
