/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_MULTI_H
#define DELTADB_MULTI_H

/** @file deltadb_multi.h

A deltadb_multi manages a collection of DeltaDB databases,
with each instance specialized to a different type of data.
This effectively gives multiple tables that then can be
queried efficiently and separately.  This API treats the
collection of tables as a single database using the 
as a single database, using the same insert, lookup, and iterate
API as the plain deltadb.  However, queries will now be much
more efficient when broken down by type, which is the common case.
*/

#include "deltadb.h"

/** Create a new database, recovering state from disk if available.
@param path A directory to contain the database on disk.  If it does not exist, it will be created.  If null, no disk storage will be used.
@return A pointer to a newly created multi-database.
*/

struct deltadb_multi * deltadb_multi_create( const char *path );

/** Delete a multi database from memory, retaining state on disk.
@param db The database to close.
*/

void deltadb_multi_delete( struct deltadb_multi *mdb );

/**
Determine if a type string is valid. A type string must be printable and not contain special characters that interfere with file and directory names.
@param name The proposed table name.
@return True if valid.
*/

int deltadb_multi_is_valid_type_string( const char *name );

/** Insert or update an object into the database.
@param db The database to access.
@param key The primary key to associate with the object.
@param j The object in the form of an jx expression.
@return True on success, false on failure.  (Could fail if unable to create a database for a new type.)
*/

int deltadb_multi_insert( struct deltadb_multi *mdb, const char *key, struct jx *j );

/** Look up an object in the database.
@param db The database to access.
@param key The primary key of the desired object.
@return A pointer to the matching object, which should not be modified or deleted. Returns null if no match found.
*/

struct jx * deltadb_multi_lookup( struct deltadb_multi *mdb, const char *key );

/** Remove an object from the database.
Causes a delete (D) record to be generated in the log.
@param db The database to access.
@param key The primary key of the desired object.
@return A pointer to the matching object, which should be discarded with @ref jx_delete when done Returns null if no match found.
*/

struct jx * deltadb_multi_remove( struct deltadb_multi *mdb, const char *key );

/** Begin iteration over all keys in the database.
This function begins a new iteration over the database.
allowing you to visit every primary key in the database.
Next, invoke @ref deltadb_multi_nextkey to retrieve each value in order.
@param db The database to access.
*/

void deltadb_multi_firstkey( struct deltadb_multi *mdb );

/** Continue iteration over the database.
This function returns the next primary key and object in the iteration.
@param db The database to access.
@param key A pointer to an unset char pointer, which will be made to point to the primary key.
@param j A pointer to an unset jx pointer, which will be made to point to the next object.
@return Zero if there are no more elements to visit, non-zero otherwise.
*/

int  deltadb_multi_nextkey( struct deltadb_multi *mdb, char **key, struct jx **j );

#endif
