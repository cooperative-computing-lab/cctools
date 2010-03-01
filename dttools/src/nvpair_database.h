/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef NVPAIR_DATABASE_H
#define NVPAIR_DATABASE_H

#include "nvpair.h"
#include "int_sizes.h"

/** @file nvpair_database.h
An nvpair database maintains a list of name-value pair objects (see @ref nvpair.h),
each identified by a unique integer key.  The database is stored in a single file
and uses logging to implement atomic transactions and crash recovery.  This module
is appropriate to use in situations where a relational database is not appropriate
because the data is small and schema-free.  In the even of a write error, the
current process will be aborted, but the database will remain in a consistent state.
*/

/** Open or create a new database at the given filename.
@param filename The name of the file to use.
@return A pointer to an open database on success, null on failure.
*/
struct nvpair_database * nvpair_database_open( const char *filename );

/** Close an open database.
@param db An open database pointer.
*/
void nvpair_database_close( struct nvpair_database *db );

/** Insert a new object into the database.
@param db An open database pointer.
@param key A pointer to the key of the newly inserted object.
@param nv A pointer to the object to be inserted.
*/
int nvpair_database_insert( struct nvpair_database *db, UINT64_T *key, struct nvpair *nv );

/** Remove an object from the database.
@param db An open database pointer.
@param key The unique key of the object.
@return The object corresponding to the key, or null if none exists.
*/
struct nvpair * nvpair_database_remove( struct nvpair_database *db, UINT64_T key );

/** Look up an object in the database.
@param db An open database pointer.
@param key The unique key of the object.
@return The object corresponding to the key, or null if none exists.
*/
struct nvpair * nvpair_database_lookup( struct nvpair_database *db, UINT64_T key );

/** Update a string property of one object in the database.
@param db An open database pointer.
@param key The key of the object to update.
@param name The name of the property to update.
@param value The value of the property to update.
@return True if the object exists and the update was carried out, false otherwise.
*/
int nvpair_database_update_string( struct nvpair_database *db, UINT64_T key, const char *name, const char *value );

/** Update an integer property of one object in the database.
@param db An open database pointer.
@param key The key of the object to update.
@param name The name of the property to update.
@param value The value of the property to update.
@return True if the object exists and the update was carried out, false otherwise.
*/
int nvpair_database_update_integer( struct nvpair_database *db, UINT64_T key, const char *name, INT64_T value );

/** Look up a string property of one object in the database.
@param db An open database pointer.
@param key The key of the object to look up.
@param name The name of the property to look up.
@return A pointer to the string value, if it exists, or zero otherwise.
*/
const char * nvpair_database_lookup_string( struct nvpair_database *db, UINT64_T key, const char *name );

/** Look up an integer property of one object in the database.
@param db An open database pointer.
@param key The key of the object to look up.
@param name The name of the property to look up.
@return The value in integer form, or zero if not found.
*/
INT64_T nvpair_database_lookup_integer( struct nvpair_database *db, UINT64_T key, const char *name );

/** Begin iterating over the database.
@param db An open database pointer.
*/
void nvpair_database_firstkey( struct nvpair_database *db );

/** Continue iterating over the database.
@param db An open database pointer.
@param key A pointer to the next key.
@param nv A pointer to the next object.
@return True if an object and key were returned, false if the iteration is complete.
*/
int nvpair_database_nextkey( struct nvpair_database *db, UINT64_T *key, struct nvpair **nv );

#endif
