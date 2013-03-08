/*
Copyright (C) 2012- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef NVPAIR_DATABASE_H
#define NVPAIR_DATABASE_H

/** @file nvpair_database.h

nvpair_database is a persistent database for keeping track of a set of
objects, each indexed by a unique key and described by a set of
arbitrary name value pairs.  The current state of the database
is kept in memory for fast queries, while a history of all modifications
is logged to disk to enable recovering the state of the database
at any point in the past.

The history function is secondary to the online access function,
so as a general rule, errors in accessing the on-disk history
are ignored in order to keep the online access going.

The on-disk history works as follows:

For each day of the year, a checkpoint file is created which is an exact
snapshot of the table at the beginning of the day.  For updates received
during that day, a corresponding log file records the individual changes.

The state of the table at any time can be obtained by starting with
the proper daily checkpoint, then playing the log of updates until
the desired time is reached.  This could be used to creating arbitrary
snapshots in time, or for running more complex queries that show the 
change of a single entity over time.

The log directory is broken down by year and day-of-year, so that
each checkpoint file is named DIR/YEAR/DAY.ckpt and the corresponding
log file is named DIR/YEAR/DAY.log

The checkpoint file is simply the text dump of all of the nvpairs
in the table, in the same format that is delivered online.

The log file consists of a series of records of the following format;

<pre>
T (time)               - Indicates the current time in Unix epoch format.
C (key)                - Create a new object with the given key, followed by the object data.
D (key)                - Delete an object with the given key.
U (key) (name) (value) - Update a property to have the given name and value.
R (key) (name)         - Remove a property with the given name.
</pre>

As of 2012, with approx 300 entities reporting to the catalog,
each day results in 20MB of log data and 150KB of checkpoint data,
totalling under 8GB data per year.
*/

#include "nvpair.h"

/** Create a new database, recovering state from disk if available.
@param logdir A directory to contain the database on disk.  If it does not exist, it will be created.  If null, no disk storage will be used.
@return A pointer to a newly created history table.
*/

struct nvpair_database * nvpair_database_create( const char *logdir );

/** Insert or update an object into the database.
If an object with the same primary key exists in the database, it will generate update (U) records in the log, otherwise a create (C) record is generated against the original object.
@param db The database to access.
@param key The primary key to associate with the object.
@param nv The object in the form of an nvpair.
*/

void            nvpair_database_insert( struct nvpair_database *db, const char *key, struct nvpair *nv );

/** Look up an object in the database.
@param db The database to access.
@param key The primary key of the desired object.
@return A pointer to the matching object, which should not be modified or deleted. Returns null if no match found.
*/

struct nvpair * nvpair_database_lookup( struct nvpair_database *db, const char *key );

/** Remove an object from the database.
Causes a delete (D) record to be generated in the log.
@param db The database to access.
@param key The primary key of the desired object.
@return A pointer to the matching object, which should be discarded with @ref nvpair_delete when done Returns null if no match found.
*/

struct nvpair * nvpair_database_remove( struct nvpair_database *db, const char *key );

/** Begin iteration over all keys in the database.
This function begins a new iteration over the database.
allowing you to visit every primary key in the database.
Next, invoke @ref nvpair_database_nextkey to retrieve each value in order.
@param db The database to access.
*/

void nvpair_database_firstkey( struct nvpair_database *db );

/** Continue iteration over the database.
This function returns the next primary key and object in the iteration.
@param db The database to access.
@param key A pointer to an unset char pointer, which will be made to point to the primary key.
@param nv A pointer to an unset nvpair pointer, which will be made to point to the next object.
@return Zero if there are no more elements to visit, non-zero otherwise.
*/

int  nvpair_database_nextkey( struct nvpair_database *db, char **key, struct nvpair **nv );

#endif
