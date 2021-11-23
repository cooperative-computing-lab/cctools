/*
Copyright (C) 2019- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <ctype.h>

#include "deltadb_multi.h"
#include "deltadb.h"
#include "jx.h"

#include "stringtools.h"
#include "create_dir.h"
#include "debug.h"
#include "hash_table.h"

struct deltadb_multi {
	/* Base path of the multi-database */
	char *path;

	/* Table mapping string keys to deltadb objects. */
	struct hash_table *table;

	/* Iterator key/value for working across all databases. */
	char *iter_key;
	struct deltadb *iter_db;
};

/*
To delete a multi-database, first remove and delete
all the sub-databases, then remove the multi-database.
*/

void deltadb_multi_delete( struct deltadb_multi *mdb )
{
	char *key;
	struct deltadb *db;

	hash_table_firstkey(mdb->table);
	while(hash_table_nextkey(mdb->table,&key,(void**)&db)) {
		hash_table_remove(mdb->table,key);
		// XXX individual delete doesn't exist yet
		// deltadb_delete(db);
	}

	hash_table_delete(mdb->table);
	free(mdb->path);
	free(mdb);
}

/*
To create a multi-database, we must open the base path, identify
each of the directories within those sub-paths, and open each
of those as distinct deltadbs.
*/

struct deltadb_multi * deltadb_multi_create( const char *path )
{
	struct deltadb_multi *mdb = malloc(sizeof(*mdb));
	memset(mdb,0,sizeof(*mdb));
	mdb->table = hash_table_create(0,0);
	mdb->path = strdup(path);

	create_dir(path,0777);

	DIR *dir = opendir(path);
	if(!dir) {
		deltadb_multi_delete(mdb);
		return 0;
	}

	struct dirent *d;
	while((d=readdir(dir))) {
		if(!strcmp(d->d_name,".")) continue;
		if(!strcmp(d->d_name,"..")) continue;

		char *dbpath = string_format("%s/%s",path,d->d_name);
		struct deltadb *db = deltadb_create(dbpath);
		if(db) {
			hash_table_insert(mdb->table,dbpath,db);
		} else {
			debug(D_DEBUG,"couldn't open database %s: %s",dbpath,strerror(errno));
		}
		free(dbpath);
	}

	closedir(dir);

	return mdb;
}

/*
To avoid dangerous filenames, a type string
must consist of printables, not begin with dot,
nor contain a slash.
*/

static int is_safe_type_string( const char *s )
{
	if(s[0]=='.') return 0;

	while(*s) {
		if(!isprint(*s)) return 0;
		if(*s=='/') return 0;
		s++;
	}

	return 1;
}

/*
To insert an item into the multi database, first determine the "type"
field of the object, lookup (or create) the corresponding database,
then do the insert.
*/

int deltadb_multi_insert( struct deltadb_multi *mdb, const char *key, struct jx *j )
{
	const char *type = jx_lookup_string(j,"type");
	if(!type) return 0;

	if(!is_safe_type_string(type)) {
		debug(D_NOTICE,"skipping illegal type field: %s",type);
		return 0;
	}

	struct deltadb *db = hash_table_lookup(mdb->table,type);
	if(!db) {
		char *dbpath = string_format("%s/%s",mdb->path,type);
		db = deltadb_create(dbpath);
		free(dbpath);

		if(!db) {
			debug(D_DEBUG,"couldn't create database for type %s: %s",type,strerror(errno));
			return 0;
		}

		hash_table_insert(mdb->table,type,db);
	}

	deltadb_insert(db,key,j);
	return 1;
}	

/*
To lookup an item by key (without knowing the type) we must iterate
over each database and perform a lookup on each one.
*/

struct jx * deltadb_multi_lookup( struct deltadb_multi *mdb, const char *key )
{
	char *dbkey;
	struct deltadb *db;

	hash_table_firstkey(mdb->table);
	while(hash_table_nextkey(mdb->table,&dbkey,(void**)&db)) {
		struct jx * result;
		result = deltadb_lookup(db,key);
		if(result) return result;
	}
	
	return 0;
}

/*
To remove an item by key (without knowing the type) we must iterate
over each database and perform a remove on each one.
*/

struct jx * deltadb_multi_remove( struct deltadb_multi *mdb, const char *key )
{
	char *dbkey;
	struct deltadb *db;

	hash_table_firstkey(mdb->table);
	while(hash_table_nextkey(mdb->table,&dbkey,(void**)&db)) {
		struct jx * result;
		result = deltadb_remove(db,key);
		if(result) return result;
	}
	
	return 0;
}

/*
To perform a whole-database iteration, we first iterate over
the multi database hash using mdb->iter_key and mdb->iter_db.
Then, for each database found, we begin an iteration.
*/

void deltadb_multi_firstkey( struct deltadb_multi *mdb )
{
	mdb->iter_key = 0;
	mdb->iter_db = 0;

	hash_table_firstkey(mdb->table);
	hash_table_nextkey(mdb->table,&mdb->iter_key,(void**)&mdb->iter_db);

	deltadb_firstkey(mdb->iter_db);
}

/*
If possible, return a value from the current database.
Otherwise, attempt to iterate to the next db and return its value.
If that iteration comes to an end, then we are done.
*/

int  deltadb_multi_nextkey( struct deltadb_multi *mdb, char **key, struct jx **j )
{
	if(deltadb_nextkey(mdb->iter_db,key,j)) {
		return 1;
	}

	while(hash_table_nextkey(mdb->table,&mdb->iter_key,(void**)&mdb->iter_db)) {
		if(deltadb_nextkey(mdb->iter_db,key,j)) return 1;
	}

	return 0;
}
