/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "nvpair_database.h"
#include "nvpair.h"
#include "debug.h"
#include "itable.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>

#define DB_LINE_MAX 1024

struct nvpair_database {
	FILE *logfile;
	struct itable *table;
	char *filename;
	INT64_T logsize;
	INT64_T nextkey;
};

static void nvpair_database_error( struct nvpair_database *db )
{
	fatal("unable to access %s: %s",db->filename,strerror(errno));
}

static void nvpair_database_sync( struct nvpair_database *db )
{
	if(ferror(db->logfile))			nvpair_database_error(db);
	if(fflush(db->logfile)!=0)		nvpair_database_error(db);
	if(ferror(db->logfile))			nvpair_database_error(db);
	if(fsync(fileno(db->logfile))!=0)	nvpair_database_error(db);
}

struct nvpair_database * nvpair_database_open( const char *filename )
{
	struct nvpair_database *db;
	struct nvpair *nv;
	char name[DB_LINE_MAX];
	char value[DB_LINE_MAX];
	char line[DB_LINE_MAX];
	INT64_T key;

	db = malloc(sizeof(*db));
	db->table = itable_create(0);
	db->logsize = 0;
	db->filename = strdup(filename);
	db->nextkey = 1;

	db->logfile = fopen(filename,"r");
	if(db->logfile) {
		while(fgets(line,sizeof(line),db->logfile)) {
			db->logsize++;
			if(sscanf(line,"I %lld",&key)) {
				nv = nvpair_create();
				if(nvpair_parse_stream(nv,db->logfile)) {
					itable_insert(db->table,key,nv);
				} else {
					nvpair_database_error(db);
				}
				db->nextkey = key+1;
			} else if(sscanf(line,"R %lld",&key)) {
				nv = itable_remove(db->table,key);
				if(nv) nvpair_delete(nv);
 			} else if(sscanf(line,"U %lld %s %s",&key,name,value)) {
				nv = itable_lookup(db->table,key);
				if(nv) nvpair_insert_string(nv,name,value);
			} else if(sscanf(line,"N %lld",&key)) {
				db->nextkey = key;
			} else {
				nvpair_database_error(db);
			}
		}
		fclose(db->logfile);
	}

	db->logfile = fopen(filename,"a");
	if(!db->logfile) {
		nvpair_database_close(db);
		return 0;
	}

	return db;
}

void nvpair_database_compress( struct nvpair_database *db )
{
	char newfilename[DB_LINE_MAX];
	FILE *file;
	int key;
	struct nvpair *nv;

	if(db->logsize < itable_size(db->table)*3 ) return;

	sprintf(newfilename,"%s.new",db->filename);

	file = fopen(newfilename,"w");
	if(!file) nvpair_database_error(db);

	itable_firstkey(db->table);
	while(itable_nextkey(db->table,&key,(void**)&nv)) {
		fprintf(file,"I %d\n",key);
		nvpair_print_text(nv,file);
	}
	fprintf(file,"N %lld\n",db->nextkey);

	if(fflush(file)!=0 || ferror(file) || fsync(fileno(file))!=0 || rename(newfilename,db->filename)!=0) {
		nvpair_database_error(db);
	}


	fclose(db->logfile);
	db->logfile = file;
	db->logsize = itable_size(db->table);
}

void nvpair_database_close( struct nvpair_database *db )
{
	if(db) {
		if(db->logfile) {
			fclose(db->logfile);
		}

		if(db->table) {
			int key;
			struct nvpair *nv;
			itable_firstkey(db->table);
			while(itable_nextkey(db->table,&key,(void**)&nv)) {
				nvpair_delete(nv);
			}
			itable_delete(db->table);
		}

		if(db->filename) {
			free(db->filename);
		}

		free(db);
	}
}

int nvpair_database_insert( struct nvpair_database *db, INT64_T *key, struct nvpair *nv )
{
	*key = db->nextkey++;

	nvpair_insert_integer(nv,"key",*key);
	itable_insert(db->table,*key,nv);
	fprintf(db->logfile,"I %lld\n",*key);
	nvpair_print_text(nv,db->logfile);
	nvpair_database_sync(db);
	nvpair_database_compress(db);

	return 1;
}

struct nvpair * nvpair_database_remove( struct nvpair_database *db, INT64_T key )
{
	struct nvpair *nv;

	nv = itable_remove(db->table,key);
	if(!nv) return 0;

	fprintf(db->logfile,"R %lld\n",key);
	nvpair_database_sync(db);
	nvpair_database_compress(db);

	return nv;
}

struct nvpair * nvpair_database_lookup( struct nvpair_database *db, INT64_T key )
{
	return itable_lookup(db->table,key);
}

int nvpair_database_update_string( struct nvpair_database *db, INT64_T key, const char *name, const char *value )
{
	struct nvpair *nv;

	nv = itable_lookup(db->table,key);
	if(nv) {
		nvpair_insert_string(nv,name,value);
		fprintf(db->logfile,"U %lld %s %s\n",key,name,value);
		nvpair_database_sync(db);
		nvpair_database_compress(db);
		return 1;
	} else {
		return 0;
	}
}

int nvpair_database_update_integer( struct nvpair_database *db, INT64_T key, const char *name, INT64_T value )
{
	struct nvpair *nv;

	nv = itable_lookup(db->table,key);
	if(nv) {
		nvpair_insert_integer(nv,name,value);
		fprintf(db->logfile,"U %lld %s %lld\n",key,name,value);
		nvpair_database_sync(db);
		nvpair_database_compress(db);
		return 1;
	} else {
		return 0;
	}
}

const char * nvpair_database_lookup_string( struct nvpair_database *db, INT64_T key, const char *name )
{
	struct nvpair *nv;

	nv = itable_lookup(db->table,key);
	if(nv) {
		return nvpair_lookup_string(nv,name);
	} else {
		return 0;
	}
}

INT64_T nvpair_database_lookup_integer( struct nvpair_database *db, INT64_T key, const char *name )
{
	struct nvpair *nv;

	nv = itable_lookup(db->table,key);
	if(nv) {
		return nvpair_lookup_integer(nv,name);
	} else {
		return 0;
	}
}

void nvpair_database_firstkey( struct nvpair_database *db )
{
	itable_firstkey(db->table);
}

int nvpair_database_nextkey( struct nvpair_database *db, INT64_T *key, struct nvpair **nv )
{
	int skey;
	if(itable_nextkey(db->table,&skey,(void**)nv)) {
		*key = skey;
		return 1;
	} else {
		return 0;
	}
}
