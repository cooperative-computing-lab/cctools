
#include "vine_cache_meta.h"
#include "vine_protocol.h"

#include "link.h"
#include "debug.h"

#include <stdio.h>
#include <stdlib.h>

struct vine_cache_meta * vine_cache_meta_create( vine_file_type_t type, vine_cache_level_t cache_level, uint64_t size, time_t mtime, timestamp_t transfer_time )
{
	struct vine_cache_meta *m = malloc(sizeof(*m));
	m->type = type;
	m->cache_level = cache_level;
	m->size = size;
	m->mtime = mtime;
	m->transfer_time = transfer_time;
	return m;
}

void vine_cache_meta_delete( struct vine_cache_meta *m )
{
	if(!m) return;
	free(m);
}

struct vine_cache_meta * vine_cache_meta_load( const char *filename )
{
	char line[VINE_LINE_MAX];
	
	FILE *file = fopen(filename,"r");
	if(!file) return 0;

	struct vine_cache_meta *meta = vine_cache_meta_create(0,0,0,0,0);
	
	/* Use of sscanf is simplified by matching %lld with long long */
	long long value;
	
	while(fgets(line,sizeof(line),file)) {
		if(sscanf(line,"type %lldd",&value)) {
			meta->type = value;
		} else if(sscanf(line,"cache_level %lld",&value)) {
			meta->cache_level = value;
		} else if(sscanf(line,"size %lld",&value)) {
			meta->size = value;
		} else if(sscanf(line,"mtime %lld",&value)) {
			meta->mtime = value;
		} else if(sscanf(line,"transfer_time %lld",&value)) {
			meta->transfer_time = value;
		} else {
			debug(D_VINE,"error in %s: %s\n",filename,line);
			vine_cache_meta_delete(meta);
			fclose(file);
			return 0;
		}
	}

	fclose(file);
	return meta;
}

int vine_cache_meta_save( struct vine_cache_meta *meta, const char *filename )
{
	FILE *file = fopen(filename,"w");
	if(!file) return 0;

	fprintf(file,"type %d\ncache_level %d\nsize %lld\nmtime %lld\ntransfer_time %lld\n",
		meta->type,
		meta->cache_level,
		(long long)meta->size,
		(long long)meta->mtime,
		(long long)meta->transfer_time
		);

	fclose(file);

	return 1;
}

