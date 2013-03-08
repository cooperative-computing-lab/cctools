/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_stats.h"

#include "debug.h"
#include "xxmalloc.h"
#include "hash_table.h"
#include "link.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>

static struct hash_table *stats_table = 0;

static UINT64_T total_ops = 0;
static UINT64_T total_bytes_read = 0;
static UINT64_T total_bytes_written = 0;

struct chirp_stats {
	char addr[LINK_ADDRESS_MAX];
	UINT64_T ops;
	UINT64_T bytes_read;
	UINT64_T bytes_written;
};

void chirp_stats_collect( const char *addr, const char *subject, UINT64_T ops, UINT64_T bytes_read, UINT64_T bytes_written )
{
	struct chirp_stats *s;

	if(!stats_table) stats_table = hash_table_create(0,0);

	s = hash_table_lookup(stats_table,addr);
	if(!s) {
		s = malloc(sizeof(*s));
		memset(s,0,sizeof(*s));
		strcpy(s->addr,addr);
		hash_table_insert(stats_table,addr,s);
	}

	s->ops += ops;
	s->bytes_read += bytes_read;
	s->bytes_written += bytes_written;

	total_ops += ops;
	total_bytes_read += bytes_read;
	total_bytes_written += bytes_written;
}

void chirp_stats_summary(char *buf, int length)
{
	int chunk;
	char *addr;
	struct chirp_stats *s;

	if(!stats_table) stats_table = hash_table_create(0,0);

	chunk = snprintf(buf,length,"bytes_written %" PRIu64 "\nbytes_read %" PRIu64 "\ntotal_ops %" PRIu64 "\n",total_bytes_written,total_bytes_read,total_ops);
	length -= chunk;
	buf += chunk;

	chunk = snprintf(buf, length, "clients ");

	length -= chunk;
	buf += chunk;

	hash_table_firstkey(stats_table);
	while(hash_table_nextkey(stats_table,&addr,(void**)&s)) {
		chunk = snprintf(buf, length, "%s,1,1,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "; ", s->addr, s->ops, s->bytes_read, s->bytes_written);
		buf += chunk;
		length -= chunk;
	}

	snprintf(buf, length, "\n");
}

void chirp_stats_cleanup()
{
	char *addr;
	struct chirp_stats *s;

	if(!stats_table) stats_table = hash_table_create(0,0);

	hash_table_firstkey(stats_table);
	while(hash_table_nextkey(stats_table,&addr,(void**)&s)) {
		hash_table_remove(stats_table,addr);
		free(s);
	}
}

static UINT64_T child_ops = 0;
static UINT64_T child_bytes_read = 0;
static UINT64_T child_bytes_written = 0;
static time_t  child_report_time = 0;

void chirp_stats_update( UINT64_T ops, UINT64_T bytes_read, UINT64_T bytes_written )
{
	child_ops += ops;
	child_bytes_read += bytes_read;
	child_bytes_written += bytes_written;
}

void chirp_stats_report( int pipefd, const char *addr, const char *subject, int interval )
{
	char line[PIPE_BUF];

	if(time(0)-child_report_time > interval) {
		snprintf(line,PIPE_BUF,"stats %s %s %" PRId64 " %" PRId64 " %" PRId64 "\n",addr,subject,child_ops,child_bytes_read,child_bytes_written);
		write(pipefd,line,strlen(line));
		debug(D_DEBUG,"sending stats: %s",line);
		child_ops = child_bytes_read = child_bytes_written = 0;
		child_report_time = time(0);
	}
}

