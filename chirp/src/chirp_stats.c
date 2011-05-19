/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_stats.h"
#include "debug.h"
#include "xmalloc.h"

#include <time.h>
#include <errno.h>
#include <string.h>
#include <sys/mman.h>

#define SHARED_SEGMENT_SIZE 4096
#define TOTAL_ENTRIES (SHARED_SEGMENT_SIZE/sizeof(struct chirp_stats))

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

static struct chirp_stats *table = 0;

void chirp_stats_init()
{
	table = (void *) mmap(0, SHARED_SEGMENT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if(table == (void *) -1)
		fatal("couldn't allocate shared page: %s\n", strerror(errno));

	memset(table, 0, SHARED_SEGMENT_SIZE);
}

void chirp_stats_sync()
{
	msync((void *) table, SHARED_SEGMENT_SIZE, MS_SYNC | MS_INVALIDATE);
}


struct chirp_stats *chirp_stats_global()
{
	return &table[0];
}

struct chirp_stats *chirp_stats_local_begin(const char *addr)
{
	int i;
	struct chirp_stats *s;

	for(i = 1; i < TOTAL_ENTRIES; i++) {
		if(!strcmp(table[i].addr, addr)) {
			table[i].active_clients++;
			table[i].total_connections++;
			return &table[i];
		}
	}

	for(i = 1; i < TOTAL_ENTRIES; i++) {
		if(!table[i].addr[0]) {
			memset(&table[i], 0, sizeof(struct chirp_stats));
			strcpy(table[i].addr, addr);
			table[i].active_clients++;
			table[i].total_connections++;
			return &table[i];
		}
	}

	/*
	   If we have so many clients that we run out of space
	   in the shared memory area, then just return a non-shared
	   memory chunk.  It won't be reported in the stats, but
	   it will simplify the caller, who doesn't have to check
	   for a null pointer.
	 */

	s = xxmalloc(sizeof(*s));
	memset(s, 0, sizeof(*s));
	return s;
}

void chirp_stats_local_end(struct chirp_stats *s)
{
	s->active_clients--;
}

void chirp_stats_cleanup()
{
	int i;
	for(i = 1; i < TOTAL_ENTRIES; i++) {
		struct chirp_stats *s = &table[i];
		if(s->active_clients == 0) {
			memset(s, 0, sizeof(*s));
		} else {
			s->total_connections = 0;
			s->total_ops = 0;
			s->bytes_read = 0;
			s->bytes_written = 0;
		}
	}
}


void chirp_stats_summary(char *buf, int length)
{
	int i;
	int chunk;
	struct chirp_stats *s;

	chunk = snprintf(buf, length, "clients ");

	length -= chunk;
	buf += chunk;

	for(i = 1; i < TOTAL_ENTRIES; i++) {
		s = &table[i];

		if(!s->addr[0]) {
			continue;
		}

		chunk = snprintf(buf, length, "%s,%d,%d,%d,%llu,%llu; ", s->addr, s->active_clients, s->total_connections, s->total_ops, s->bytes_read, s->bytes_written);
		buf += chunk;
		length -= chunk;
	}

	snprintf(buf, length, "\n");
}
