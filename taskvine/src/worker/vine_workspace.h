#ifndef VINE_WORKSPACE_H
#define VINE_WORKSPACE_H

#include "vine_cache.h"

struct vine_workspace {
	char *workspace_dir;
	char *cache_dir;
	char *trash_dir;
	char *temp_dir;
};

struct vine_workspace * vine_workspace_create( const char *manual_tmpdir );
int                     vine_workspace_check( struct vine_workspace *w );
int                     vine_workspace_prepare( struct vine_workspace *w );
int                     vine_workspace_cleanup( struct vine_workspace *w );
void                    vine_workspace_delete( struct vine_workspace *w );

#endif
