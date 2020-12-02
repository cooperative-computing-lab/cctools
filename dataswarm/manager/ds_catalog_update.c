
#include "ds_catalog_update.h"

#include "ds_manager.h"
#include "username.h"
#include "catalog_query.h"
#include "jx.h"
#include "jx_print.h"
#include "jx_parse.h"
#include "debug.h"

struct jx * manager_status_jx( struct ds_manager *m )
{
	char owner[USERNAME_MAX];
	username_get(owner);

	struct jx * j = jx_object(0);
	jx_insert_string(j,"type","ds_manager");
	jx_insert_string(j,"project",m->project_name);
	jx_insert_integer(j,"starttime",(m->start_time/1000000));
	jx_insert_string(j,"owner",owner);
	jx_insert_string(j,"version",CCTOOLS_VERSION);
	jx_insert_integer(j,"port",m->server_port);

	return j;
}

void ds_catalog_update( struct ds_manager *m, int force_update )
{
	if(!m->force_update && (time(0) - m->catalog_last_update_time) < m->update_interval)
		return;

	if(!m->catalog_hosts) m->catalog_hosts = strdup(CATALOG_HOST);

	struct jx *j = manager_status_jx(m);
	char *str = jx_print_string(j);

	debug(D_DATASWARM, "advertising to the catalog server(s) at %s ...", m->catalog_hosts);
	catalog_query_send_update_conditional(m->catalog_hosts, str);

	free(str);
	jx_delete(j);
	m->catalog_last_update_time = time(0);
}

