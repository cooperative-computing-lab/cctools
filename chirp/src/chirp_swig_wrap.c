#include "buffer.h"
#include "chirp_reli.h"
#include "chirp_types.h"
#include "xxmalloc.h"

static void accumulate_one_acl(const char *line, void *args)
{
	buffer_t *B = (struct buffer *) args;

	if(buffer_pos(B) > 0) {
		buffer_printf(B, "\n");
	}

	buffer_putstring(B, line);
}

struct chirp_stat *chirp_wrap_stat(const char *hostname, const char *path, time_t stoptime) {

	struct chirp_stat *info = malloc(sizeof(struct chirp_stat));

	int status = chirp_reli_stat(hostname, path, info, stoptime);

	if(status < 0) {
		free(info);
		return NULL;
	}

	return info;
}

char *chirp_wrap_listacl(const char *hostname, const char *path, time_t stoptime)
{
	buffer_t B[1];
	buffer_init(B);
	buffer_abortonfailure(B, 1);

	int status = chirp_reli_getacl(hostname, path, accumulate_one_acl, B, stoptime);

	char *acls = NULL;
	if(status >= 0) {
		buffer_dup(B, &acls);
	}
	buffer_free(B);

	return acls;
}

char *chirp_wrap_whoami(const char *hostname, time_t stoptime)
{
	char id[4096] = "";

	chirp_reli_whoami(hostname, id, sizeof(id), stoptime);

	return xxstrdup(id);
}

/* vim: set noexpandtab tabstop=4: */
