#include "buffer.h"
#include "chirp_reli.h"
#include "chirp_types.h"
#include "xxmalloc.h"

static void accumulate_one_acl(char *line, void *args)
{
	struct buffer *b = (struct buffer *) args;

	if(buffer_pos(b) > 0) {
		buffer_printf(b, "\n");
	}

	buffer_printf(b, line);
}

struct chirp_stat *chirp_wrap_stat(const char *hostname, const char *path, time_t stoptime) {

	struct chirp_stat *info = malloc(sizeof(struct chirp_stat));

	int status = chirp_reli_stat(hostname, path, info, stoptime);

	if(status < 0) {
		return NULL;
	}

	return info;
}

char *chirp_wrap_listacl(const char *hostname, const char *path, time_t stoptime)
{
	struct buffer b;
	buffer_init(&b);

	int status = chirp_reli_getacl(hostname, path, accumulate_one_acl, &b, stoptime);

	char *acls;
	if(status >= 0) {
		acls = xxstrdup(buffer_tostring(&b));
	}
	else {
		acls = NULL;
	}

	buffer_free(&b);

	return acls;
}

char *chirp_wrap_whoami(const char *hostname, time_t stoptime)
{
	static int max_id_len = 1024;
	char id[max_id_len];

	id[0] = '\0';

	chirp_reli_whoami(hostname, id, max_id_len, stoptime);

	return xxstrdup(id);
}

/* vim: set noexpandtab tabstop=4: */
