#include "buffer.h"
#include "chirp_reli.h"
#include "chirp_types.h"
#include "xxmalloc.h"

#include <assert.h>

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

char *chirp_wrap_hash(const char *hostname, const char *path, const char *algorithm, time_t stoptime) {
	int i, result;
	unsigned char digest[CHIRP_DIGEST_MAX];
	char hexdigest[CHIRP_DIGEST_MAX*2+1] = "";

	result = chirp_reli_hash(hostname, path, algorithm, digest, stoptime);

	if(result < 0)
		return NULL;

	assert(result <= CHIRP_DIGEST_MAX);
	for (i = 0; i < result; i++)
		sprintf(&hexdigest[i*2], "%02X", (unsigned int)digest[i]);

	return xxstrdup(hexdigest);
}

int64_t chirp_wrap_job_create (const char *host, const char *json, time_t stoptime)
{
	chirp_jobid_t id;

	int64_t result;
	result = chirp_reli_job_create(host, json, &id, stoptime);

	if(result < 0)
		return result;

	return id;
}


int64_t chirp_wrap_job_commit (const char *host, const char *json, time_t stoptime)
{
	int64_t result;
	result = chirp_reli_job_commit(host, json, stoptime);

	return result;
}


int64_t chirp_wrap_job_kill (const char *host, const char *json, time_t stoptime)
{
	int64_t result;
	result = chirp_reli_job_kill(host, json, stoptime);

	return result;
}


int64_t chirp_wrap_job_reap (const char *host, const char *json, time_t stoptime)
{
	int64_t result;
	result = chirp_reli_job_reap(host, json, stoptime);

	return result;
}


char *chirp_wrap_job_status (const char *host, const char *json, time_t stoptime)
{
	char *status;

	int64_t result;
	result = chirp_reli_job_status(host, json, &status, stoptime);

	if(result < 0)
		return NULL;

	return status;
}


char *chirp_wrap_job_wait  (const char *host, chirp_jobid_t id, int64_t timeout, time_t stoptime)
{
	char *status;

	int64_t result;
	result = chirp_reli_job_wait(host, id, timeout, &status, stoptime);

	if(result < 0)
		return NULL;

	return status;
}


/* vim: set noexpandtab tabstop=8: */
