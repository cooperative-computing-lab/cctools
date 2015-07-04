#include "chirp_reli.h"

struct chirp_stat *chirp_wrap_stat(const char *hostname, const char *path, time_t stoptime);

char *chirp_wrap_listacl(const char *hostname, const char *path, time_t stoptime);

char *chirp_wrap_whoami(const char *hostname, time_t stoptime);

char *chirp_wrap_hash(const char *hostname, const char *path, const char *algorithm, time_t stoptime);

int64_t chirp_wrap_job_create(const char *host, const char *json, time_t stoptime);
int64_t chirp_wrap_job_commit(const char *host, const char *json, time_t stoptime);
int64_t chirp_wrap_job_kill  (const char *host, const char *json, time_t stoptime);
int64_t chirp_wrap_job_reap  (const char *host, const char *json, time_t stoptime);
char   *chirp_wrap_job_status(const char *host, const char *json, time_t stoptime);
char   *chirp_wrap_job_wait  (const char *host, chirp_jobid_t id, int64_t timeout, time_t stoptime);
