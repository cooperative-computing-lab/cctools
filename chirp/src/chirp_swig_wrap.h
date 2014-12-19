#include "chirp_reli.h"

struct chirp_stat *chirp_wrap_stat(const char *hostname, const char *path, time_t stoptime);

char *chirp_wrap_listacl(const char *hostname, const char *path, time_t stoptime);

char *chirp_wrap_whoami(const char *hostname, time_t stoptime);
