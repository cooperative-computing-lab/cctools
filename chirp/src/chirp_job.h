#ifndef CHIRP_JOB_H
#define CHIRP_JOB_H

#include "chirp_protocol.h"
#include "chirp_client.h"

void    chirp_job_starter();

INT64_T chirp_job_begin(  const char *subject, const char *cwd, const char *infile, const char *outfile, const char *errfile, const char *path, const char *args );
INT64_T chirp_job_commit( const char *subject, INT64_T jobid );
INT64_T chirp_job_wait( const char *subject, INT64_T jobid, struct chirp_job_state *s, time_t stoptime );
INT64_T chirp_job_kill( const char *subject, INT64_T jobid );
INT64_T chirp_job_remove( const char *subject, INT64_T jobid );

void *                   chirp_job_list_open();
struct chirp_job_state * chirp_job_list_next( void *list );
void                     chirp_job_list_close( void *list );

#endif
