#include "chirp_types.h"

const char * chirp_job_state_string( chirp_job_state_t state )
{
	switch(state) {
		case CHIRP_JOB_STATE_BEGIN:     return "BEGIN";
		case CHIRP_JOB_STATE_IDLE:      return "IDLE";
		case CHIRP_JOB_STATE_RUNNING:   return "RUNNING";
		case CHIRP_JOB_STATE_SUSPENDED: return "SUSPENDED";
		case CHIRP_JOB_STATE_COMPLETE:  return "COMPLETE";
		case CHIRP_JOB_STATE_FAILED:    return "FAILED";
		case CHIRP_JOB_STATE_KILLED:    return "KILLED";
		default:                        return "UNKNOWN";
	}
}
