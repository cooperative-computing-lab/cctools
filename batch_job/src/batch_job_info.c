
#include "batch_job_info.h"

#include <stdlib.h>

struct batch_job_info *batch_job_info_create()
{
	struct batch_job_info *info = calloc(1,sizeof(*info));
	return info;
}

void batch_job_info_delete(struct batch_job_info *info)
{
	free(info);
}

