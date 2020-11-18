#include "ds_ids.h"

#include "uuid.h"
#include "stringtools.h"

char *ds_create_taskid( struct ds_manager *m )
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);
	return string_format("T-%s",uuid.str);
}

char *ds_create_fileid( struct ds_manager *m )
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);
	return string_format("F-%s",uuid.str);
}

char *ds_create_blobid( struct ds_manager *m )
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);
	return string_format("B-%s",uuid.str);
}

char *ds_create_serviceid( struct ds_manager *m )
{
	cctools_uuid_t uuid;
	cctools_uuid_create(&uuid);
	return string_format("S-%s",uuid.str);
}

