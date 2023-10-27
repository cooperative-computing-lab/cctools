#include "vine_worker_options.h"

#include "hash_table.h"

#include <stdlib.h>
#include <string.h>

struct vine_worker_options *vine_worker_options_create()
{
	struct vine_worker_options *self = malloc(sizeof(*self));
	memset(self, 0, sizeof(*self));

	self->manual_gpus_option = -1;
	self->idle_timeout = 900;
	self->connect_timeout = 900;
	self->active_timeout = 3600;

	self->init_backoff_interval = 1;
	self->max_backoff_interval = 8;

	self->check_resources_interval = 5;
	self->max_time_on_measurement = 3;

	self->features = hash_table_create(0, 0);

	return self;
}

void vine_worker_options_delete(struct vine_worker_options *self)
{
	hash_table_delete(self->features);
	free(self);
}
