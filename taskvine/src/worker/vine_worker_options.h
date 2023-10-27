#ifndef VINE_WORKER_OPTIONS_H
#define VINE_WORKER_OPTIONS_H

#include <stdint.h>
#include <sys/time.h>

#include "hash_table.h"
#include "timestamp.h"

struct vine_worker_options {
	
	/* 0 means not given as a command line option. */
	int64_t manual_cores_option;
	int64_t manual_disk_option;
	int64_t manual_memory_option;
	time_t manual_wall_time_option;

	/* -1 means not given as a command line option. */
	int64_t manual_gpus_option;

	/* In single shot mode, immediately quit when disconnected. Useful for accelerating the test suite. */
	int single_shot_mode;

	/* Maximum time to stay connected to a single manager without any work. */
	int idle_timeout;

	/* Current time at which we will give up if no work is received. */
	time_t idle_stoptime;

	/* Current time at which we will give up if no manager is found. */
	time_t connect_stoptime;

	/* Maximum time to attempt connecting to all available managers before giving up. */
	int connect_timeout;

	/* Maximum time to attempt sending/receiving any given file or message. */
	int active_timeout;

	/* Initial value for backoff interval (in seconds) when worker fails to connect to a manager. */
	int init_backoff_interval;

	/* Maximum value for backoff interval (in seconds) when worker fails to connect to a manager. */
	int max_backoff_interval;

	/* Absolute end time (in useconds) for worker, worker is killed after this point. */
	timestamp_t end_time;

	/* Password shared between manager and worker. */
	char *vine_worker_password;

	/* If set to "by_ip", "by_hostname", or "by_apparent_ip", overrides manager's preferred connection mode. */
	char *preferred_connection;

	/*
	Whether to force a ssl connection. If using the catalog server and the
	manager announces it is using SSL, then SSL is used regardless of manual_ssl_option.
	*/
	int manual_ssl_option;

	/* Manual option given by the user to control the location of the workspace. */
	char *manual_workspace_option;
	
	/* Table of user-specified features. The key represents the name of the feature. */
	/* The corresponding value is just a pointer to feature_dummy and can be ignored. */
	struct hash_table *features;

	/* How frequently to measure resources available. */
	int check_resources_interval;

	/* Maximum number of seconds to spend on each resource management. */
	int max_time_on_measurement;

};

struct vine_worker_options * vine_worker_options_create();
void vine_worker_options_delete( struct vine_worker_options *s );

#endif

