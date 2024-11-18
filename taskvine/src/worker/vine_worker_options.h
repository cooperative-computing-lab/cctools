#ifndef VINE_WORKER_OPTIONS_H
#define VINE_WORKER_OPTIONS_H

#include <stdint.h>
#include <unistd.h>
#include <sys/time.h>

#include "hash_table.h"
#include "timestamp.h"

struct vine_worker_options {
	
	/* 0 means not given as a command line option. */
	int64_t cores_total;
	int64_t disk_total;
	int64_t memory_total;
	time_t manual_wall_time_option;

	/* -1 means not given as a command line option. */
	int64_t gpus_total;

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
	char *password;

	/* If set to "by_ip", "by_hostname", or "by_apparent_ip", overrides manager's preferred connection mode. */
	char *preferred_connection;

	/*
	Whether to force a ssl connection. If using the catalog server and the
	manager announces it is using SSL, then SSL is used regardless of ssl_requested.
	*/
	int ssl_requested;

	/*
	If SNI tls is different from hostname. Implies ssl_requested.
	*/
	char *tls_sni;

	/* Manual option given by the user to control the location of the workspace. */
	char *workspace_dir;

	/* Keep contents of workspace after exit, for future reuse. */
	int keep_workspace_at_exit;
	
	/* Table of user-specified features. The key represents the name of the feature. */
	/* The corresponding value is just a pointer to feature_dummy and can be ignored. */
	struct hash_table *features;

	/* How frequently to measure resources available. */
	int check_resources_interval;

	/* Maximum number of seconds to spend on each resource management. */
	int max_time_on_measurement;

	/* Name of worker architecture and operating system */
	char *arch_name;
	char *os_name;

	/* A regular expression (usually just a plain string) naming the manager(s) to connect to .*/
	char *project_regex;

	/* A string giving the list of catalog hosts to query to find the manager. */
	char *catalog_hosts;

	/* The name of the factory process that started this worker, if any. */
	char *factory_name;

	/* When the amount of disk is not specified, manually set the reporting disk to
	 * this percentage of the measured disk. This safeguards the fact that disk measurements
	 * are estimates and thus may unncessarily forsaken tasks with unspecified resources.
	 * Defaults to 90%. */
	int disk_percent;

	/* The parent process pid, to detect when the parent has exited. */
	pid_t initial_ppid;

	/* Range of ports allowed to set the server for transfers between workers. */
	int transfer_port_min;
	int transfer_port_max;

	/* Maximum number of concurrent worker transfer requests made by worker */
	int max_transfer_procs;

  /* Explicit contact host (address or hostname) for transfers bewteen workers. */
  char *reported_transfer_host;
  int reported_transfer_port;
};

struct vine_worker_options * vine_worker_options_create();
void vine_worker_options_show_help( const char *cmd, struct vine_worker_options *options );
void vine_worker_options_get( struct vine_worker_options *options, int argc, char *argv[] );
void vine_worker_options_delete( struct vine_worker_options *options );

#endif

