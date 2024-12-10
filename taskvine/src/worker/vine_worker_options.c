#include "vine_worker_options.h"
#include "vine_catalog.h"
#include "vine_transfer_server.h"

#include "address.h"
#include "catalog_query.h"
#include "cctools.h"
#include "copy_stream.h"
#include "create_dir.h"
#include "debug.h"
#include "domain_name.h"
#include "hash_table.h"
#include "macros.h"
#include "path.h"
#include "stringtools.h"

#include "getopt.h"
#include "getopt_aux.h"
#include "xxmalloc.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/utsname.h>

struct vine_worker_options *vine_worker_options_create()
{
	struct vine_worker_options *self = malloc(sizeof(*self));
	memset(self, 0, sizeof(*self));

	self->gpus_total = -1;
	self->idle_timeout = 900;
	self->connect_timeout = 900;
	self->active_timeout = 3600;

	self->init_backoff_interval = 1;
	self->max_backoff_interval = 8;

	self->check_resources_interval = 5;
	self->max_time_on_measurement = 3;

	self->features = hash_table_create(0, 0);

	struct utsname uname_data;
	uname(&uname_data);
	self->os_name = xxstrdup(uname_data.sysname);
	self->arch_name = xxstrdup(uname_data.machine);

	self->catalog_hosts = xxstrdup(CATALOG_HOST);
	self->disk_percent = 50;

	self->initial_ppid = 0;

	self->tls_sni = NULL;

	self->transfer_port_min = 0;
	self->transfer_port_max = 0;

	self->max_transfer_procs = 10;

	self->reported_transfer_host = 0;

	return self;
}

void vine_worker_options_delete(struct vine_worker_options *self)
{
	/* XXX: if part not needed here, free(NULL) is legal */
	if (self->os_name)
		free(self->os_name);
	if (self->arch_name)
		free(self->arch_name);
	if (self->project_regex)
		free(self->project_regex);
	if (self->catalog_hosts)
		free(self->catalog_hosts);
	if (self->factory_name)
		free(self->factory_name);
	if (self->reported_transfer_host)
		free(self->reported_transfer_host);

	hash_table_delete(self->features);
	free(self);
}

void vine_worker_options_show_help(const char *cmd, struct vine_worker_options *options)
{
	printf("Use: %s [options] <managerhost> <port> \n"
	       "or\n     %s [options] \"managerhost:port[;managerhost:port;managerhost:port;...]\"\n"
	       "or\n     %s [options] -M projectname\n",
			cmd,
			cmd,
			cmd);
	printf("where options are:\n");
	printf(" %-30s Show version string\n", "-v,--version");
	printf(" %-30s Show this help screen\n", "-h,--help");
	printf(" %-30s Name of manager (project) to contact.  May be a regular expression.\n", "-M,--manager-name=<name>");
	printf(" %-30s Catalog server to query for managers.  (default: %s:%d) \n", "-C,--catalog=<host:port>", CATALOG_HOST, CATALOG_PORT);
	printf(" %-30s Enable debugging for this subsystem.\n", "-d,--debug=<subsystem>");
	printf(" %-30s Send debugging to this file. (can also be :stderr, or :stdout)\n", "-o,--debug-file=<file>");
	printf(" %-30s Set the maximum size of the debug log (default 10M, 0 disables).\n", "--debug-rotate-max=<bytes>");
	printf(" %-30s Use SSL to connect to the manager. (Not needed if using -M)", "--ssl");
	printf(" %-30s Password file for authenticating to the manager.\n", "-P,--password=<pwfile>");
	printf(" %-30s Set both --idle-timeout and --connect-timeout.\n", "-t,--timeout=<time>");
	printf(" %-30s Disconnect after this time if manager sends no work. (default=%ds)\n", "   --idle-timeout=<time>", options->idle_timeout);
	printf(" %-30s Abort after this time if no managers are available. (default=%ds)\n", "   --connect-timeout=<time>", options->idle_timeout);
	printf(" %-30s Exit if parent process dies.\n", "--parent-death");
	printf(" %-30s Set TCP window size.\n", "-w,--tcp-window-size=<size>");
	printf(" %-30s Set initial value for backoff interval when worker fails to connect\n", "-i,--min-backoff=<time>");
	printf(" %-30s to a manager. (default=%ds)\n", "", options->init_backoff_interval);
	printf(" %-30s Set maximum value for backoff interval when worker fails to connect\n", "-b,--max-backoff=<time>");
	printf(" %-30s to a manager. (default=%ds)\n", "", options->max_backoff_interval);
	printf(" %-30s Set architecture string for the worker to report to manager instead\n", "-A,--arch=<arch>");
	printf(" %-30s of the value in uname (%s).\n", "", options->arch_name);
	printf(" %-30s Set operating system string for the worker to report to manager instead\n", "-O,--os=<os>");
	printf(" %-30s of the value in uname (%s).\n", "", options->os_name);
	printf(" %-30s Set the workspace dir for this worker. (default is /tmp/worker-UID-PID)\n", "-s,--workspace=<path>");
	printf(" %-30s Keep (do not delete) the workspace dir when worker exits.\n", "   --keep-workspace");
	printf(" %-30s Set the number of cores reported by this worker. If not given, or less than 1,\n", "--cores=<n>");
	printf(" %-30s then try to detect cores available.\n", "");

	printf(" %-30s Set the number of GPUs reported by this worker. If not given, or less than 0,\n", "--gpus=<n>");
	printf(" %-30s then try to detect gpus available.\n", "");

	printf(" %-30s Manually set the amount of memory (in MB) reported by this worker.\n", "--memory=<mb>");
	printf(" %-30s If not given, or less than 1, then try to detect memory available.\n", "");

	printf(" %-30s Manually set the amount of disk (in MB) reported by this worker.\n", "--disk=<mb>");
	printf(" %-30s If not given, or less than 1, then try to detect disk space available.\n", "");

	printf(" %-30s Set the conservative disk reporting percent when --disk is unspecified.\n", "--disk-percent=<percent>");
	printf(" %-30s Defaults to %d.\n", "", options->disk_percent);

	printf(" %-30s Use loop devices for task sandboxes (default=disabled, requires root access).\n", "--disk-allocation");
	printf(" %-30s Specifies a user-defined feature the worker provides. May be specified several times.\n", "--feature");
	printf(" %-30s Set the maximum number of seconds the worker may be active. (in s).\n", "--wall-time=<s>");

	printf(" %-30s When using -M, override manager preference to resolve its address.\n", "--connection-mode");
	printf(" %-30s One of by_ip, by_hostname, or by_apparent_ip. Default is set by manager.\n", "");

	printf(" %-30s Forbid the use of symlinks for cache management.\n", "--disable-symlinks");
	printf(" %-30s Single-shot mode -- quit immediately after disconnection.\n", "--single-shot");
	printf(" %-30s Listening port for worker-worker transfers. Either port or port_min:port_max (default: any)\n", "--transfer-port");
	printf(" %-30s Explicit contact host:port for worker-worker transfers, e.g., when routing is used. (default: :<transfer_port>)\n", "--contact-hostport");
	printf(" %-30s Maximum number of concurrent worker transfer requests (default=%d)\n", "--max-transfer-procs", options->max_transfer_procs);

	printf(" %-30s Enable tls connection to manager (manager should support it).\n", "--ssl");
	printf(" %-30s SNI domain name if different from manager hostname. Implies --ssl.\n", "--tls-sni=<domain name>");
}

enum {
	LONG_OPT_DEBUG_FILESIZE = 256,
	LONG_OPT_BANDWIDTH,
	LONG_OPT_DEBUG_RELEASE,
	LONG_OPT_CORES,
	LONG_OPT_MEMORY,
	LONG_OPT_DISK,
	LONG_OPT_DISK_PERCENT,
	LONG_OPT_GPUS,
	LONG_OPT_OPTIONS_IDLE_TIMEOUT,
	LONG_OPT_CONNECT_TIMEOUT,
	LONG_OPT_SINGLE_SHOT,
	LONG_OPT_WALL_TIME,
	LONG_OPT_MEMORY_THRESHOLD,
	LONG_OPT_FEATURE,
	LONG_OPT_PARENT_DEATH,
	LONG_OPT_CONN_MODE,
	LONG_OPT_USE_SSL,
	LONG_OPT_TLS_SNI,
	LONG_OPT_PYTHON_FUNCTION,
	LONG_OPT_FROM_FACTORY,
	LONG_OPT_TRANSFER_PORT,
	LONG_OPT_CONTACT_HOSTPORT,
	LONG_OPT_WORKSPACE,
	LONG_OPT_KEEP_WORKSPACE,
	LONG_OPT_MAX_TRANSFER_PROCS,
};

static const struct option long_options[] = {{"advertise", no_argument, 0, 'a'},
		{"catalog", required_argument, 0, 'C'},
		{"debug", required_argument, 0, 'd'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, LONG_OPT_DEBUG_FILESIZE},
		{"manager-name", required_argument, 0, 'M'},
		{"master-name", required_argument, 0, 'M'},
		{"password", required_argument, 0, 'P'},
		{"timeout", required_argument, 0, 't'},
		{"idle-timeout", required_argument, 0, LONG_OPT_OPTIONS_IDLE_TIMEOUT},
		{"connect-timeout", required_argument, 0, LONG_OPT_CONNECT_TIMEOUT},
		{"tcp-window-size", required_argument, 0, 'w'},
		{"min-backoff", required_argument, 0, 'i'},
		{"max-backoff", required_argument, 0, 'b'},
		{"single-shot", no_argument, 0, LONG_OPT_SINGLE_SHOT},
		{"disk-threshold", required_argument, 0, 'z'},
		{"memory-threshold", required_argument, 0, LONG_OPT_MEMORY_THRESHOLD},
		{"arch", required_argument, 0, 'A'},
		{"os", required_argument, 0, 'O'},
		{"workdir", required_argument, 0, 's'}, // backwards compatibility
		{"workspace", required_argument, 0, LONG_OPT_WORKSPACE},
		{"keep-workspace", no_argument, 0, LONG_OPT_KEEP_WORKSPACE},
		{"bandwidth", required_argument, 0, LONG_OPT_BANDWIDTH},
		{"cores", required_argument, 0, LONG_OPT_CORES},
		{"memory", required_argument, 0, LONG_OPT_MEMORY},
		{"disk", required_argument, 0, LONG_OPT_DISK},
		{"disk-percent", required_argument, 0, LONG_OPT_DISK_PERCENT},
		{"gpus", required_argument, 0, LONG_OPT_GPUS},
		{"wall-time", required_argument, 0, LONG_OPT_WALL_TIME},
		{"help", no_argument, 0, 'h'},
		{"version", no_argument, 0, 'v'},
		{"feature", required_argument, 0, LONG_OPT_FEATURE},
		{"parent-death", no_argument, 0, LONG_OPT_PARENT_DEATH},
		{"connection-mode", required_argument, 0, LONG_OPT_CONN_MODE},
		{"ssl", no_argument, 0, LONG_OPT_USE_SSL},
		{"tls-sni", required_argument, 0, LONG_OPT_TLS_SNI},
		{"from-factory", required_argument, 0, LONG_OPT_FROM_FACTORY},
		{"transfer-port", required_argument, 0, LONG_OPT_TRANSFER_PORT},
		{"max-transfer-procs", required_argument, 0, LONG_OPT_MAX_TRANSFER_PROCS},
		{"contact-hostport", required_argument, 0, LONG_OPT_CONTACT_HOSTPORT},
		{0, 0, 0, 0}};

static void vine_worker_options_get_env(const char *name, int64_t *manual_option)
{
	char *value;
	value = getenv(name);
	if (value) {
		*manual_option = atoi(value);
		/* unset variable so that children task cannot read the global value */
		unsetenv(name);
	}
}

static void vine_worker_options_get_envs(struct vine_worker_options *options)
{
	vine_worker_options_get_env("CORES", &options->cores_total);
	vine_worker_options_get_env("MEMORY", &options->memory_total);
	vine_worker_options_get_env("DISK", &options->disk_total);
	vine_worker_options_get_env("GPUS", &options->gpus_total);
}

void set_transfer_host(struct vine_worker_options *options, const char *hostport)
{
	int error = 0;

	free(options->reported_transfer_host);
	options->reported_transfer_host = NULL;

	if (!hostport || (strlen(hostport) == 0)) {
		error = 1;
	} else if (hostport[0] == ':') {
		char *end = NULL;
		errno = 0;
		options->reported_transfer_port = strtoll((hostport + 1), &end, 10);
		if (options->reported_transfer_port == 0 && error != 0) {
			error = 1;
		}
	} else {
		int port;
		char host[DOMAIN_NAME_MAX];
		if (address_parse_hostport(hostport, host, &port, 0)) {
			options->reported_transfer_host = xxstrdup(host);
			options->reported_transfer_port = port;
		} else {
			error = 1;
		}
	}

	if (error) {
		fatal("transfer host not of the form HOSTNAME:PORT or :PORT");
	}
}

void set_min_max_ports(struct vine_worker_options *options, const char *range)
{
	char *r = xxstrdup(range);
	char *ptr;

	ptr = strtok(r, ":");
	options->transfer_port_min = atoi(ptr);
	options->transfer_port_max = options->transfer_port_min;

	ptr = strtok(NULL, ":");
	if (ptr) {
		options->transfer_port_max = atoi(ptr);
	}

	ptr = strtok(NULL, ":");
	if (ptr) {
		fatal("Malformed port range. Expected either a PORT or PORT_MIN:PORT_MAX");
	}

	if (options->transfer_port_min > options->transfer_port_max) {
		fatal("Malformed port range. PORT_MIN > PORT_MAX");
	}

	free(r);
}

void vine_worker_options_get(struct vine_worker_options *options, int argc, char *argv[])
{
	int c;

	/* Before parsing the command line, read in defaults from environment. */
	/* These will be overridden by command line args if needed. */
	vine_worker_options_get_envs(options);

	while ((c = getopt_long(argc, argv, "aC:d:t:o:p:M:N:P:w:i:b:z:A:O:s:v:h", long_options, 0)) != -1) {
		switch (c) {
		case 'C':
			options->catalog_hosts = xxstrdup(optarg);
			break;
		case 'd':
			debug_flags_set(optarg);
			break;
		case LONG_OPT_DEBUG_FILESIZE:
			debug_config_file_size(MAX(0, string_metric_parse(optarg)));
			break;
		case 't':
			options->connect_timeout = options->idle_timeout = string_time_parse(optarg);
			break;
		case LONG_OPT_OPTIONS_IDLE_TIMEOUT:
			options->idle_timeout = string_time_parse(optarg);
			break;
		case LONG_OPT_CONNECT_TIMEOUT:
			options->connect_timeout = string_time_parse(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'M':
		case 'N':
			options->project_regex = xxstrdup(optarg);
			break;
		case 'w': {
			int w = string_metric_parse(optarg);
			link_window_set(w, w);
			break;
		}
		case 'i':
			options->init_backoff_interval = string_metric_parse(optarg);
			break;
		case 'b':
			options->max_backoff_interval = string_metric_parse(optarg);
			if (options->max_backoff_interval < options->init_backoff_interval) {
				fprintf(stderr, "Maximum backoff interval provided must be greater than the initial backoff interval of %ds.\n", options->init_backoff_interval);
				exit(1);
			}
			break;
		case 'A':
			options->arch_name = xxstrdup(optarg);
			break;
		case 'O':
			options->os_name = xxstrdup(optarg);
			break;
		case LONG_OPT_WORKSPACE:
		case 's': {
			char temp_abs_path[PATH_MAX];
			create_dir(optarg, 0755);
			path_absolute(optarg, temp_abs_path, 1);
			options->workspace_dir = xxstrdup(temp_abs_path);
			break;
		}
		case LONG_OPT_KEEP_WORKSPACE:
			if (!options->workspace_dir) {
				fprintf(stderr, "%s: error: --keep-workspace also requires explicit --workspace argument.\n", argv[0]);
				exit(EXIT_FAILURE);
			}
			options->keep_workspace_at_exit = 1;
			break;
		case 'v':
			cctools_version_print(stdout, argv[0]);
			exit(EXIT_SUCCESS);
			break;
		case 'P':
			if (copy_file_to_buffer(optarg, &options->password, NULL) < 0) {
				fprintf(stderr, "vine_worker: couldn't load password from %s: %s\n", optarg, strerror(errno));
				exit(EXIT_FAILURE);
			}
			break;
		case LONG_OPT_BANDWIDTH:
			setenv("VINE_BANDWIDTH", optarg, 1);
			break;
		case LONG_OPT_DEBUG_RELEASE:
			setenv("VINE_RESET_DEBUG_FILE", "yes", 1);
			break;
		case LONG_OPT_CORES:
			if (!strncmp(optarg, "all", 3)) {
				options->cores_total = 0;
			} else {
				options->cores_total = atoi(optarg);
			}
			break;
		case LONG_OPT_MEMORY:
			if (!strncmp(optarg, "all", 3)) {
				options->memory_total = 0;
			} else {
				options->memory_total = atoll(optarg);
			}
			break;
		case LONG_OPT_DISK:
			if (!strncmp(optarg, "all", 3)) {
				options->disk_total = 0;
			} else {
				options->disk_total = atoll(optarg);
			}
			break;
		case LONG_OPT_DISK_PERCENT:
			if (!strncmp(optarg, "all", 3)) {
				options->disk_percent = 100;
			} else {
				/* guard the disk percent value to [0, 100]. */
				options->disk_percent = MIN(100, MAX(atoi(optarg), 0));
			}
			break;
		case LONG_OPT_GPUS:
			if (!strncmp(optarg, "all", 3)) {
				options->gpus_total = -1;
			} else {
				options->gpus_total = atoi(optarg);
			}
			break;
		case LONG_OPT_WALL_TIME:
			options->manual_wall_time_option = atoi(optarg);
			if (options->manual_wall_time_option < 1) {
				options->manual_wall_time_option = 0;
				warn(D_NOTICE, "Ignoring --wall-time, a positive integer is expected.");
			}
			break;
		case LONG_OPT_SINGLE_SHOT:
			options->single_shot_mode = 1;
			break;
		case 'h':
			vine_worker_options_show_help(argv[0], options);
			exit(0);
			break;
		case LONG_OPT_FEATURE:
			hash_table_insert(options->features, optarg, "feature");
			break;
		case LONG_OPT_PARENT_DEATH:
			options->initial_ppid = getppid();
			break;
		case LONG_OPT_CONN_MODE:
			free(options->preferred_connection);
			options->preferred_connection = xxstrdup(optarg);
			if (strcmp(options->preferred_connection, "by_ip") && strcmp(options->preferred_connection, "by_hostname") &&
					strcmp(options->preferred_connection, "by_apparent_ip")) {
				fatal("connection-mode should be one of: by_ip, by_hostname, by_apparent_ip");
			}
			break;
		case LONG_OPT_USE_SSL:
			options->ssl_requested = 1;
			break;
		case LONG_OPT_TLS_SNI:
			free(options->tls_sni);
			options->tls_sni = xxstrdup(optarg);
			options->ssl_requested = 1;
			break;
		case LONG_OPT_FROM_FACTORY:
			options->factory_name = xxstrdup(optarg);
			break;
		case LONG_OPT_TRANSFER_PORT:
			set_min_max_ports(options, optarg);
			break;
		case LONG_OPT_CONTACT_HOSTPORT:
			set_transfer_host(options, optarg);
			break;
		case LONG_OPT_MAX_TRANSFER_PROCS:
			options->max_transfer_procs = atoi(optarg);
			break;
		default:
			vine_worker_options_show_help(argv[0], options);
			exit(1);
		}
	}
}
