/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "cctools.h"
#include "catalog_server.h"
#include "datagram.h"
#include "link.h"
#include "nvpair_database.h"
#include "debug.h"
#include "getopt.h"
#include "nvpair.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "username.h"
#include "list.h"
#include "xxmalloc.h"
#include "macros.h"
#include "daemon.h"
#include "getopt_aux.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/select.h>

#ifndef LINE_MAX
#define LINE_MAX 1024
#endif

#define MAX_TABLE_SIZE 10000

/* Timeout in communicating with the querying client */
#define HANDLE_QUERY_TIMEOUT 15

/* The table of nvpairs, hashed on address:port */
static struct nvpair_database *table = 0;

/* An array of nvpais used to sort for display */
static struct nvpair *array[MAX_TABLE_SIZE];

/* The time for which updated data lives before automatic deletion */
static int lifetime = 1800;

/* Time when the table was most recently cleaned of expired items. */
static time_t last_clean_time = 0;

/* How frequently to clean out expired items. */
static time_t clean_interval = 60;

/* The port upon which to listen. */
static int port = CATALOG_PORT_DEFAULT;

/* The file for writing out the port number. */
const char *port_file = 0;

/* The preferred hostname set on the command line. */
static const char *preferred_hostname = 0;

/* This machine's canonical hostname. */
static char hostname[DOMAIN_NAME_MAX];

/* This process's owner */
static char owner[USERNAME_MAX];

/* Time when the process was started. */
static time_t starttime;

/* If true, for for every query */
static int fork_mode = 1;

/* The maximum number of simultaneous children that can be running. */
static int child_procs_max = 50;

/* Number of query processses currently running. */
static int child_procs_count = 0;

/* Maximum time to allow a child process to run. */
static int child_procs_timeout = 60;

/* The maximum size of a server that will actually be believed. */
static INT64_T max_server_size = 0;

/* Logfile for new updates. */
static FILE *logfile = 0;
static char *logfilename = 0;

/* Location of the history file. */
static const char * history_dir = 0;

/* debug filename */
static char *debug_filename = 0;

/* Settings for the master catalog that we will report *to* */
static int outgoing_alarm = 0;
static int outgoing_timeout = 300;
static struct list *outgoing_host_list;

struct datagram *update_dgram = 0;
struct datagram *outgoing_dgram = 0;

void shutdown_clean(int sig)
{
	exit(0);
}

void ignore_signal(int sig)
{
}

static void install_handler(int sig, void (*handler) (int sig))
{
	struct sigaction s;
	s.sa_handler = handler;
	sigfillset(&s.sa_mask);
	s.sa_flags = 0;
	sigaction(sig, &s, 0);
}

int compare_nvpair(const void *a, const void *b)
{
	struct nvpair **pa = (struct nvpair **) a;
	struct nvpair **pb = (struct nvpair **) b;

	const char *sa = nvpair_lookup_string(*pa, "name");
	const char *sb = nvpair_lookup_string(*pb, "name");

	if(!sa)
		sa = "unknown";
	if(!sb)
		sb = "unknown";

	return strcasecmp(sa, sb);
}

static void remove_expired_records()
{
	struct nvpair *nv;
	char *key;

	time_t current = time(0);

	// Only clean every clean_interval seconds.
	if((current-last_clean_time)<clean_interval) return;

	// After restarting, all records will have appear to be stale.
	// Run for a minimum of lifetime seconds before cleaning anything up.
	if((current-starttime)<lifetime ) return;

	nvpair_database_firstkey(table);
	while(nvpair_database_nextkey(table, &key, &nv)) {
		time_t lastheardfrom = nvpair_lookup_integer(nv,"lastheardfrom");

		int this_lifetime = nvpair_lookup_integer(nv,"lifetime");
		if(this_lifetime>0) {
			this_lifetime = MIN(lifetime,this_lifetime);
		} else {
			this_lifetime = lifetime;
		}

		if( (current-lastheardfrom) > this_lifetime ) {
		    	nv = nvpair_database_remove(table,key);
			if(nv) nvpair_delete(nv);
		}
	}

	last_clean_time = current;
}


static int update_one_catalog(void *outgoing_host, const void *text)
{
	char addr[DATAGRAM_ADDRESS_MAX];
	if(domain_name_cache_lookup(outgoing_host, addr)) {
		debug(D_DEBUG, "sending update to %s:%d", outgoing_host, CATALOG_PORT);
		datagram_send(outgoing_dgram, text, strlen(text), addr, CATALOG_PORT);
	}
	return 1;
}

static void update_all_catalogs(struct datagram *outgoing_dgram)
{
	char text[DATAGRAM_PAYLOAD_MAX];
	int length;

	length = sprintf(text, "type catalog\nversion %d.%d.%s\nurl http://%s:%d\nname %s\nowner %s\nstarttime %lu\nport %d\n", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, preferred_hostname, port, preferred_hostname, owner, (long)starttime, port);

	if(!length)
		return;

	list_iterate(outgoing_host_list, update_one_catalog, text);
}

static void make_hash_key(struct nvpair *nv, char *key)
{
	const char *name, *addr;
	int port;

	addr = nvpair_lookup_string(nv, "address");
	if(!addr)
		addr = "unknown";

	port = nvpair_lookup_integer(nv, "port");

	name = nvpair_lookup_string(nv, "name");
	if(!name)
		name = "unknown";

	sprintf(key, "%s:%d:%s", addr, port, name);
}

static void handle_updates(struct datagram *update_port)
{
	char data[DATAGRAM_PAYLOAD_MAX * 2];
	char addr[DATAGRAM_ADDRESS_MAX];
	char key[LINE_MAX];
	int port;
	int result;
	struct nvpair *nv;

	while(1) {
		result = datagram_recv(update_port, data, DATAGRAM_PAYLOAD_MAX, addr, &port, 0);
		if(result <= 0)
			return;

		data[result] = 0;

		nv = nvpair_create();
		nvpair_parse(nv, data);

		nvpair_insert_string(nv, "address", addr);
		nvpair_insert_integer(nv, "lastheardfrom", time(0));

		/* If the server reports unbelievable numbers, simply reset them */

		if(max_server_size > 0) {
			INT64_T total = nvpair_lookup_integer(nv, "total");
			INT64_T avail = nvpair_lookup_integer(nv, "avail");

			if(total > max_server_size || avail > max_server_size) {
				nvpair_insert_integer(nv, "total", max_server_size);
				nvpair_insert_integer(nv, "avail", max_server_size);
			}
		}

		/* Do not believe the server's reported name, just resolve it backwards. */

		char name[DOMAIN_NAME_MAX];
		if(domain_name_cache_lookup_reverse(addr, name)) {
			nvpair_insert_string(nv, "name", name);
		} else if (nvpair_lookup_string(nv, "name") == NULL) {
			/* If rDNS is unsuccessful, then we use the name reported if given.
			 * This allows for hostnames that are only valid in the subnet of
			 * the reporting server.  Here we set the "name" field to the IP
			 * Address, addr, because it was not set by the reporting server.
			 */
			nvpair_insert_string(nv, "name", addr);
		}

		make_hash_key(nv, key);

		if(logfile) {
			if(!nvpair_database_lookup(table,key)) {
				nvpair_print_text(nv,logfile);
				fflush(logfile);
			}
		}

		nvpair_database_insert(table, key, nv);

		debug(D_DEBUG, "received udp update from %s", key);
	}
}

static struct nvpair_header html_headers[] = {
	{"type", "TYPE", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{"name", "NAME", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{"port", "PORT", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 0},
	{"owner", "OWNER", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{"total", "TOTAL", NVPAIR_MODE_METRIC, NVPAIR_ALIGN_RIGHT, 0},
	{"avail", "AVAIL", NVPAIR_MODE_METRIC, NVPAIR_ALIGN_RIGHT, 0},
	{"load5", "LOAD5", NVPAIR_MODE_STRING, NVPAIR_ALIGN_RIGHT, 0},
	{"version", "VERSION", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{0,0,0,0,0}
};

static void handle_query(struct link *query_link)
{
	FILE *stream;
	char line[LINE_MAX];
	char url[LINE_MAX];
	char path[LINE_MAX];
	char action[LINE_MAX];
	char version[LINE_MAX];
	char hostport[LINE_MAX];
	char addr[LINK_ADDRESS_MAX];
	char key[LINE_MAX];
	int port;
	time_t current;

	char *hkey;
	struct nvpair *nv;
	int i, n;

	link_address_remote(query_link, addr, &port);
	debug(D_DEBUG, "www query from %s:%d", addr, port);

	if(link_readline(query_link, line, LINE_MAX, time(0) + HANDLE_QUERY_TIMEOUT)) {
		string_chomp(line);
		if(sscanf(line, "%s %s %s", action, url, version) != 3) {
			return;
		}

		// Consume the rest of the query
		while(1) {
			if(!link_readline(query_link, line, LINE_MAX, time(0) + HANDLE_QUERY_TIMEOUT)) {
				return;
			}

			if(line[0] == 0) {
				break;
			}
		}
	} else {
		return;
	}

	// Output response
	stream = fdopen(link_fd(query_link), "w");
	if(!stream) {
		return;
	}
	link_nonblocking(query_link, 0);

	current = time(0);
	fprintf(stream, "HTTP/1.1 200 OK\n");
	fprintf(stream, "Date: %s", ctime(&current));
	fprintf(stream, "Server: catalog_server\n");
	fprintf(stream, "Connection: close\n");
	fprintf(stream, "Access-Control-Allow-Origin: *\n");

	if(sscanf(url, "http://%[^/]%s", hostport, path) == 2) {
		// continue on
	} else {
		strcpy(path, url);
	}

	/* load the hash table entries into one big array */

	n = 0;
	nvpair_database_firstkey(table);
	while(nvpair_database_nextkey(table, &hkey, &nv)) {
		array[n] = nv;
		n++;
	}

	/* sort the array by name before displaying */

	qsort(array, n, sizeof(struct nvpair *), compare_nvpair);

	if(!strcmp(path, "/query.text")) {
		fprintf(stream, "Content-type: text/plain\n\n");
		for(i = 0; i < n; i++)
			nvpair_print_text(array[i], stream);
	} else if(!strcmp(path, "/query.json")) {
		fprintf(stream, "Content-type: text/plain\n\n");
		fprintf(stream,"[\n");
		for(i = 0; i < n; i++) {
			nvpair_print_json(array[i], stream);
			if(i<(n-1)) fprintf(stream,",\n");
		}
		fprintf(stream,"]\n");
	} else if(!strcmp(path, "/query.oldclassads")) {
		fprintf(stream, "Content-type: text/plain\n\n");
		for(i = 0; i < n; i++)
			nvpair_print_old_classads(array[i], stream);
	} else if(!strcmp(path, "/query.newclassads")) {
		fprintf(stream, "Content-type: text/plain\n\n");
		for(i = 0; i < n; i++)
			nvpair_print_new_classads(array[i], stream);
	} else if(!strcmp(path, "/query.xml")) {
		fprintf(stream, "Content-type: text/xml\n\n");
		fprintf(stream, "<?xml version=\"1.0\" standalone=\"yes\"?>\n");
		fprintf(stream, "<catalog>\n");
		for(i = 0; i < n; i++)
			nvpair_print_xml(array[i], stream);
		fprintf(stream, "</catalog>\n");
	} else if(sscanf(path, "/detail/%s", key) == 1) {
		struct nvpair *nv;
		fprintf(stream, "Content-type: text/html\n\n");
		nv = nvpair_database_lookup(table, key);
		if(nv) {
			const char *name = nvpair_lookup_string(nv, "name");
			if(!name)
				name = "unknown";
			fprintf(stream, "<title>%s catalog server: %s</title>\n", preferred_hostname, name);
			fprintf(stream, "<center>\n");
			fprintf(stream, "<h1>%s catalog server</h1>\n", preferred_hostname);
			fprintf(stream, "<h2>%s</h2>\n", name);
			fprintf(stream, "<p><a href=/>return to catalog view</a><p>\n");
			nvpair_print_html_solo(nv, stream);
			fprintf(stream, "</center>\n");
		} else {
			fprintf(stream, "<title>%s catalog server</title>\n", preferred_hostname);
			fprintf(stream, "<center>\n");
			fprintf(stream, "<h1>%s catalog server</h1>\n", preferred_hostname);
			fprintf(stream, "<h2>Unknown Item!</h2>\n");
			fprintf(stream, "</center>\n");
		}
	} else {
		char avail_line[LINE_MAX];
		char total_line[LINE_MAX];
		INT64_T sum_total = 0;
		INT64_T sum_avail = 0;
		INT64_T sum_devices = 0;

		fprintf(stream, "Content-type: text/html\n\n");
		fprintf(stream, "<title>%s catalog server</title>\n", preferred_hostname);
		fprintf(stream, "<center>\n");
		fprintf(stream, "<h1>%s catalog server</h1>\n", preferred_hostname);
		fprintf(stream, "<a href=/query.text>text</a> - ");
		fprintf(stream, "<a href=/query.html>html</a> - ");
		fprintf(stream, "<a href=/query.xml>xml</a> - ");
		fprintf(stream, "<a href=/query.json>json</a> - ");
		fprintf(stream, "<a href=/query.oldclassads>oldclassads</a> - ");
		fprintf(stream, "<a href=/query.newclassads>newclassads</a>");
		fprintf(stream, "<p>\n");

		for(i = 0; i < n; i++) {
			nv = array[i];
			sum_total += nvpair_lookup_integer(nv, "total");
			sum_avail += nvpair_lookup_integer(nv, "avail");
			sum_devices++;
		}

		string_metric(sum_avail, -1, avail_line);
		string_metric(sum_total, -1, total_line);
		fprintf(stream, "<b>%sB available out of %sB on %d devices</b><p>\n", avail_line, total_line, (int) sum_devices);

		nvpair_print_html_header(stream, html_headers);
		for(i = 0; i < n; i++) {
			nv = array[i];
			make_hash_key(nv, key);
			sprintf(url, "/detail/%s", key);
			nvpair_print_html_with_link(nv, stream, html_headers, "name", url);
		}
		nvpair_print_html_footer(stream, html_headers);
		fprintf(stream, "</center>\n");
	}
	fclose(stream);
}

static void show_help(const char *cmd)
{
	fprintf(stdout, "Use: %s [options]\n", cmd);
	fprintf(stdout, "where options are:\n");
	fprintf(stdout, " %-30s Run as a daemon.\n", "-b,--background");
	fprintf(stdout, " %-30s Write process identifier (PID) to file.\n", "-B,--pid-file=<file>");
	fprintf(stdout, " %-30s Enable debugging for this subsystem\n", "-d,--debug=<subsystem>");
	fprintf(stdout, " %-30s Show this help screen\n", "-h,--help");
	fprintf(stdout, " %-30s Record catalog history to this directory.\n", "-H,--history=<directory>");
	fprintf(stdout, " %-30s Lifetime of data, in seconds (default is %d)\n", "-l,--lifetime=<secs>", lifetime);
	fprintf(stdout, " %-30s Log new updates to this file.\n", "-L,--update-log=<file>");
	fprintf(stdout, " %-30s Maximum number of child processes.  (default is %d)\n", "-m,--max-jobs=<n>",child_procs_max);
	fprintf(stdout, " %-30s Maximum size of a server to be believed.  (default is any)\n", "-M,--server-size=<size>");
	fprintf(stdout, " %-30s Preferred host name of this server.\n", "-n,--name=<name>");
	fprintf(stdout, " %-30s Send debugging to this file.\n", "-o,--debug-file=<file>");
	fprintf(stdout, " %-30s Rotate debug file once it reaches this size.\n", "-O,--debug-rotate-max=<bytes>");
	fprintf(stdout, " %-30s Port number to listen on (default is %d)\n", "-p,--port=<port>", port);
	fprintf(stdout, " %-30s Single process mode; do not work on queries.\n", "-S,--single");
	fprintf(stdout, " %-30s Maximum time to allow a query process to run.  (default is %ds)\n", "-T,--timeout=<time>",child_procs_timeout);
	fprintf(stdout, " %-30s Send status updates to this host. (default is %s)\n", "-u,--update-host=<host>", CATALOG_HOST_DEFAULT);
	fprintf(stdout, " %-30s Send status updates at this interval. (default is 5m)\n", "-U,--update-interval=<time>");
	fprintf(stdout, " %-30s Show version string\n", "-v,--version");
        fprintf(stdout, " %-30s Select port at random and write it to this file. (default: disabled)\n", "-Z,--port-file=<file>");
}

int main(int argc, char *argv[])
{
	struct link *link, *list_port = 0;
	signed char ch;
	time_t current;
	int is_daemon = 0;
	char *pidfile = NULL;

	outgoing_host_list = list_create();

	debug_config(argv[0]);

	static struct option long_options[] = {
		{"background", no_argument, 0, 'b'},
		{"pid-file", required_argument, 0, 'B'},
		{"debug", required_argument, 0, 'd'},
		{"help", no_argument, 0, 'h'},
		{"history", required_argument, 0, 'H'},
		{"lifetime", required_argument, 0, 'l'},
		{"update-log", required_argument, 0, 'L'},
		{"max-jobs", required_argument, 0, 'm'},
		{"server-size", required_argument, 0, 'M'},
		{"name", required_argument, 0, 'n'},
		{"debug-file", required_argument, 0, 'o'},
		{"debug-rotate-max", required_argument, 0, 'O'},
		{"port", required_argument, 0, 'p'},
		{"single", no_argument, 0, 'S'},
		{"timeout", required_argument, 0, 'T'},
		{"update-host", required_argument, 0, 'u'},
		{"update-interval", required_argument, 0, 'U'},
		{"version", no_argument, 0, 'v'},
		{"port-file", required_argument, 0, 'Z'},
        {0,0,0,0}};


	while((ch = getopt_long(argc, argv, "bB:d:hH:l:L:m:M:n:o:O:p:ST:u:U:vZ:", long_options, NULL)) > -1) {
		switch (ch) {
			case 'b':
				is_daemon = 1;
				break;
			case 'B':
				free(pidfile);
				pidfile = strdup(optarg);
				break;
			case 'd':
				debug_flags_set(optarg);
				break;
			case 'h':
			default:
				show_help(argv[0]);
				return 1;
			case 'l':
				lifetime = string_time_parse(optarg);
				break;
			case 'L':
				logfilename = strdup(optarg);
				break;
			case 'H':
				history_dir = strdup(optarg);
				break;
			case 'm':
				child_procs_max = atoi(optarg);
				break;
			case 'M':
				max_server_size = string_metric_parse(optarg);
				break;
			case 'n':
				preferred_hostname = optarg;
				break;
			case 'o':
				free(debug_filename);
				debug_filename = strdup(optarg);
				break;
			case 'O':
				debug_config_file_size(string_metric_parse(optarg));
				break;
			case 'p':
				port = atoi(optarg);
				break;
			case 'S':
				fork_mode = 0;
				break;
			case 'T':
				child_procs_timeout = string_time_parse(optarg);
				break;
			case 'u':
				list_push_head(outgoing_host_list, xxstrdup(optarg));
				break;
			case 'U':
				outgoing_timeout = string_time_parse(optarg);
				break;
			case 'v':
				cctools_version_print(stdout, argv[0]);
				return 0;
			case 'Z':
				port_file = optarg;
				port = 0;
				break;
			}
	}

	if (is_daemon) daemonize(0, pidfile);

	debug_config_file(debug_filename);

	cctools_version_debug(D_DEBUG, argv[0]);

	if(logfilename) {
		logfile = fopen(logfilename,"a");
		if(!logfile) fatal("couldn't open %s: %s\n",optarg,strerror(errno));
	}

	current = time(0);
	debug(D_ALL, "*** %s starting at %s", argv[0], ctime(&current));

	if(!list_size(outgoing_host_list)) {
		list_push_head(outgoing_host_list, CATALOG_HOST_DEFAULT);
	}

	install_handler(SIGPIPE, ignore_signal);
	install_handler(SIGHUP, ignore_signal);
	install_handler(SIGCHLD, ignore_signal);
	install_handler(SIGINT, shutdown_clean);
	install_handler(SIGTERM, shutdown_clean);
	install_handler(SIGQUIT, shutdown_clean);
	install_handler(SIGALRM, shutdown_clean);

	if(!preferred_hostname) {
		domain_name_cache_guess(hostname);
		preferred_hostname = hostname;
	}

	username_get(owner);
	starttime = time(0);

	table = nvpair_database_create(history_dir);
	if(!table)
		fatal("couldn't create directory %s: %s\n",history_dir,strerror(errno));

	list_port = link_serve(port);
	if(list_port) {
		/*
		If a port was chosen automatically, read it back
		so that the same one can be used for the update port.
		There is the possibility that the UDP listen will
		fail because that port is in use.
		*/
	  
		if(port==0) {
			char addr[LINK_ADDRESS_MAX];
			link_address_local(list_port,addr,&port);
		}
	} else {
		fatal("couldn't listen on tcp port %d", port);
	}

	outgoing_dgram = datagram_create(0);
	if(!outgoing_dgram)
		fatal("couldn't create outgoing udp port");

	update_dgram = datagram_create(port);
	if(!update_dgram)
		fatal("couldn't listen on udp port %d", port);

	opts_write_port_file(port_file,port);

	while(1) {
		fd_set rfds;
		int ufd = datagram_fd(update_dgram);
		int lfd = link_fd(list_port);
		int result, maxfd;
		struct timeval timeout;

		remove_expired_records();

		if(time(0) > outgoing_alarm) {
			update_all_catalogs(outgoing_dgram);
			outgoing_alarm = time(0) + outgoing_timeout;
		}

		while(1) {
			int status;
			pid_t pid = waitpid(-1, &status, WNOHANG);
			if(pid>0) {
				child_procs_count--;
				continue;
			} else {
				break;
			}
		}

		FD_ZERO(&rfds);
		FD_SET(ufd, &rfds);
		if(child_procs_count < child_procs_max) {
			FD_SET(lfd, &rfds);
		}
		maxfd = MAX(ufd, lfd) + 1;

		timeout.tv_sec = 5;
		timeout.tv_usec = 0;

		result = select(maxfd, &rfds, 0, 0, &timeout);
		if(result <= 0)
			continue;

		if(FD_ISSET(ufd, &rfds)) {
			handle_updates(update_dgram);
		}

		if(FD_ISSET(lfd, &rfds)) {
			link = link_accept(list_port, time(0) + 5);
			if(link) {
				if(fork_mode) {
					pid_t pid = fork();
					if(pid == 0) {
						alarm(child_procs_timeout);
						handle_query(link);
						_exit(0);
					} else if (pid>0) {
						child_procs_count++;
					}
				} else {
					handle_query(link);
				}
				link_close(link);
			}
		}
	}

	return 1;
}
