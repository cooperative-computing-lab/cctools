/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_server.h"
#include "datagram.h"
#include "link.h"
#include "hash_cache.h"
#include "debug.h"
#include "getopt.h"
#include "nvpair.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "username.h"
#include "list.h"
#include "xmalloc.h"
#include "macros.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef HAS_ALLOCA_H
#include <alloca.h>
#endif
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/select.h>

#ifndef LINE_MAX
#define LINE_MAX 1024
#endif

#define MAX_TABLE_SIZE 10000

/* The table of nvpairs, hashed on address:port */
static struct hash_cache *table = 0;

/* An array of nvpais used to sort for display */
static struct nvpair *array[MAX_TABLE_SIZE];

/* The time for which updated data lives before automatic deletion */
static int lifetime = 1800;

/* The port upon which to listen. */
static int port = CATALOG_PORT_DEFAULT;

/* This machine's canonical name. */
static char hostname[DOMAIN_NAME_MAX];

/* This process's owner */
static char owner[USERNAME_MAX];

/* Time when the process was started. */
static time_t starttime;

/* If true, for for every query */
static int fork_mode = 1;

/* The maximum size of a server that will actually be believed. */
static INT64_T max_server_size = 0;

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

void reap_child(int sig)
{
	pid_t pid;
	int status;

	do {
		pid = waitpid(-1, &status, WNOHANG);
	} while(pid > 0);
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

int update_one_catalog(void *outgoing_host, const void *text)
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
	unsigned uptime;
	int length;

	uptime = time(0) - starttime;

	length = sprintf(text, "type catalog\nversion %d.%d.%d\nurl http://%s:%d\nname %s\nowner %s\nuptime %u\nport %d\n", CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, hostname, port, hostname, owner, uptime, port);

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
	int timeout;
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
				nvpair_insert_integer(nv, "total", 0);
				nvpair_insert_integer(nv, "avail", 0);
			}
		}

		/* Do not believe the server's reported name, just resolve it backwards. */

		char name[DOMAIN_NAME_MAX];
		if(domain_name_cache_lookup_reverse(addr, name)) {
			nvpair_insert_string(nv, "name", name);
		} else {
			nvpair_insert_string(nv, "name", addr);
		}

		timeout = nvpair_lookup_integer(nv, "lifetime");
		if(!timeout)
			timeout = lifetime;
		timeout = MIN(timeout, lifetime);

		make_hash_key(nv, key);
		hash_cache_insert(table, key, nv, timeout);

		debug(D_DEBUG, "received udp update from %s", key);
	}
}

static struct nvpair_header html_headers[] = {
	{"type", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{"name", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{"port", NVPAIR_MODE_INTEGER, NVPAIR_ALIGN_LEFT, 0},
	{"owner", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{"total", NVPAIR_MODE_METRIC, NVPAIR_ALIGN_RIGHT, 0},
	{"avail", NVPAIR_MODE_METRIC, NVPAIR_ALIGN_RIGHT, 0},
	{"load5", NVPAIR_MODE_STRING, NVPAIR_ALIGN_RIGHT, 0},
	{"version", NVPAIR_MODE_STRING, NVPAIR_ALIGN_LEFT, 0},
	{0,}
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

	link_nonblocking(query_link, 0);
	stream = fdopen(link_fd(query_link), "r+");
	if(!stream)
		return;

	if(!fgets(line, sizeof(line), stream))
		return;
	string_chomp(line);
	if(sscanf(line, "%s %s %s", action, url, version) != 3)
		return;

	while(1) {
		if(!fgets(line, sizeof(line), stream))
			return;
		if(line[0] == '\n' || line[0] == '\r')
			break;
	}

	current = time(0);
	fprintf(stream, "HTTP/1.1 200 OK\n");
	fprintf(stream, "Date: %s", ctime(&current));
	fprintf(stream, "Server: catalog_server\n");
	fprintf(stream, "Connection: close\n");

	if(sscanf(url, "http://%[^/]%s", hostport, path) == 2) {
		// continue on
	} else {
		strcpy(path, url);
	}

	/* load the hash table entries into one big array */

	n = 0;
	hash_cache_firstkey(table);
	while(hash_cache_nextkey(table, &hkey, (void **) &nv)) {
		array[n] = nv;
		n++;
	}

	/* sort the array by name before displaying */

	qsort(array, n, sizeof(struct nvpair *), compare_nvpair);

	if(!strcmp(path, "/query.text")) {
		fprintf(stream, "Content-type: text/plain\n\n");
		for(i = 0; i < n; i++)
			nvpair_print_text(array[i], stream);
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
		nv = hash_cache_lookup(table, key);
		if(nv) {
			const char *name = nvpair_lookup_string(nv, "name");
			if(!name)
				name = "unknown";
			fprintf(stream, "<title>%s storage catalog: %s</title>\n", hostname, name);
			fprintf(stream, "<center>\n");
			fprintf(stream, "<h1>%s storage catalog</h1>\n", hostname);
			fprintf(stream, "<h2>%s</h2>\n", name);
			fprintf(stream, "<p><a href=/>return to catalog view</a><p>\n");
			nvpair_print_html_solo(nv, stream);
			fprintf(stream, "</center>\n");
		} else {
			fprintf(stream, "<title>%s storage catalog</title>\n", hostname);
			fprintf(stream, "<center>\n");
			fprintf(stream, "<h1>%s storage catalog</h1>\n", hostname);
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
		fprintf(stream, "<title>%s storage catalog</title>\n", hostname);
		fprintf(stream, "<center>\n");
		fprintf(stream, "<h1>%s storage catalog</h1>\n", hostname);
		fprintf(stream, "<a href=/query.text>text</a> - ");
		fprintf(stream, "<a href=/query.html>html</a> - ");
		fprintf(stream, "<a href=/query.xml>xml</a> - ");
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

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options]\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number to listen on (default is %d)\n", port);
	printf(" -l <secs>      Lifetime of data, in seconds (default is %d)\n", lifetime);
	printf(" -d <subsystem> Enable debugging for this subsystem\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -O <bytes>     Rotate debug file once it reaches this size.\n");
	printf(" -u <host>      Send status updates to this host. (default is %s)\n", CATALOG_HOST_DEFAULT);
	printf(" -M <size>      Maximum size of a server to be believed.  (default is any)\n");
	printf(" -U <time>      Send status updates at this interval. (default is 5m)\n");
	printf(" -S             Single process mode; do not work on queries.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

int main(int argc, char *argv[])
{
	struct link *link, *list_port = 0;
	char ch;
	time_t current;

	outgoing_host_list = list_create();

	debug_config(argv[0]);

	while((ch = getopt(argc, argv, "p:l:M:d:o:O:u:U:Shv")) != (char) -1) {
		switch (ch) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'M':
			max_server_size = string_metric_parse(optarg);
			break;
		case 'p':
			port = atoi(optarg);
			break;
		case 'o':
			debug_config_file(optarg);
			break;
		case 'O':
			debug_config_file_size(string_metric_parse(optarg));
			break;
		case 'u':
			list_push_head(outgoing_host_list, xstrdup(optarg));
			break;
		case 'U':
			outgoing_timeout = string_time_parse(optarg);
			break;
		case 'l':
			lifetime = string_time_parse(optarg);
			break;
		case 'S':
			fork_mode = 0;
			break;
		case 'v':
			show_version(argv[0]);
			return 0;
		case 'h':
		default:
			show_help(argv[0]);
			return 1;
		}
	}

	current = time(0);
	debug(D_ALL, "*** %s starting at %s", argv[0], ctime(&current));

	if(!list_size(outgoing_host_list)) {
		list_push_head(outgoing_host_list, CATALOG_HOST_DEFAULT);
	}

	install_handler(SIGPIPE, ignore_signal);
	install_handler(SIGHUP, ignore_signal);
	install_handler(SIGCHLD, reap_child);
	install_handler(SIGINT, shutdown_clean);
	install_handler(SIGTERM, shutdown_clean);
	install_handler(SIGQUIT, shutdown_clean);

	domain_name_cache_guess(hostname);
	username_get(owner);
	starttime = time(0);

	table = hash_cache_create(127, hash_string, (hash_cache_cleanup_t) nvpair_delete);
	if(!table)
		fatal("couldn't make hash table");

	update_dgram = datagram_create(port);
	if(!update_dgram)
		fatal("couldn't listen on udp port %d", port);

	outgoing_dgram = datagram_create(0);
	if(!outgoing_dgram)
		fatal("couldn't create outgoing udp port");

	list_port = link_serve(port);
	if(!list_port)
		fatal("couldn't listen on tcp port %d", port);

	while(1) {
		fd_set rfds;
		int ufd = datagram_fd(update_dgram);
		int lfd = link_fd(list_port);
		int result, maxfd;
		struct timeval timeout;

		if(time(0) > outgoing_alarm) {
			update_all_catalogs(outgoing_dgram);
			outgoing_alarm = time(0) + outgoing_timeout;
		}

		FD_ZERO(&rfds);
		FD_SET(ufd, &rfds);
		FD_SET(lfd, &rfds);
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
						handle_query(link);
						exit(0);
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
