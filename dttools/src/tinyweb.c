/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "link.h"
#include "debug.h"
#include "getopt.h"
#include "stringtools.h"
#include "domain_name_cache.h"
#include "list.h"
#include "xmalloc.h"
#include "macros.h"
#include "copy_stream.h"
#include "hash_table.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <ctype.h>

#ifndef LINE_MAX
#define LINE_MAX 1024
#endif

/* The port upon which to listen. */
static int port = 8080;

/* If true, for for every query */
static int fork_mode = 1;

/* The root directory for documents */
static const char *rootpath;

/* The table of mime types */
static struct hash_table *mime_table = 0;

int contains_evil( const char *str )
{
	if(strpbrk(str,"\"\'`~!@#$%^&*()[]\\{}|;:,<>? \t\n")) return 1;
	if(strstr(str,"..")) return 1;
	return 0;
}

void load_mime_types()
{
	char *suffix, *mtype;
	FILE *file;
	char line[LINE_MAX];

	mime_table = hash_table_create(0,0);

	file = fopen("/etc/mime.types","r");
	if(!file) return;

	while(fgets(line,sizeof(line),file)) {
		if(line[0]=='#') continue;
		if(isspace(line[0])) continue;

		mtype = strtok(line," \t\n");
		if(!mtype) continue;

		while((suffix=strtok(0," \t\n"))) {
			hash_table_insert(mime_table,suffix,strdup(mtype));
		}
	}

	fclose(file);
}

const char *content_type_from_path( const char *path )
{
	char *suffix;
	char *ctype = 0;

	suffix = strrchr(path,'.');
	if(suffix) {
		suffix++;
		ctype = hash_table_lookup(mime_table,suffix);
	}

	if(!ctype) ctype = "application/octet-stream";

	return ctype;
}

void send_http_response( FILE *stream, int code, const char *text, const char *content_type )
{
	time_t current = time(0);

	debug(D_HTTP,"response: %d %s",code,text);

	fprintf(stream,"HTTP/1.0 %d %s\n",code,text);
	fprintf(stream,"Date: %s", ctime(&current));
	fprintf(stream,"Server: tinyweb\n");
	fprintf(stream,"Connection: close\n");
	if(content_type) {
		fprintf(stream,"Content-type: %s\n\n",content_type);
	}
}

static void handle_http_query(struct link *query_link)
{
	FILE *stream;
	char line[LINE_MAX];
	char url[LINE_MAX];
	char shortpath[LINE_MAX];
	char fullpath[LINE_MAX];
	char action[LINE_MAX];
	char version[LINE_MAX];
	char hostport[LINE_MAX];

	link_nonblocking(query_link, 0);
	stream = fdopen(link_fd(query_link), "r+");
	if(!stream) return;

	if(!fgets(line, sizeof(line), stream)) return;
	string_chomp(line);

	debug(D_HTTP,"request: %s",line);

	if(sscanf(line, "%s %s %s", action, url, version) != 3) return;

	while(1) {
		if(!fgets(line, sizeof(line), stream))
			return;
		if(line[0] == '\n' || line[0] == '\r')
			break;
	}

	if(sscanf(url, "http://%[^/]%s", hostport, shortpath) == 2) {
		// continue on
	} else {
		strcpy(shortpath, url);
	}

	sprintf(fullpath,"%s/%s",rootpath,shortpath);

	if(contains_evil(fullpath)) {
		send_http_response(stream,403,"Permission Denied",0);
	} else {
		FILE *file=0;
		const char *content_type = 0;
		struct stat info;

		retry:

		if(stat(fullpath,&info)==0) {
			if(S_ISREG(info.st_mode)) {
				if(!strcmp(string_back(fullpath,4),".cgi")) {
					file = popen(fullpath,"r");
				} else {
					file = fopen(fullpath,"r");
					content_type = content_type_from_path(fullpath);
				}

				if(file) {
					send_http_response(stream,200,"OK",content_type);
					copy_stream_to_stream(file,stream);
					if(content_type) {
						fclose(file);
					} else {
						pclose(file);
					}
				} else {
					send_http_response(stream,403,"Permission Denied",0);
				}
			} else if(S_ISDIR(info.st_mode)) {
				strcat(fullpath,"/index.html");
				goto retry;
			} else {
				send_http_response(stream,403,"Permission Denied",0);
			}
		} else {
			send_http_response(stream,404,"File Not Found",0);
		}
	}

	fclose(stream);
}

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

static void show_version(const char *cmd)
{
	printf("%s version %d.%d.%d built by %s@%s on %s at %s\n", cmd, CCTOOLS_VERSION_MAJOR, CCTOOLS_VERSION_MINOR, CCTOOLS_VERSION_MICRO, BUILD_USER, BUILD_HOST, __DATE__, __TIME__);
}

static void show_help(const char *cmd)
{
	printf("Use: %s [options]\n", cmd);
	printf("where options are:\n");
	printf(" -p <port>      Port number to listen on (default is %d)\n", port);
	printf(" -r <rootpath>  Root of files to serve\n");
	printf(" -d <subsystem> Enable debugging for this subsystem\n");
	printf(" -o <file>      Send debugging to this file.\n");
	printf(" -O <bytes>     Rotate debug file once it reaches this size.\n");
	printf(" -U <time>      Send status updates at this interval. (default is 5m)\n");
	printf(" -S             Single process mode; do not fork on queries.\n");
	printf(" -v             Show version string\n");
	printf(" -h             Show this help screen\n");
}

int main(int argc, char *argv[])
{
	struct link *link, *list_port = 0;
	char ch;
	time_t current;

	debug_config(argv[0]);

	while((ch = getopt(argc, argv, "r:p:d:o:O:Shv")) != (char) -1) {
		switch (ch) {
		case 'd':
			debug_flags_set(optarg);
			break;
		case 'r':
			rootpath = optarg;
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

	if(!rootpath) {
		fprintf(stderr,"%s: you must specify a web root with -r!\n",argv[0]);
		return 1;
	}

	if(chdir(rootpath)!=0) {
		fprintf(stderr,"%s: %s: %s\n",argv[0],rootpath,strerror(errno));
		return 1;
	}

	current = time(0);
	debug(D_ALL, "*** %s starting at %s", argv[0], ctime(&current));

	load_mime_types();

	install_handler(SIGPIPE, ignore_signal);
	install_handler(SIGHUP, ignore_signal);
	install_handler(SIGCHLD, reap_child);
	install_handler(SIGINT, shutdown_clean);
	install_handler(SIGTERM, shutdown_clean);
	install_handler(SIGQUIT, shutdown_clean);

	list_port = link_serve(port);
	if(!list_port)
		fatal("couldn't listen on tcp port %d", port);

	while(1) {
		link = link_accept(list_port, time(0) + 5);
		if(link) {
			if(fork_mode) {
				pid_t pid = fork();
				if(pid == 0) {
					handle_http_query(link);
					exit(0);
				}
			} else {
				handle_http_query(link);
			}
			link_close(link);
		}
	}

	return 1;
}
