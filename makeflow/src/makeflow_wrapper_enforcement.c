/*
 * Copyright (C) 2015- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 * */

#include "stringtools.h"
#include "xxmalloc.h"
#include "copy_stream.h"
#include "debug.h"

#include "list.h"
#include "dag.h"
#include "makeflow_wrapper.h"
#include "makeflow_wrapper_enforcement.h"

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#define enforcer_pattern "enforcer_"
#define mountlist_pattern "mount_"
#define tmp_pattern "tmp_"
#define vartmp_pattern "vartmp_"
#define shm_pattern "shm_"
#define local_parrot_path "parrot_run"

void makeflow_log_file_expectation( struct dag *d, struct list *file_list );
void makeflow_log_file_existence( struct dag *d, struct list *file_list );


void makeflow_wrapper_enforcer_init(struct makeflow_wrapper *w, char *parrot_path) {
	struct stat stat_buf;
	int local_parrot = open(local_parrot_path, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
	int host_parrot = open(parrot_path, O_RDONLY);
	if (host_parrot == -1) {
		fatal("could not open parrot at `%s': %s", parrot_path, strerror(errno));
	}
	fstat(host_parrot, &stat_buf);
	if (!(stat_buf.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH))) {
		fatal("%s is not executable", parrot_path);
	}
	if (local_parrot == -1) {
		if (errno != EEXIST) fatal("could not create local copy of parrot: %s", strerror(errno));
		/* parrot_run is already in the current directory, so we'll just use that */
	} else {
		fchmod(local_parrot, 0755);
		if (copy_fd_to_fd(host_parrot, local_parrot) != stat_buf.st_size) {
			fatal("could not copy parrot: %s");
		}
	}
	close(local_parrot);
	close(host_parrot);

	makeflow_wrapper_add_input_file(w, local_parrot_path);
	makeflow_wrapper_add_input_file(w, xxstrdup(enforcer_pattern "%%"));
	makeflow_wrapper_add_input_file(w, xxstrdup(mountlist_pattern "%%"));
	makeflow_wrapper_add_input_file(w, xxstrdup(tmp_pattern "%%"));
	makeflow_wrapper_add_input_file(w, xxstrdup(vartmp_pattern "%%"));
	makeflow_wrapper_add_input_file(w, xxstrdup(shm_pattern "%%"));
	w->command = xxstrdup("./" enforcer_pattern "%%");
}

char *makeflow_wrap_enforcer( char *result, struct dag_node *n, struct makeflow_wrapper *w, struct list *input_list, struct list *output_list )
{
	if(!w) return result;

	struct list *enforcer_paths;
	struct dag_file *f;
	FILE *enforcer;
	char *enforcer_path = string_format(enforcer_pattern "%d", n->nodeid);
	char *mountlist_path = string_format(mountlist_pattern "%d", n->nodeid);
	char *tmp_path = string_format(tmp_pattern "%d", n->nodeid);
	char *vartmp_path = string_format(vartmp_pattern "%d", n->nodeid);
	char *shm_path = string_format(shm_pattern "%d", n->nodeid);

	enforcer_paths = list_create();
	list_push_tail(enforcer_paths, dag_file_lookup_or_create(n->d, tmp_path));
	list_push_tail(enforcer_paths, dag_file_lookup_or_create(n->d, vartmp_path));
	list_push_tail(enforcer_paths, dag_file_lookup_or_create(n->d, shm_path));
	makeflow_log_file_expectation(n->d, enforcer_paths);

	/* Create local directories to bind to /tmp, /var/tmp, and /dev/shm */
	if (mkdir(tmp_path, S_IRWXU) || mkdir(vartmp_path, S_IRWXU) || mkdir(shm_path, S_IRWXU)) {
		fatal("could not create temp directory: %s", strerror(errno));
	}

	makeflow_log_file_existence(n->d, enforcer_paths);
	list_delete(enforcer_paths);

	enforcer_paths = list_create();
	list_push_tail(enforcer_paths, dag_file_lookup_or_create(n->d, mountlist_path));
	makeflow_log_file_expectation(n->d, enforcer_paths);

	/* make an invalid mountfile to send */
	int mountlist_fd = open(mountlist_path, O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
	if (mountlist_fd == -1) {
		fatal("could not create `%s': %s", mountlist_path, strerror(errno));
	}
	write(mountlist_fd, "mountlist\n", 10);
	close(mountlist_fd);

	makeflow_log_file_existence(n->d, enforcer_paths);
	list_delete(enforcer_paths);

	enforcer_paths = list_create();
	list_push_tail(enforcer_paths, dag_file_lookup_or_create(n->d, enforcer_path));
	makeflow_log_file_expectation(n->d, enforcer_paths);

	/* and generate a wrapper script with the current nodeid */
	int enforcer_fd = open(enforcer_path, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
	if (enforcer_fd == -1 || (enforcer = fdopen(enforcer_fd, "w")) == NULL) {
		fatal("could not create `%s': %s", enforcer_path, strerror(errno));
	}
	fchmod(enforcer_fd, 0755);
	fprintf(enforcer, "#!/bin/sh\n\n");
	fprintf(enforcer, "MOUNTFILE='%s'\n", mountlist_path);
	fprintf(enforcer, "cat > \"$MOUNTFILE\" <<EOF\n");
	fprintf(enforcer, "/\t\trx\n");
	fprintf(enforcer, "/tmp\t$PWD/%s\trwx\n", tmp_path);
	fprintf(enforcer, "/var/tmp\t$PWD/%s\trwx\n", vartmp_path);
	fprintf(enforcer, "/dev/shm\t$PWD/%s\trwx\n", shm_path);
	fprintf(enforcer, "/dev/null\trwx\n");
	fprintf(enforcer, "/dev/zero\trwx\n");
	fprintf(enforcer, "/dev/full\trwx\n");
	fprintf(enforcer, "/dev/random\trwx\n");
	fprintf(enforcer, "/dev/urandom\trwx\n");
	fprintf(enforcer, "/home\t\tDENY\n");

	/* We have some X related exceptions in case someone needs to
	 * do some troubleshooting/configuration graphically
	 */
	fprintf(enforcer, "$HOME/.Xauthority\trwx\n");
	fprintf(enforcer, "/tmp/.X11-unix\trwx\n");

	list_first_item(input_list);
	while((f=list_next_item(input_list))) {
		fprintf(enforcer, "$PWD/%s\trwx\n", f->filename);
	}
	list_first_item(output_list);
	while((f=list_next_item(output_list))) {
		fprintf(enforcer, "$PWD/%s\trwx\n", f->filename);
	}
	fprintf(enforcer, "EOF\n\n");
	fprintf(enforcer, "./parrot_run -m \"$MOUNTFILE\" -- \"$@\"\n");
	fprintf(enforcer, "exit $?\n");
	fclose(enforcer);

	makeflow_log_file_existence(n->d, enforcer_paths);
	list_delete(enforcer_paths);

	free(enforcer_path);
	free(mountlist_path);
	free(tmp_path);
	free(vartmp_path);
	free(shm_path);

	return makeflow_wrap_wrapper(result, n, w);
}
