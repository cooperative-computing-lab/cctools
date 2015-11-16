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

const char *enforcer_script_pattern = "./enforcer_%d";
const char *mountlist_pattern = "mount_%d";
const char *tmp_pattern = "tmp_%d";
const char *vartmp_pattern = "vartmp_%d";
const char *shm_pattern = "shm_%d";

void makeflow_wrapper_enforcer_init(struct makeflow_wrapper *enforcer, char *parrot_path) {
	struct stat stat_buf;
	const char *local_parrot_path = "./parrot_run";
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

	makeflow_wrapper_add_input_file(enforcer, local_parrot_path);
}

struct list *makeflow_enforcer_generate_files( struct list *result, struct list *input, struct dag_node *n, struct makeflow_wrapper *w) {
	struct list *extra_files;
	if (input->size > 0) {
		/* Only add input files */
		extra_files = list_create();
		list_push_tail(extra_files, string_format(enforcer_script_pattern, n->nodeid));
		list_push_tail(extra_files, string_format(mountlist_pattern, n->nodeid));
		list_push_tail(extra_files, string_format(tmp_pattern, n->nodeid));
		list_push_tail(extra_files, string_format(vartmp_pattern, n->nodeid));
		list_push_tail(extra_files, string_format(shm_pattern, n->nodeid));
		result = makeflow_wrapper_generate_files(result, extra_files, n, w);
	}
	return makeflow_wrapper_generate_files(result, input, n, w);
}

char *makeflow_wrap_enforcer( char *result, struct dag_node *n, struct makeflow_wrapper *w, struct list *input_list, struct list *output_list )
{
	if(!w) return result;

	struct dag_file *f;
	FILE *enforcer_script;
	char *enforcer_script_path = string_format(enforcer_script_pattern, n->nodeid);
	char *mountlist_path = string_format(mountlist_pattern, n->nodeid);
	char *tmp_path = string_format(tmp_pattern, n->nodeid);
	char *vartmp_path = string_format(vartmp_pattern, n->nodeid);
	char *shm_path = string_format(shm_pattern, n->nodeid);

	/* Create local directories to bind to /tmp, /var/tmp, and /dev/shm */
	if (mkdir(tmp_path, S_IRWXU) || mkdir(vartmp_path, S_IRWXU) || mkdir(shm_path, S_IRWXU)) {
		fatal("could not create temp directory: %s", strerror(errno));
	}

	/* make an invalid mountfile to send */
	int mountlist_fd = open(mountlist_path, O_WRONLY|O_CREAT|O_EXCL, S_IRUSR|S_IWUSR);
	if (mountlist_fd == -1) {
		fatal("could not create `%s': %s", mountlist_path, strerror(errno));
	}
	write(mountlist_fd, "mountlist\n", 10);
	close(mountlist_fd);

	/* and generate a wrapper script with the current nodeid */
	int enforcer_script_fd = open(enforcer_script_path, O_WRONLY|O_CREAT|O_EXCL, S_IRWXU);
	if (enforcer_script_fd == -1 || (enforcer_script = fdopen(enforcer_script_fd, "w")) == NULL) {
		fatal("could not create `%s': %s", enforcer_script_path, strerror(errno));
	}
	fchmod(enforcer_script_fd, 0755);
	fprintf(enforcer_script, "#!/bin/sh\n\n");
	fprintf(enforcer_script, "MOUNTFILE='%s'\n", mountlist_path);
	fprintf(enforcer_script, "cat > \"$MOUNTFILE\" <<EOF\n");
	fprintf(enforcer_script, "/\t\trx\n");
	fprintf(enforcer_script, "/tmp\t$PWD/%s\trwx\n", tmp_path);
	fprintf(enforcer_script, "/var/tmp\t$PWD/%s\trwx\n", vartmp_path);
	fprintf(enforcer_script, "/dev/$PWD/shm\t%s\trwx\n", shm_path);
	fprintf(enforcer_script, "/dev/null\trwx\n");
	fprintf(enforcer_script, "/dev/zero\trwx\n");
	fprintf(enforcer_script, "/dev/full\trwx\n");
	fprintf(enforcer_script, "/dev/random\trwx\n");
	fprintf(enforcer_script, "/dev/urandom\trwx\n");
	fprintf(enforcer_script, "/home\t\tDENY\n");
	list_first_item(input_list);
	while((f=list_next_item(input_list))) {
		fprintf(enforcer_script, "$PWD/%s\trwx\n", f->filename);
	}
	list_first_item(output_list);
	while((f=list_next_item(output_list))) {
		fprintf(enforcer_script, "$PWD/%s\trwx\n", f->filename);
	}
	fprintf(enforcer_script, "EOF\n\n");
	fprintf(enforcer_script, "./parrot_run -m \"$MOUNTFILE\" -- \"$@\"\n");
	fprintf(enforcer_script, "exit $?\n");
	fclose(enforcer_script);

	w->command = enforcer_script_path;

	free(mountlist_path);

	return makeflow_wrap_wrapper(result, n, w);
}
