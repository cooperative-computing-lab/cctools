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
#include "makeflow_wrapper_sandbox.h"

#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

void makeflow_wrapper_sandbox_init(struct makeflow_wrapper *sandbox, char *parrot_path) {
	struct stat stat_buf;
	const char *local_parrot_path = "./parrot_run";
	int local_parrot = open(local_parrot_path, O_WRONLY|O_CREAT|O_EXCL);
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

	makeflow_wrapper_add_input_file(sandbox, local_parrot_path);
}

char *makeflow_wrap_sandbox( char *result, struct dag_node *n, struct makeflow_wrapper *w, struct list *input_list, struct list *output_list )
{
	if(!w) return result;

	struct dag_file *f;
	FILE *sandbox_script;
	char *sandbox_script_path = xxstrdup("./sandbox_XXXXXX");
	int sandbox_script_fd =  mkstemp(sandbox_script_path);
	if (sandbox_script_fd == -1 || (sandbox_script = fdopen(sandbox_script_fd, "w")) == NULL) {
		fatal("could not create `%s': %s", sandbox_script_path, strerror(errno));
	}
	fchmod(sandbox_script_fd, 0755);
	fprintf(sandbox_script, "#!/bin/sh\n\n");
	fprintf(sandbox_script, "MOUNTFILE=`mktemp mount_XXXXXX`\n");
	fprintf(sandbox_script, "cat > $MOUNTFILE <<EOF\n");
	fprintf(sandbox_script, "/\t\trx\n");
	fprintf(sandbox_script, "/dev/null\trwx\n");
	fprintf(sandbox_script, "/dev/zero\trwx\n");
	fprintf(sandbox_script, "/dev/full\trwx\n");
	fprintf(sandbox_script, "/dev/random\trwx\n");
	fprintf(sandbox_script, "/dev/urandom\trwx\n");
	fprintf(sandbox_script, "/home\t\tDENY\n");
	list_first_item(input_list);
	while((f=list_next_item(input_list))) {
		fprintf(sandbox_script, "$PWD/%s\trwx\n", f->filename);
	}
	list_first_item(output_list);
	while((f=list_next_item(output_list))) {
		fprintf(sandbox_script, "$PWD/%s\trwx\n", f->filename);
	}
	fprintf(sandbox_script, "EOF\n\n");
	fprintf(sandbox_script, "./parrot_run -m $MOUNTFILE -- \"$@\"\n");
	fprintf(sandbox_script, "RC=$?\n");
	fprintf(sandbox_script, "rm -f $MOUNTFILE\n");
	fprintf(sandbox_script, "exit $RC\n");
	fclose(sandbox_script);

	list_push_tail(input_list, sandbox_script_path);

	w->command = sandbox_script_path;

	return makeflow_wrap_wrapper(result, n, w);
}
