
#include "vine_workspace.h"
#include "vine_protocol.h"
#include "vine_worker.h"
#include "vine_worker_options.h"

#include "create_dir.h"
#include "debug.h"
#include "envtools.h"
#include "path.h"
#include "stringtools.h"
#include "trash.h"
#include "unlink_recursive.h"
#include "xxmalloc.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#if defined(CCTOOLS_OPSYS_FREEBSD)
#include <sys/wait.h>
#endif

/* Create a new workspace object and sub-paths */

struct vine_workspace *vine_workspace_create(const char *manual_workspace_dir)
{
	char *workspace_dir = 0;
	char absolute[VINE_LINE_MAX];

	if (manual_workspace_dir) {
		workspace_dir = strdup(manual_workspace_dir);
	} else {
		const char *tmpdir = system_tmp_dir(0);
		workspace_dir = string_format("%s/worker-%d-%d", tmpdir, (int)getuid(), (int)getpid());
	}

	printf("vine_worker: creating workspace %s\n", workspace_dir);
	if (!create_dir(workspace_dir, 0777)) {
		free(workspace_dir);
		return 0;
	}

	/* Convert to absolute dir since we will eventually chdir. */
	path_absolute(workspace_dir, absolute, 1);
	free(workspace_dir);

	struct vine_workspace *w = malloc(sizeof(*w));

	w->workspace_dir = xxstrdup(absolute);
	w->cache_dir = string_format("%s/cache", w->workspace_dir);
	w->transfer_dir = string_format("%s/transfer", w->workspace_dir);
	w->temp_dir = string_format("%s/temp", w->workspace_dir);
	w->trash_dir = string_format("%s/trash", w->workspace_dir);

	return w;
}

/* Check that the workspace is actually writable and executable. */

int vine_workspace_check(struct vine_workspace *w)
{
	int error = 0; /* set 1 on error */
	char *fname = string_format("%s/test.sh", w->workspace_dir);

	FILE *file = fopen(fname, "w");
	if (!file) {
		warn(D_NOTICE, "Could not write to %s", w->workspace_dir);
		error = 1;
	} else {
		fprintf(file, "#!/bin/sh\nexit 0\n");
		fclose(file);
		chmod(fname, 0755);

		int exit_status = system(fname);

		if (WIFEXITED(exit_status) && WEXITSTATUS(exit_status) == 126) {
			/* Note that we do not set status=1 on 126, as the executables may live ouside workspace. */
			warn(D_NOTICE,
					"Could not execute a test script in the workspace directory '%s'.",
					w->workspace_dir);
			warn(D_NOTICE, "Is the filesystem mounted as 'noexec'?\n");
			warn(D_NOTICE, "Unless the task command is an absolute path, the task will fail with exit status 126.\n");
		} else if (!WIFEXITED(exit_status) || WEXITSTATUS(exit_status) != 0) {
			error = 1;
		}
	}

	/* do not use trash here; workspace has not been set up yet */
	unlink(fname);
	free(fname);

	if (error) {
		warn(D_NOTICE, "The workspace %s could not be used.\n", w->workspace_dir);
		warn(D_NOTICE, "Use the --workdir command line switch to change where the workspace is created.\n");
	}

	return !error;
}

/* Prepare the workspace prior to working with a manager. */

int vine_workspace_prepare(struct vine_workspace *w)
{
	debug(D_VINE, "preparing workspace %s", w->workspace_dir);

	if (!create_dir(w->cache_dir, 0777)) {
		debug(D_VINE, "couldn't create %s: %s", w->cache_dir, strerror(errno));
		return 0;
	}

	if (!create_dir(w->transfer_dir, 0777)) {
		debug(D_VINE, "couldn't create %s: %s", w->transfer_dir, strerror(errno));
		return 0;
	}

	if (!create_dir(w->temp_dir, 0777)) {
		debug(D_VINE, "couldn't create %s: %s", w->cache_dir, strerror(errno));
		return 0;
	}

	setenv("WORKER_TMPDIR", w->temp_dir, 1);

	trash_setup(w->trash_dir);

	return 1;
}

/* Cleanup task directories when disconnecting from a given manager. */

int vine_workspace_cleanup(struct vine_workspace *w)
{
	debug(D_VINE, "cleaning workspace %s", w->workspace_dir);

	DIR *dir = opendir(w->workspace_dir);
	if (dir) {
		struct dirent *d;
		while ((d = readdir(dir))) {
			if (!strcmp(d->d_name, "."))
				continue;
			if (!strcmp(d->d_name, ".."))
				continue;
			if (!strcmp(d->d_name, "trash"))
				continue;
			if (!strcmp(d->d_name, "cache"))
				continue;

			/* Anything not matching gets moved into the trash. */
			trash_file(d->d_name);
		}
		closedir(dir);
	}

	/* Now remove everything in the trash. */
	trash_empty();

	return 1;
}

/* Remove the entire workspace recursively when the worker exits. */

void vine_workspace_delete(struct vine_workspace *w)
{
	if (options->keep_workspace_at_exit) {
		printf("vine_worker: keeping workspace %s for future use...\n", w->workspace_dir);
	} else {
		printf("vine_worker: deleting workspace %s\n", w->workspace_dir);
		unlink_recursive(w->workspace_dir);
	}

	free(w->workspace_dir);
	free(w->transfer_dir);
	free(w->cache_dir);
	free(w->trash_dir);
	free(w->temp_dir);
	free(w);
}
