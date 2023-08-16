/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef BATCH_WRAPPER_H
#define BATCH_WRAPPER_H

#include "batch_task.h"

/** Create a builder for a batch wrapper.
 * Use batch_wrapper_pre, batch_wrapper_cmd, etc. to add
 * commands to the wrapper. These exist only in memory until
 * calling batch_wrapper_write. Each command must exit successfully
 * to continue executing the wrapper. A wrapper may only have a single
 * main command (args, argv, or cmd).
 */
struct batch_wrapper *batch_wrapper_create(void);

/** Free a batch_wrapper.
 * Any scripts written out will continue to work after
 * calling this function.
 */
void batch_wrapper_delete(struct batch_wrapper *w);

/** Add a shell command to the batch wrapper.
 * Can be called multiple times to append multiple commands.
 * These commands run before the main wrapper command.
 * Each command must be a self-contained shell statement.
 * @param cmd The shell command to add.
 */
void batch_wrapper_pre(struct batch_wrapper *w, const char *cmd);

/** Specify a command line to execute in the wrapper.
 * The arguments in argv are executed as-is, with no shell interpretation.
 * This command executes after any pre commands.
 * It is undefined behavior to add another command after calling this.
 * @param argv The command line to run.
 */
void batch_wrapper_argv(struct batch_wrapper *w, char *const argv[]);

/** Specify a command line to execute with shell interpretation.
 * Same as batch_wrapper_argv, but each arg is individually
 * interpreted by the shell for variable substitution and such.
 * @param args The command line to run.
 */
void batch_wrapper_args(struct batch_wrapper *w, char *const args[]);

/** Specify a shell command to execute.
 * Same as batch_wrapper_argv, but takes a shell command.
 * @param cmd The command line to run.
 */
void batch_wrapper_cmd(struct batch_wrapper *w, const char *cmd);


/** Specify cleanup commands.
 * The shell statement specified will be executed before exiting the wrapper,
 * even if previous commands failed. This is a good place for cleanup actions.
 * Can be called multiple times.
 * @param cmd The shell command to add.
 */
void batch_wrapper_post(struct batch_wrapper *w, const char *cmd);

/** Set the name prefix to use for the wrapper script.
 * The actual filename will consist of the prefix, an underscore,
 * and some random characters to ensure that the name is unique.
 * Defaults to "./wrapper"
 * @param prefix The filename prefix to use.
 */
void batch_wrapper_prefix(struct batch_wrapper *w, const char *prefix);

/**
 * Write out the batch_wrapper as a shell script.
 * Does not consume the batch_wrapper.
 * @param prefix The prefix to use to generate a unique name for the wrapper.
 * @returns The name of the generated wrapper, which the caller must free().
 * @returns NULL on failure, and sets errno.
 */
char *batch_wrapper_write(struct batch_wrapper *w, struct batch_task *t);

/** Generate one or more wrapper scripts from a JX command spec.
 * All generated scripts will be added as inputs to the given
 * batch task.
 * @returns The name of the outermost wrapper script.
 * @returns NULL on failure, and sets errno.
 */
char *batch_wrapper_expand(struct batch_task *t, struct jx *spec);

#endif
/* vim: set noexpandtab tabstop=8: */
