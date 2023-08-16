/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef SHELL_H
#define SHELL_H

#include "buffer.h"

int shellcode (const char *cmd, const char * const env[], const char *in, size_t len, buffer_t *Bout, buffer_t *Berr, int *status);

#endif

/* vim: set noexpandtab tabstop=8: */
