/*
Copyright (C) 2008- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_THIRDPUT_H
#define CHIRP_THIRDPUT_H

#include <sys/time.h>

int64_t chirp_thirdput(const char *subject, const char *lpath, const char *hostname, const char *rpath, time_t stoptime);

#endif

/* vim: set noexpandtab tabstop=4: */
