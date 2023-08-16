/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_SYSLOG_H

#include "debug.h"

#include <syslog.h>

#include <stdarg.h>

void debug_syslog_write (INT64_T flags, const char *str)
{
	int priority = LOG_USER;
	if (flags & D_FATAL) {
		priority |= LOG_NOTICE;
	} else if (flags & D_ERROR) {
		priority |= LOG_ERR;
	} else if (flags & D_NOTICE) {
		priority |= LOG_CRIT;
	} else if (flags & D_DEBUG) {
		priority |= LOG_DEBUG;
	} else {
		priority |= LOG_INFO;
	}
	syslog(priority, "%s", str);
}

void debug_syslog_config (const char *name)
{
	openlog(name, LOG_PID|LOG_NOWAIT, LOG_USER);
}
#endif

/* vim: set noexpandtab tabstop=8: */
