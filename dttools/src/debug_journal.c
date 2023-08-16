/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_SYSTEMD_JOURNAL_H

#include "debug.h"

#include <systemd/sd-journal.h>

#include <stdarg.h>

void debug_journal_write (INT64_T flags, const char *str)
{
	int priority = 0;
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
	sd_journal_print(priority, "%s", str);
}

#endif

/* vim: set noexpandtab tabstop=8: */
