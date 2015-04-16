/*
 * Copyright (C) 2014- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef CATCH_H
#define CATCH_H

#include <errno.h>
#include <string.h>

#define THROW_QUIET(e) \
	do {\
		rc = (e);\
		goto out;\
	} while (0)

#define CATCH(expr) \
	do {\
		rc = (expr);\
		if (rc) {\
			debug(D_DEBUG, "[%s:%d] error: %d `%s'", __FILE__, __LINE__, rc, strerror(rc));\
			goto out;\
		}\
	} while (0)

#define CATCHUNIX(expr) \
	do {\
		rc = (expr);\
		if (rc == -1) {\
			debug(D_DEBUG, "[%s:%d] unix error: -1 (errno = %d) `%s'", __FILE__, __LINE__, errno, strerror(errno));\
			rc = errno;\
			goto out;\
		}\
	} while (0)

#define CATCHUNIXIGNORE(expr,err) \
	do {\
		rc = (expr);\
		if (rc == -1 && errno != err) {\
			debug(D_DEBUG, "[%s:%d] unix error: -1 (errno = %d) `%s'", __FILE__, __LINE__, errno, strerror(errno));\
			rc = errno;\
			goto out;\
		}\
	} while (0)

#endif

/* vim: set noexpandtab tabstop=4: */
