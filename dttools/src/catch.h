/*
 * Copyright (C) 2014- The University of Notre Dame
 * This software is distributed under the GNU General Public License.
 * See the file COPYING for details.
 */

#ifndef CATCH_H
#define CATCH_H

#include "debug.h"

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
			debug(D_DEBUG, "%s: %s:%d[%s] error: %d `%s'", __func__, __FILE__, __LINE__, CCTOOLS_SOURCE, (int)rc, strerror((int)rc));\
			goto out;\
		}\
	} while (0)

#define CATCHUNIX(expr) \
	do {\
		rc = (expr);\
		if (rc == -1) {\
			rc = errno;\
			debug(D_DEBUG, "%s: %s:%d[%s] unix error: -1 (errno = %d) `%s'", __func__, __FILE__, __LINE__, CCTOOLS_SOURCE, (int)rc, strerror((int)rc));\
			goto out;\
		}\
	} while (0)

#define CATCHUNIXIGNORE(expr,err) \
	do {\
		rc = (expr);\
		if (rc == -1 && errno != err) {\
			rc = errno;\
			debug(D_DEBUG, "%s: %s:%d[%s] unix error: -1 (errno = %d) `%s'", __func__, __FILE__, __LINE__, CCTOOLS_SOURCE, (int)rc, strerror((int)rc));\
			goto out;\
		}\
	} while (0)

#define RCUNIX(rc) (rc == 0 ? 0 : (errno = (int)rc, -1))

#endif

/* vim: set noexpandtab tabstop=4: */
