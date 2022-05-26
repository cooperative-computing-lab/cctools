/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef LINK_AUTH_H
#define LINK_AUTH_H

#include "link.h"

/* @file link_auth.h Simple authentication routines. */

/** Authenticate a link based on the contents of a shared password, without sending it in the clear.
@param link The link to authenticate.
@param shared_key A string containing the password to verify.
@param stoptime The time at which to abort.
@return Non-zero on success, zero on failure.
*/

int link_auth_password( struct link *link, const char *password, time_t stoptime );

#endif
