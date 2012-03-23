/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef AUTH_ALL_H
#define AUTH_ALL_H

/** @file auth_all.h Global authentication controls.
Chirp has a flexible authentication system that allows users
to identify themselves to severs in many different ways.
This module controls which methods are currently active.

Most programs should call @ref auth_register_all to enable
all modes by default, and only call @ref auth_register_byname
to  choose a specific method when directed by the user with
the <tt>-a</tt> command line option.
*/

#include "auth.h"
#include "auth_unix.h"
#include "auth_kerberos.h"
#include "auth_globus.h"
#include "auth_hostname.h"
#include "auth_address.h"
#include "auth_ticket.h"

/** Enables a specific authentication mode.
If called multiple times, the methods will be
attempted in the order chosen.
@param name The authentication mode, which may be:
- "globus"
- "kerberos"
- "unix"
- "hostname"
- "address"
@see auth_register_all
*/

int auth_register_byname(const char *name);

/** Enable all authentication modes.
Enables all authentication modes, in a default order.
@see auth_register_byname
*/

int auth_register_all(void);

#endif
