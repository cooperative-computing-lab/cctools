/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_TICKET_H
#define CHIRP_TICKET_H

#include "md5.h"

#include <string.h>

#define CHIRP_TICKET_PREFIX  ".__"
#define CHIRP_TICKET_PREFIX_LENGTH  strlen(CHIRP_TICKET_PREFIX)
#define CHIRP_TICKET_FORMAT  "ticket:%n%32[0123456789abcdefABCDEF]%n"
#define CHIRP_TICKET_DIGEST_LENGTH  (MD5_DIGEST_LENGTH_HEX)

int chirp_ticket_isticketsubject(const char *subject, const char **digest);

#endif
