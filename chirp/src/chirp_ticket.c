/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_ticket.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

int chirp_ticket_isticketsubject(const char *subject, const char **digest)
{
	int n1, n2;
	char buffer[CHIRP_TICKET_DIGEST_LENGTH + 1];
	int i;
	if(((i = sscanf(subject, CHIRP_TICKET_FORMAT, &n1, buffer, &n2)) == 1) && ((n2 - n1) == CHIRP_TICKET_DIGEST_LENGTH) && (strlen(subject) == (size_t) n2)) {
		assert(n1 > 0);
		*digest = subject + n1;
		return 1;
	} else {
		return 0;
	}
}
