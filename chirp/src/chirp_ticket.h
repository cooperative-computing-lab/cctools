/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_TICKET_H
#define CHIRP_TICKET_H

#include <time.h>
#include <string.h>

struct chirp_ticket {
	char *subject;
	char *ticket;
	time_t expiration;
	short expired;
	size_t nrights;
	struct chirp_ticket_rights {
		char *directory;
		char *acl;
	} *rights;
};

int chirp_ticket_read(const char *ticket, struct chirp_ticket *ct);
void chirp_ticket_free(struct chirp_ticket *ct);

char *chirp_ticket_tostring(struct chirp_ticket *ct);
void chirp_ticket_name(const char *root, const char *pk, char *ticket_subject, char *ticket_filename);
void chirp_ticket_subject(char *ticket_subject, const char *ticket_filename);
void chirp_ticket_filename(char *ticket_filename, const char *root, const char *ticket_subject);
int chirp_ticket_isticketfilename(const char *ticket_filename, const char **digest);
int chirp_ticket_isticketsubject(const char *ticket_subject, const char **digest);
const char *chirp_ticket_digest(const char *pk);

#endif
