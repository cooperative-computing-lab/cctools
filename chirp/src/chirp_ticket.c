/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "chirp_ticket.h"

#include "buffer.h"
#include "md5.h"
#include "xxmalloc.h"

#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <string.h>

#define DIGEST_SCAN  "%n%32[0123456789abcdefABCDEF]%n"
#define DIGEST_FORMAT  "%32s"
#define TICKET_PREFIX  ".__"
#define TICKET_PREFIX_LENGTH  (sizeof(TICKET_PREFIX)/sizeof(char))
#define TICKET_FILENAME_FORMAT TICKET_PREFIX "ticket." DIGEST_FORMAT
#define TICKET_FILENAME_SCAN TICKET_PREFIX "ticket." DIGEST_SCAN
#define TICKET_SUBJECT_FORMAT "ticket:" DIGEST_FORMAT
#define TICKET_SUBJECT_SCAN "ticket:" DIGEST_SCAN
#define TICKET_DIGEST_LENGTH  (MD5_DIGEST_LENGTH_HEX)

#define unsigned_isspace(c) isspace((unsigned char) c)

static int readquote(const char **buffer, const char **s, size_t * l)
{
	while(unsigned_isspace(**buffer))
		(*buffer)++;
	if(**buffer != '"')
		return 0;
	(*buffer)++;
	*l = 0;
	*s = *buffer;
	while(**buffer != '"' && **buffer != '\0') {
		(*buffer)++;
		(*l)++;
	}
	if(**buffer != '"')
		return 0;
	(*buffer)++;
	return 1;
}

void chirp_ticket_free(struct chirp_ticket *ct)
{
	size_t n = 0;
	free(ct->subject);
	free(ct->ticket);
	while(n < ct->nrights) {
		free(ct->rights[n].directory);
		free(ct->rights[n].acl);
		n++;
	}
	free(ct->rights);
}

char *chirp_ticket_tostring(struct chirp_ticket *ct)
{
	size_t n;
	char *result;
	buffer_t B[1];

	buffer_init(B);
	buffer_abortonfailure(B, 1);

	buffer_putfstring(B, "subject \"%s\"\n", ct->subject);
	buffer_putfstring(B, "ticket \"%s\"\n", ct->ticket);
	buffer_putfstring(B, "expiration \"%lu\"\n", (unsigned long) ct->expiration);
	for(n = 0; n < ct->nrights; n++) {
		buffer_putfstring(B, "rights \"%s\" \"%s\"\n", ct->rights[n].directory, ct->rights[n].acl);
	}

	buffer_dup(B, &result);
	buffer_free(B);

	return result;
}

int chirp_ticket_read(const char *ticket, struct chirp_ticket *ct)
{
	int status = 0;

	const char *b = ticket;
	size_t l = strlen(ticket);

	/* Ticket format (quoted strings may span multiple lines):
	 * subject "<subject>"
	 * ticket "<ticket>"
	 * expiration "<UTC seconds since Epoch>"
	 * rights "<directory>" "<acl>"
	 * rights "<directory>" "<acl>"
	 * ...
	 */
	size_t n;
	const char *s;
	const char *buffer = b;
	time_t now = time(NULL);
	ct->subject = NULL;
	ct->ticket = NULL;
	ct->expiration = now;	/* default expire now... */
	ct->expired = 1;	/* default is expired */
	ct->nrights = 0;
	ct->rights = NULL;
	while(1) {
		assert(buffer >= b && b + l >= buffer);
		while(unsigned_isspace(*buffer))
			buffer++;
		assert(buffer >= b && b + l >= buffer);
		if(strncmp(buffer, "subject", strlen("subject")) == 0) {
			buffer += strlen("subject");
			if(!readquote(&buffer, &s, &n))
				break;
			ct->subject = xxrealloc(ct->subject, n + 1);
			memcpy(ct->subject, s, n);
			ct->subject[n] = '\0';
		} else if(strncmp(buffer, "ticket", strlen("ticket")) == 0) {
			buffer += strlen("ticket");
			if(!readquote(&buffer, &s, &n))
				break;
			ct->ticket = xxrealloc(ct->ticket, n + 1);
			memcpy(ct->ticket, s, n);
			ct->ticket[n] = '\0';
		} else if(strncmp(buffer, "expiration", strlen("expiration")) == 0) {
			buffer += strlen("expiration");
			if(!readquote(&buffer, &s, &n))
				break;
			char *stime = xxmalloc(n + 1);
			memcpy(stime, s, n);
			stime[n] = '\0';
			ct->expiration = (time_t) strtoul(stime, NULL, 10);
			ct->expired = (ct->expiration <= now);
			free(stime);
		} else if(strncmp(buffer, "rights", strlen("rights")) == 0) {
			buffer += strlen("rights");
			if(!readquote(&buffer, &s, &n))
				break;
			ct->rights = xxrealloc(ct->rights, sizeof(*ct->rights) * (++ct->nrights) + 1);
			ct->rights[ct->nrights - 1].directory = xxmalloc(n + 1);
			memcpy(ct->rights[ct->nrights - 1].directory, s, n);
			ct->rights[ct->nrights - 1].directory[n] = '\0';
			if(!readquote(&buffer, &s, &n))
				break;
			char *acl = xxmalloc(n + 1);
			memcpy(acl, s, n);
			acl[n] = '\0';
			ct->rights[ct->nrights - 1].acl = xxstrdup(acl);
			free(acl);
		} else if(*buffer == '\0') {
			if(ct->subject && ct->ticket && ct->nrights > 0) {
				status = 1;
			}
			break;
		} else {
			break;
		}
	}
	if(ct->rights == NULL) {
		assert(ct->nrights == 0);
		ct->rights = xxrealloc(ct->rights, sizeof(*ct->rights) * (++ct->nrights) + 1);
		ct->rights[ct->nrights - 1].directory = xxstrdup("/");
		ct->rights[ct->nrights - 1].acl = xxstrdup("n");
		ct->nrights = 1;
	}
	return status && !ct->expired;
}

void chirp_ticket_name(const char *pk, char *ticket_subject, char *ticket_filename)
{
	unsigned char digest[TICKET_DIGEST_LENGTH];
	md5_context_t context;
	md5_init(&context);
	md5_update(&context, (const unsigned char *) pk, strlen(pk));
	md5_final(digest, &context);
	sprintf(ticket_subject, TICKET_SUBJECT_FORMAT, md5_to_string(digest));
	sprintf(ticket_filename, "/" TICKET_FILENAME_FORMAT, md5_to_string(digest));
}

void chirp_ticket_filename(char *ticket_filename, const char *ticket_subject, const char *digest)
{
	if(digest == NULL) {
		assert(ticket_subject);
		int result = chirp_ticket_isticketsubject(ticket_subject, &digest);
		assert(result);
	}
	sprintf(ticket_filename, "/" TICKET_FILENAME_FORMAT, digest);
}

void chirp_ticket_subject(char *ticket_subject, const char *ticket_filename)
{
	const char *digest;
	int result = chirp_ticket_isticketfilename(ticket_filename, &digest);
	assert(result);
	sprintf(ticket_subject, TICKET_SUBJECT_FORMAT, digest);
}

int chirp_ticket_isticketfilename(const char *ticket_filename, const char **digest)
{
	int n1, n2;
	char buffer[TICKET_DIGEST_LENGTH + 1];
	if((sscanf(ticket_filename, TICKET_FILENAME_SCAN, &n1, buffer, &n2) == 1) && ((n2 - n1) == TICKET_DIGEST_LENGTH) && (strlen(ticket_filename) == (size_t) n2)) {
		assert(n1 > 0);
		*digest = ticket_filename + n1;
		return 1;
	} else {
		return 0;
	}
}

int chirp_ticket_isticketsubject(const char *ticket_subject, const char **digest)
{
	int n1, n2;
	char buffer[TICKET_DIGEST_LENGTH + 1];
	if((sscanf(ticket_subject, TICKET_SUBJECT_SCAN, &n1, buffer, &n2) == 1) && ((n2 - n1) == TICKET_DIGEST_LENGTH) && (strlen(ticket_subject) == (size_t) n2)) {
		assert(n1 > 0);
		*digest = ticket_subject + n1;
		return 1;
	} else {
		return 0;
	}
}

const char *chirp_ticket_digest(const char *pk)
{
	unsigned char digest[TICKET_DIGEST_LENGTH];
	md5_context_t context;
	md5_init(&context);
	md5_update(&context, (const unsigned char *) pk, strlen(pk));
	md5_final(digest, &context);
	return md5_to_string(digest);	/* static memory */
}

/* vim: set noexpandtab tabstop=8: */
