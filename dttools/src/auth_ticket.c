/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "auth_ticket.h"

#include "auth.h"
#include "buffer.h"
#include "catch.h"
#include "debug.h"
#include "dpopen.h"
#include "full_io.h"
#include "hash_table.h"
#include "link.h"
#include "list.h"
#include "md5.h"
#include "random.h"
#include "shell.h"
#include "sort_dir.h"
#include "stringtools.h"
#include "xxmalloc.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Prevent openssl from opening $HOME/.rnd */
#define OPENSSL_RANDFILE                                                                                               \
	"if [ -r /dev/urandom ]; then\n"                                                                               \
	"	export RANDFILE=/dev/urandom\n"                                                                              \
	"elif [ -r /dev/random ]; then\n"                                                                              \
	"	export RANDFILE=/dev/random\n"                                                                               \
	"else\n"                                                                                                       \
	"	unset RANDFILE\n"                                                                                            \
	"	export HOME=/\n"                                                                                             \
	"fi\n"

#define CHALLENGE_LENGTH (64)

static auth_ticket_server_callback_t server_callback = NULL;
static struct list *client_ticket_list = NULL;

static int auth_ticket_assert(struct link *link, time_t stoptime)
{
	int rc;
	char line[AUTH_LINE_MAX];
	char *ticket;

	LIST_ITERATE(client_ticket_list, ticket)
	{
		char digest[MD5_DIGEST_LENGTH_HEX + 1] = "";
		char challenge[1024];

		if (access(ticket, R_OK) == -1) {
			debug(D_AUTH, "could not access ticket %s: %s", ticket, strerror(errno));
			continue;
		}

		/* load the digest */
		{
			static const char cmd[] = OPENSSL_RANDFILE "openssl rsa -in \"$TICKET\" -pubout\n";

			const char *env[] = {NULL, NULL};
			BUFFER_STACK_ABORT(Benv, 8192);
			BUFFER_STACK(Bout, 4096);
			BUFFER_STACK(Berr, 4096);
			int status;

			buffer_putfstring(Benv, "TICKET=%s", ticket);
			env[0] = buffer_tostring(Benv);
			CATCHUNIX(shellcode(cmd, env, NULL, 0, Bout, Berr, &status));
			if (buffer_pos(Berr))
				debug(D_DEBUG, "shellcode:\n%s", buffer_tostring(Berr));

			if (status == 0 && buffer_pos(Bout) > 0) {
				size_t i;
				unsigned char md5digest[MD5_DIGEST_LENGTH];
				BUFFER_STACK_ABORT(Bhex, sizeof(digest));
				md5_buffer(buffer_tostring(Bout), buffer_pos(Bout), md5digest);
				for (i = 0; i < sizeof(md5digest); i++)
					buffer_putfstring(Bhex, "%02x", (unsigned int)md5digest[i]);
				assert(buffer_pos(Bhex) < sizeof(digest));
				strcpy(digest, buffer_tostring(Bhex));
			} else {
				debug(D_AUTH, "openssl did not return pubkey, trying next ticket");
				continue;
			}
		}

		debug(D_AUTH, "trying ticket %s", digest);
		CATCHUNIX(link_printf(link, stoptime, "%s\n", digest));

		CATCHUNIX(link_readline(link, line, sizeof(line), stoptime) ? 0 : -1);
		if (strcmp(line, "declined") == 0) {
			debug(D_AUTH, "ticket %s declined, trying next one...", digest);
			continue;
		}

		errno = 0;
		unsigned long length = strtoul(line, NULL, 10);
		if (errno == ERANGE || errno == EINVAL)
			CATCH(EIO);
		else if (length > sizeof(challenge))
			CATCH(EINVAL);

		CATCHUNIX(link_read(link, challenge, length, stoptime));
		debug(D_AUTH, "received challenge of %lu bytes", length);

		{
			/* reads challenge from stdin */
#if defined(HAS_OPENSSL_PKEYUTL)
			static const char cmd[] = OPENSSL_RANDFILE "openssl pkeyutl -inkey \"$TICKET\" -sign\n";
#else
			static const char cmd[] = OPENSSL_RANDFILE "openssl rsautl -inkey \"$TICKET\" -sign\n";
#endif			
			const char *env[] = {NULL, NULL};
			BUFFER_STACK_ABORT(Benv, 8192);
			BUFFER_STACK_ABORT(Bout, 65536);
			BUFFER_STACK(Berr, 4096);
			int status;

			buffer_putfstring(Benv, "TICKET=%s", ticket);
			env[0] = buffer_tostring(Benv);
			rc = shellcode(cmd, env, challenge, length, Bout, Berr, &status);
			if (buffer_pos(Berr))
				debug(D_DEBUG, "shellcode:\n%s", buffer_tostring(Berr));
			if (rc == -1) {
				debug(D_AUTH, "openssl failed, your keysize may be too small");
				debug(D_AUTH, "please debug using \"dd if=/dev/urandom count=64 bs=1 | openssl pkeyutl -inkey <ticket file> -sign\"");
				CATCHUNIX(rc);
			}

			if (status) {
				debug(D_AUTH, "openssl did not return digest, trying next ticket");
				continue;
			}

			CATCHUNIX(link_printf(link, stoptime, "%zu\n", buffer_pos(Bout)));
			CATCHUNIX(link_putlstring(link, buffer_tostring(Bout), buffer_pos(Bout), stoptime));
			debug(D_AUTH, "sent signed challenge of %zu bytes", buffer_pos(Bout));
		}

		CATCHUNIX(link_readline(link, line, sizeof(line), stoptime) ? 0 : -1);

		if (strcmp(line, "success") == 0) {
			debug(D_AUTH, "succeeded challenge for %s", digest);
			rc = 0;
			goto out;
		} else if (strcmp(line, "failure") == 0) {
			debug(D_AUTH, "failed challenge for %s", digest);
			THROW_QUIET(EINVAL);
		} else {
			debug(D_AUTH, "received bad response: '%s'", line);
			THROW_QUIET(EINVAL);
		}
	}
	CATCHUNIX(link_putliteral(link, "==\n", stoptime));

	rc = EACCES;
	goto out;
out:
	return RCUNIX(rc);
}

/*
Fill in "filename" with a unique temporary file location, then write length bytes of data to it.
*/

static int write_data_to_temp_file(char *filename, const char *data, int length)
{
	strcpy(filename, "/tmp/ticket.tmp.XXXXXX");
	int fd = mkstemp(filename);
	if (fd < 0) {
		debug(D_AUTH, "ticket: unable to create temp file %s: %s\n", filename, strerror(errno));
		return 0;
	}

	full_write(fd, data, length);
	close(fd);
	return 1;
}

/*
Invoke the server callback to get the ticket body from the digest name.
Then, challenge the client to prove that they hold the corresponding key.
Return true if challenge succeeds.
*/

static int server_accepts_ticket(struct link *link, const char *ticket_digest, time_t stoptime)
{
	char line[AUTH_LINE_MAX];
	char challenge[CHALLENGE_LENGTH];
	char signature[CHALLENGE_LENGTH * 64]; /* 64x (4096) should be more than enough... */
	int signature_length = 0;

	if (!server_callback) {
		return 0;
	}

	/* Get the actual stored ticket based on the digest name. */
	char *ticket = server_callback(ticket_digest);
	if (!ticket) {
		return 0;
	}

	/* The challenge data is just a random array to be encyrpted by the client */
	random_array(challenge, sizeof(challenge));

	/* Send the length of the challenge followed by the data itself. */
	debug(D_AUTH, "ticket: sending challenge of %zu bytes", sizeof(challenge));
	link_printf(link, stoptime, "%zu\n", sizeof(challenge));
	link_putlstring(link, challenge, sizeof(challenge), stoptime);

	/* Read back the client response */
	if (!link_readline(link, line, sizeof(line), stoptime)) {
		return 0;
	}

	/* The response should be an integer length followed by that many signature bytes */

	errno = 0;
	signature_length = strtol(line, NULL, 10);

	if (errno != 0 || signature_length > (int)sizeof(signature)) {
		debug(D_AUTH, "ticket: invalid response to challenge\n");
		return 0;
	}

	if (link_read(link, signature, signature_length, stoptime) != signature_length) {
		debug(D_AUTH, "ticket: unable to read entire signature of %d bytes\n", signature_length);
		return 0;
	}

	debug(D_AUTH, "ticket: received signed challenge of %d bytes", signature_length);

	/* Now use openssl to verify the signature. */
	char ticket_file[PATH_MAX];
	char signature_file[PATH_MAX];
	char challenge_file[PATH_MAX];

	if (!write_data_to_temp_file(ticket_file, ticket, strlen(ticket))) {
		debug(D_AUTH, "ticket: couldn't write to %s: %s\n", ticket_file, strerror(errno));
		return 0;
	}

	if (!write_data_to_temp_file(signature_file, signature, signature_length)) {
		debug(D_AUTH, "ticket: couldn't write to %s: %s\n", signature_file, strerror(errno));
		unlink(ticket_file);
		return 0;
	}

	if (!write_data_to_temp_file(challenge_file, challenge, sizeof(challenge))) {
		debug(D_AUTH, "ticket: couldn't write to %s: %s\n", challenge_file, strerror(errno));
		unlink(ticket_file);
		unlink(signature_file);
		return 0;
	}

#if defined(HAS_OPENSSL_PKEYUTL)
	char *cmd = string_format("openssl pkeyutl -pubin -inkey \"%s\" -in \"%s\" -sigfile \"%s\" -verify",
#else
	char *cmd = string_format("openssl rsautl -pubin -inkey \"%s\" -in \"%s\" -verify",
#endif
			ticket_file,
			challenge_file,
			signature_file);

	debug(D_DEBUG, "ticket: %s\n", cmd);
	int result = system(cmd);
	free(cmd);

	unlink(signature_file);
	unlink(challenge_file);
	unlink(ticket_file);

	if (result != 0) {
		debug(D_AUTH, "ticket: failed challenge for %s", ticket_digest);
		return 0;
	}

	debug(D_AUTH, "ticket: succeeded challenge for %s", ticket_digest);
	return 1;
}

/*
Accept an auth ticket request from the client.
The client may send any number of ticket digests.
For each one, the server will respond "success" or "failure"
until the client sends "==" to indicate end of list.
*/

static int auth_ticket_accept(struct link *link, char **subject, time_t stoptime)
{
	debug(D_AUTH, "ticket: waiting for tickets");

	while (1) {
		char ticket_digest[AUTH_LINE_MAX];
		if (!link_readline(link, ticket_digest, sizeof(ticket_digest), stoptime)) {
			debug(D_AUTH, "ticket: disconnected from client");
			break;
		}

		if (strcmp(ticket_digest, "==") == 0) {
			debug(D_AUTH, "ticket: exhausted all ticket challenges");
			break;
		}

		if (strlen(ticket_digest) != MD5_DIGEST_LENGTH_HEX) {
			debug(D_AUTH, "ticket: bad response");
			break;
		}

		debug(D_AUTH, "ticket: read ticket digest: %s", ticket_digest);

		if (server_accepts_ticket(link, ticket_digest, stoptime)) {
			link_putliteral(link, "success\n", stoptime);
			/* for tickets, the digest itself is the subject name */
			*subject = strdup(ticket_digest);
			return 1;
		} else {
			link_putliteral(link, "failure\n", stoptime);
			/* keep going around for next attempt */
		}
	}

	return 0;
}

int auth_ticket_register(void)
{
	if (!client_ticket_list) {
		client_ticket_list = list_create();
	}
	debug(D_AUTH, "ticket: registered");
	return auth_register("ticket", auth_ticket_assert, auth_ticket_accept);
}

void auth_ticket_server_callback(auth_ticket_server_callback_t sc)
{
	server_callback = sc;
}

void auth_ticket_load(const char *tickets)
{
	if (tickets) {
		char *tickets_copy = strdup(tickets);
		char *t = strtok(tickets_copy, ",");
		while (t) {
			debug(D_CHIRP, "adding %s", t);
			list_push_tail(client_ticket_list, strdup(t));
			t = strtok(0, ",");
		}
		free(tickets_copy);
	} else {
		/* populate a list with tickets in the current directory */
		int i;
		char **list;
		sort_dir(".", &list, strcmp);
		for (i = 0; list[i]; i++) {
			if (strncmp(list[i], "ticket.", strlen("ticket.")) == 0 &&
					(strlen(list[i]) == (strlen("ticket.") + (MD5_DIGEST_LENGTH << 1)))) {
				debug(D_CHIRP, "adding ticket %s", list[i]);
				list_push_tail(client_ticket_list, strdup(list[i]));
			}
		}
		sort_dir_free(list);
	}
}

/* vim: set noexpandtab tabstop=8: */
