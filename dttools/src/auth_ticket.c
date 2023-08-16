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
#include "md5.h"
#include "random.h"
#include "shell.h"
#include "sort_dir.h"
#include "xxmalloc.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Prevent openssl from opening $HOME/.rnd */
#define OPENSSL_RANDFILE \
	"if [ -r /dev/urandom ]; then\n" \
	"	export RANDFILE=/dev/urandom\n" \
	"elif [ -r /dev/random ]; then\n" \
	"	export RANDFILE=/dev/random\n" \
	"else\n" \
	"	unset RANDFILE\n" \
	"	export HOME=/\n" \
	"fi\n"

#define CHALLENGE_LENGTH  (64)

static auth_ticket_server_callback_t server_callback = NULL;
static char **client_tickets = NULL;

static int auth_ticket_assert(struct link *link, time_t stoptime)
{
	int rc;
	char line[AUTH_LINE_MAX];
	char **tickets = client_tickets;

	if(tickets) {
		char *ticket;

		for (ticket = *tickets; ticket; ticket = *(++tickets)) {
			char digest[MD5_DIGEST_LENGTH_HEX+1] = "";
			char challenge[1024];

			if (access(ticket, R_OK) == -1) {
				debug(D_AUTH, "could not access ticket %s: %s", ticket, strerror(errno));
				continue;
			}

			/* load the digest */
			{
				static const char cmd[] =
					OPENSSL_RANDFILE
					"openssl rsa -in \"$TICKET\" -pubout\n"
					;

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
			if(strcmp(line, "declined") == 0) {
				debug(D_AUTH, "ticket %s declined, trying next one...", digest);
				continue;
			}

			errno = 0;
			unsigned long length = strtoul(line, NULL, 10);
			if(errno == ERANGE || errno == EINVAL)
				CATCH(EIO);
			else if (length > sizeof(challenge))
				CATCH(EINVAL);

			CATCHUNIX(link_read(link, challenge, length, stoptime));
			debug(D_AUTH, "received challenge of %lu bytes", length);

			{
				static const char cmd[] =
					OPENSSL_RANDFILE
					"openssl rsautl -inkey \"$TICKET\" -sign\n" /* reads challenge from stdin */
					;

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
					debug(D_AUTH, "please debug using \"dd if=/dev/urandom count=64 bs=1 | openssl rsautl -inkey <ticket file> -sign\"");
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

			if(strcmp(line, "success") == 0) {
				debug(D_AUTH, "succeeded challenge for %s", digest);
				rc = 0;
				goto out;
			} else if(strcmp(line, "failure") == 0) {
				debug(D_AUTH, "failed challenge for %s", digest);
				THROW_QUIET(EINVAL);
			} else {
				debug(D_AUTH, "received bad response: '%s'", line);
				THROW_QUIET(EINVAL);
			}
		}
	}
	CATCHUNIX(link_putliteral(link, "==\n", stoptime));

	rc = EACCES;
	goto out;
out:
	return RCUNIX(rc);
}

static int auth_ticket_accept(struct link *link, char **subject, time_t stoptime)
{
	int rc;
	char *ticket = NULL;
	int tmpfd = -1;
	char tmpf[PATH_MAX] = "";

	debug(D_AUTH, "ticket: waiting for tickets");

	while(1) {
		char line[AUTH_LINE_MAX];
		CATCHUNIX(link_readline(link, line, sizeof(line), stoptime) ? 0 : -1);
		if(strcmp(line, "==") == 0) {
			debug(D_AUTH, "ticket: exhausted all ticket challenges");
			break;
		} else if(strlen(line) == MD5_DIGEST_LENGTH_HEX) {
			char ticket_digest[MD5_DIGEST_LENGTH_HEX + 1];
			char ticket_subject[AUTH_LINE_MAX];
			strcpy(ticket_digest, line);
			strcpy(ticket_subject, line);
			debug(D_AUTH, "ticket: read ticket digest: %s", ticket_digest);
			if(server_callback) {
				free(ticket); /* free previously allocated ticket string or NULL (noop) */
				ticket = server_callback(ticket_digest);
				if(ticket) {
					char challenge[CHALLENGE_LENGTH];
					char sig[CHALLENGE_LENGTH*64]; /* 64x (4096) should be more than enough... */
					size_t siglen;

					random_array(challenge, sizeof(challenge));
					CATCHUNIX(link_printf(link, stoptime, "%zu\n", sizeof(challenge)));
					CATCHUNIX(link_putlstring(link, challenge, sizeof(challenge), stoptime));
					debug(D_AUTH, "sending challenge of %zu bytes", sizeof(challenge));

					CATCHUNIX(link_readline(link, line, sizeof(line), stoptime) ? 0 : -1);
					errno = 0;
					siglen = strtoul(line, NULL, 10);
					if(errno == ERANGE || errno == EINVAL) {
						link_soak(link, siglen, stoptime);
						CATCHUNIX(link_putliteral(link, "failure\n", stoptime));
						CATCH(EIO);
					} else if (siglen > sizeof(sig)) {
						link_soak(link, siglen, stoptime);
						CATCHUNIX(link_putliteral(link, "failure\n", stoptime));
						CATCH(EINVAL);
					}
					CATCHUNIX(link_read(link, sig, siglen, stoptime));
					debug(D_AUTH, "received signed challenge of %zu bytes", siglen);

					{
						static const char cmd[] =
							OPENSSL_RANDFILE
							"openssl rsautl -inkey \"$TICKET\" -pubin -verify\n"
							;

						const char *env[] = {NULL, NULL};
						BUFFER_STACK_ABORT(Benv, 8192);
						BUFFER_STACK(Bout, 4096);
						BUFFER_STACK(Berr, 4096);
						int status;

						strcpy(tmpf, "/tmp/tmp.XXXXXX");
						CATCHUNIX(tmpfd = mkstemp(tmpf));
						CATCHUNIX(full_write(tmpfd, ticket, strlen(ticket)));
						CATCHUNIX(close(tmpfd));
						tmpfd = -1;

						buffer_putfstring(Benv, "TICKET=%s", tmpf);
						env[0] = buffer_tostring(Benv);
						CATCHUNIX(shellcode(cmd, env, sig, siglen, Bout, Berr, &status));
						unlink(tmpf);
						strcpy(tmpf, "");
						if (buffer_pos(Berr))
							debug(D_DEBUG, "shellcode:\n%s", buffer_tostring(Berr));

						if (status) {
							debug(D_AUTH, "openssl failed!");
							CATCHUNIX(link_putliteral(link, "failure\n", stoptime));
							continue;
						}
						if (buffer_pos(Bout) != sizeof(challenge) || memcmp(buffer_tostring(Bout), challenge, sizeof(challenge)) != 0) {
							debug(D_AUTH, "failed challenge for %s", ticket_digest);
							CATCHUNIX(link_putliteral(link, "failure\n", stoptime));
							continue;
						}

						debug(D_AUTH, "succeeded challenge for %s", ticket_digest);
						CATCHUNIX(link_putliteral(link, "success\n", stoptime));
						*subject = xxmalloc(AUTH_LINE_MAX);
						strcpy(*subject, ticket_subject);
						rc = 0;
						goto out;
					}
				} else {
					debug(D_AUTH, "declining key %s", ticket_digest);
					CATCHUNIX(link_putliteral(link, "declined\n", stoptime));
				}
			} else {
				debug(D_AUTH, "declining key %s", ticket_digest);
				CATCHUNIX(link_putliteral(link, "declined\n", stoptime));
			}
		} else {
			debug(D_AUTH, "ticket: bad response");
			break;
		}
	}

	rc = 1;
	goto out;
out:
	if (tmpf[0])
		unlink(tmpf);
	if (tmpfd >= 0)
		close(tmpfd);
	free(ticket); /* free previously allocated ticket string or NULL (noop) */
	return rc == 0 ? 1 : 0;
}

int auth_ticket_register(void)
{
	if(!client_tickets) {
		client_tickets = xxrealloc(NULL, sizeof(char *));
		client_tickets[0] = NULL;
	}
	debug(D_AUTH, "ticket: registered");
	return auth_register("ticket", auth_ticket_assert, auth_ticket_accept);
}

void auth_ticket_server_callback (auth_ticket_server_callback_t sc)
{
	server_callback = sc;
}

void auth_ticket_load(const char *tickets)
{
	size_t n = 0;
	client_tickets = xxrealloc(client_tickets, sizeof(char *));
	client_tickets[n] = NULL;

	if(tickets) {
		const char *start, *end;
		for(start = end = tickets; start < tickets + strlen(tickets); start = ++end) {
			while(*end != '\0' && *end != ',')
				end++;
			if(start == end)
				continue;
			char *value = xxmalloc(end - start + 1);
			memset(value, 0, end - start + 1);
			strncpy(value, start, end - start);
			debug(D_CHIRP, "adding %s", value);
			client_tickets = xxrealloc(client_tickets, sizeof(char *) * ((++n) + 1));
			client_tickets[n - 1] = value;
			client_tickets[n] = NULL;
		}
	} else {
		/* populate a list with tickets in the current directory */
		int i;
		char **list;
		sort_dir(".", &list, strcmp);
		for(i = 0; list[i]; i++) {
			if(strncmp(list[i], "ticket.", strlen("ticket.")) == 0 && (strlen(list[i]) == (strlen("ticket.") + (MD5_DIGEST_LENGTH << 1)))) {
				debug(D_CHIRP, "adding ticket %s", list[i]);
				client_tickets = xxrealloc(client_tickets, sizeof(char *) * ((++n) + 1));
				client_tickets[n - 1] = strdup(list[i]);
				client_tickets[n] = NULL;
			}
		}
		sort_dir_free(list);
	}
}

/* vim: set noexpandtab tabstop=8: */
