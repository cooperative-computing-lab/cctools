#include "auth_ticket.h"

#include "auth.h"
#include "debug.h"
#include "dpopen.h"
#include "full_io.h"
#include "link.h"
#include "md5.h"
#include "xmalloc.h"

#include <sys/stat.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DIGEST_LENGTH  (MD5_DIGEST_LENGTH_HEX)
#define CHALLENGE_LENGTH  (64)
#define CHALLENGE_LENGTH_STR  "64"

static int auth_ticket_assert (struct link *link, struct hash_table *t, time_t stoptime)
{
  /* FIXME need to save errno ? */
  char line[AUTH_LINE_MAX];

  char **tickets=0;
  
  if (t)
	tickets = (char **) hash_table_lookup(t, "ticket");

  if (tickets) {
	char *ticket;
    char digest[DIGEST_LENGTH];

	while ((ticket = *(tickets++)) != NULL) {
  	  char command[PATH_MAX*2+4096];

	  /* load the digest */
	  sprintf(command, "openssl rsa -in '%s' -pubout 2> /dev/null | openssl md5", ticket);
	  FILE *digestf = popen(command, "r");
      if (full_fread(digestf, digest, DIGEST_LENGTH) < DIGEST_LENGTH) {
        pclose(digestf);
        return 0;
      }
      pclose(digestf);
      debug(D_AUTH, "trying ticket %.*s", DIGEST_LENGTH, digest);
      if (link_write(link, digest, DIGEST_LENGTH, stoptime) <= 0) return 0;
      if (link_write(link, "\n", 1, stoptime) <= 0) return 0;

      if (link_readline(link, line, sizeof(line), stoptime) <= 0) return 0;
      if (strcmp(line, "declined") == 0) continue;

      unsigned long length = strtoul(line, NULL, 10);
      if (errno == ERANGE || errno == EINVAL) return 0; /* not a number? */
      debug(D_AUTH, "receiving challenge of %d bytes", length);

	  FILE *in, *out;
      static const char command_template[] =
          "T1=`mktemp`\n" /* signed challenge */
          "T2=`mktemp`\n" /* private key without comments */
          "sed '/^\\s*#/d' < '%s' > \"$T2\"\n"
          "openssl rsautl -inkey \"$T2\" -sign > \"$T1\" 2> /dev/null\n"
          "R=\"$?\"\n"
          "if [ \"$R\" -ne 0 ]; then\n"
          "  rm -f \"$T1\" \"$T2\"\n"
          "  exit \"$R\"\n"
          "fi\n"
          "ls -l \"$T1\" | awk '{ print $5 }'\n"
          "cat \"$T1\"\n"
          "rm -f \"$T1\" \"$T2\"\n";
	  sprintf(command, command_template, ticket);
	  pid_t pid = dpopen(command, &in, &out);
	  if (pid == 0) return 0;
	  if (link_stream_to_file(link, in, length, stoptime) <= 0) {
        dpclose(in, out, pid);
        debug(D_AUTH, "openssl failed, your keysize may be too small");
        debug(D_AUTH, "please debug using \"dd if=/dev/urandom count=64 bs=1 | openssl rsautl -inkey <ticket file> -sign\"");
		return 0;
	  }
      fclose(in);
      in = NULL;
	  if (link_stream_from_file(link, out, 1<<20, stoptime) <= 0) {
        dpclose(in, out, pid);
        debug(D_AUTH, "openssl failed, your keysize may be too small");
        debug(D_AUTH, "please debug using \"dd if=/dev/urandom count=64 bs=1 | openssl rsautl -inkey <ticket file> -sign\"");
	    return 0;
	  }
      dpclose(in, out, pid);

      if (link_readline(link, line, sizeof(line), stoptime) <= 0) return 0;
      if (strcmp(line, "success") == 0) {
        debug(D_AUTH, "succeeded challenge for %.*s\n", DIGEST_LENGTH, digest);
		return 1;
      } else if (strcmp(line, "failure") == 0) {
        debug(D_AUTH, "failed challenge for %.*s\n", DIGEST_LENGTH, digest);
		errno = EINVAL;
		return 0;
      } else {
        debug(D_AUTH, "received bad response: '%s'", line);
		errno = EINVAL;
		return 0;
      }
    }
  }
  const char *done = "==\n";
  link_write(link, done, strlen(done), stoptime);

  return 0;
}

static int auth_ticket_accept (struct link *link, char **subject, struct hash_table *t, time_t stoptime)
{
  int serrno = errno;
  int status = 0;
  char line[AUTH_LINE_MAX];
  char ticket_subject[AUTH_LINE_MAX];
  struct hash_table *TICKETS = NULL;

  errno = 0;
  
  if (t)
    TICKETS = (struct hash_table *) hash_table_lookup(t, "ticket");

  debug(D_AUTH, "ticket: waiting for tickets");

  while (1) {
    if (link_readline(link, line, sizeof(line), stoptime) > 0) {
      if (strcmp(line, "==") == 0) {
        debug(D_AUTH, "ticket: exhausted all ticket challenges");
        break;
      } else if (strlen(line) == DIGEST_LENGTH) {
        char ticket_digest[DIGEST_LENGTH+1];
        strcpy(ticket_digest, line);
        strcpy(ticket_subject, line);
        debug(D_AUTH, "ticket: read ticket digest: %s", ticket_digest);
        if (TICKETS) {
          char *ticket = hash_table_lookup(TICKETS, ticket_digest);
          if (ticket) {
            static const char command_template[] = 
                "T1=`mktemp`\n" /* The RSA Public Key */
                "T2=`mktemp`\n" /* The Challenge */
                "T3=`mktemp`\n" /* The Signed Challenge */
                "T4=`mktemp`\n" /* The Decrypted (verified) Signed Challenge */
                "echo -n '%s' > \"$T1\"\n"
                "dd if=/dev/urandom of=\"$T2\" bs=%u count=1 > /dev/null 2> /dev/null\n"
                "cat \"$T2\"\n" /* to stdout */
                "cat > \"$T3\"\n" /* from stdin */
                "openssl rsautl -inkey \"$T1\" -pubin -verify < \"$T3\" > \"$T4\" 2> /dev/null\n"
                "cmp \"$T2\" \"$T4\" > /dev/null 2> /dev/null\n"
                "R=\"$?\"\n"
                "rm -f \"$T1\" \"$T2\" \"$T3\" \"$T4\" > /dev/null 2> /dev/null\n"
                "exit \"$R\"\n";

            char *command = xxmalloc(sizeof(command_template)+strlen(ticket)+64);
            sprintf(command, command_template, ticket, CHALLENGE_LENGTH);

            FILE *in, *out;
            pid_t pid = dpopen(command, &in, &out);
            free(command);
            if (pid == 0) break;

            if (!link_write(link, CHALLENGE_LENGTH_STR "\n", strlen(CHALLENGE_LENGTH_STR)+1, stoptime)) break;
            if (!link_stream_from_file(link, out, CHALLENGE_LENGTH, stoptime)) break;

            if (link_readline(link, line, sizeof(line), stoptime) <= 0) break;
            unsigned long length = strtoul(line, NULL, 10);
            if (errno == ERANGE || errno == EINVAL) break; /* not a number? */
            if (!link_stream_to_file(link, in, length, stoptime)) break;

            int result = dpclose(in, out, pid);

            if (result == 0) {
              debug(D_AUTH, "succeeded challenge for %s\n", ticket_digest);
              static const char success[] = "success\n";
              link_write(link, success, strlen(success), stoptime);
              status = 1;
              break;
            } else {
              debug(D_AUTH, "failed challenge for %s\n", ticket_digest);
              static const char *failure = "failure\n";
              link_write(link, failure, strlen(failure), stoptime);
              break;
            }
          } else {
            const char *declined = "declined\n";
            debug(D_AUTH, "declining key %s", ticket_digest);
            link_write(link, declined, strlen(declined), stoptime);
          }
        } else {
          const char *declined = "declined\n";
          debug(D_AUTH, "declining key %s", ticket_digest);
          link_write(link, declined, strlen(declined), stoptime);
        }
      } else {
        debug(D_AUTH, "ticket: bad response");
        break;
      }
    } else {
      break;
    }
  }

  if (status) {
    *subject = xxmalloc(AUTH_LINE_MAX);
    strcpy(*subject, ticket_subject);
  }
  errno = serrno;
  return status;
}

int auth_ticket_register()
{
	debug(D_AUTH,"ticket: registered");
	return auth_register("ticket",auth_ticket_assert,auth_ticket_accept);
}
