/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "link_auth.h"
#include "link.h"
#include "debug.h"
#include "stringtools.h"
#include "sha1.h"
#include "full_io.h"

#include <string.h>

/*
This function performs a simple hash-based verification
that the other side is holding the same password without
transmitting it in the clear.

server:           generate random key SK
server -> client: SK
client:           generate random key CK
client -> server: CK
server -> client: SHA1(P+CK)
client:           verify SHA1(P+CK) is correct.
client -> server  SHA1(P+SK)
server:           verify SHA(P+SK) is correct.

Note that the protocol is symmetric, so the same code
works for either side.
*/

#define RANDOM_KEY_LENGTH 64
#define LINK_AUTH_LINE_MAX 1024

static const char *auth_password_ident = "auth password sha1";

int link_auth_password( struct link *link, const char *password, time_t stoptime )
{
	int peer_authenticated = 0;
	int self_authenticated = 0;

	// Verify we are using the same procedure.
	char line[LINK_AUTH_LINE_MAX];
	link_printf(link,stoptime,"%s\n",auth_password_ident);
	link_readline(link,line,sizeof(line),stoptime);
	if(strcmp(line,auth_password_ident)) {
		debug(D_AUTH,"peer is not using password authentication.\n");
		return 0;
	}

	// Generate and send my challenge string
	debug(D_AUTH,"sending challenge data");
	char my_random_key[LINK_AUTH_LINE_MAX];
	string_cookie(my_random_key,RANDOM_KEY_LENGTH);
	link_printf(link,stoptime,"%s\n",my_random_key);

	// Read and parse the peer's random key.
	debug(D_AUTH,"receiving peer's challenge data");
	char peer_random_key[LINK_AUTH_LINE_MAX];
	if(!link_readline(link,peer_random_key,sizeof(peer_random_key),stoptime)) goto failure;

	// Compute and send SHA1( password + peer_random_key )
	debug(D_AUTH,"sending my response");
	char my_response[LINK_AUTH_LINE_MAX*2+1];
	unsigned char digest[SHA1_DIGEST_LENGTH];
	sprintf(my_response,"%s %s",password,peer_random_key);
	sha1_buffer(my_response,strlen(my_response),digest);
	link_printf(link,stoptime,"%s\n",sha1_string(digest));

	// Compute the expected value of SHA1( password + my_random_key )
	char expected_response[LINK_AUTH_LINE_MAX*2+1];
	sprintf(expected_response,"%s %s",password,my_random_key);
	sha1_buffer(expected_response,strlen(expected_response),digest);
	strcpy(expected_response,sha1_string(digest));

	// Get the peer's actual response.
	debug(D_AUTH,"getting peer's response");
	char actual_response[LINK_AUTH_LINE_MAX];
	if(!link_readline(link,actual_response,sizeof(actual_response),stoptime)) goto failure;

	// Send back whether we accept it or not, for troubleshooting
	if(!strcmp(expected_response,actual_response)) {
		debug(D_AUTH,"peer sent correct response");
		link_putstring(link,"ok\n",stoptime);
		peer_authenticated = 1;
	} else {
		debug(D_AUTH,"peer did not send correct response");
		link_putstring(link,"failure\n",stoptime);
		peer_authenticated = 0;
	}

	// Read back whether the peer accepted ours or not.
	if(!link_readline(link,line,sizeof(line),stoptime)) goto failure;
	if(!strcmp(line,"ok")) {
		debug(D_AUTH,"peer accepted my response");
		self_authenticated = 1;
	} else {
		debug(D_AUTH,"peer did not accept my response");
		self_authenticated = 0;
	}

	return (peer_authenticated && self_authenticated);

	failure:
	debug(D_AUTH,"failed to read response from peer");
	return 0;
}


/* vim: set noexpandtab tabstop=8: */
