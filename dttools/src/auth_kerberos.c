/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#if defined(HAS_KRB5)

#include "krb5.h"

#include "auth.h"
#include "catch.h"
#include "link.h"
#include "debug.h"
#include "xxmalloc.h"
#include "domain_name_cache.h"

#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#define SERVICE "host"
#define VERSION "dttools_auth_protocol_1"

int auth_kerberos_assert(struct link *link, time_t stoptime)
{
	int rc;
	krb5_context context;
	krb5_ccache ccdef;
	krb5_principal client, server;
	krb5_data cksum;
	krb5_auth_context auth_context = 0;
	krb5_ap_rep_enc_part *rep_ret;
	krb5_error *err_ret;
	int port;

	char addr[LINK_ADDRESS_MAX];
	char dname[DOMAIN_NAME_MAX];

	debug(D_AUTH, "kerberos: determining service name");

	link_address_remote(link, addr, &port);
	if(domain_name_cache_lookup_reverse(addr, dname)) {
		debug(D_AUTH, "kerberos: name of %s is %s", addr, dname);
		cksum.data = dname;
		cksum.length = strlen(dname);

		debug(D_AUTH, "kerberos: creating context");
		if(!krb5_init_context(&context)) {

			debug(D_AUTH, "kerberos: opening credential cache");
			if(!krb5_cc_default(context, &ccdef)) {

				debug(D_AUTH, "kerberos: loading my credentials");
				if(!krb5_cc_get_principal(context, ccdef, &client)) {

					char *name;
					krb5_unparse_name(context, client, &name);
					debug(D_AUTH, "kerberos: I am %s", name);
					free(name);

					debug(D_AUTH, "kerberos: building server principal");
					if(!krb5_sname_to_principal(context, dname, SERVICE, KRB5_NT_SRV_HST, &server)) {

						krb5_unparse_name(context, server, &name);
						debug(D_AUTH, "kerberos: expecting server %s", name);
						free(name);
						debug(D_AUTH, "kerberos: waiting for server");
						if(auth_barrier(link, "yes\n", stoptime) == 0) {
							debug(D_AUTH, "kerberos: authenticating with server");
							int fd = link_fd(link);
							link_nonblocking(link, 0);
							int result = krb5_sendauth(context, &auth_context, &fd, VERSION, client, server, AP_OPTS_MUTUAL_REQUIRED, &cksum, 0, ccdef, &err_ret, &rep_ret, 0);
							link_nonblocking(link, 1);
							if(result == 0) {
								debug(D_AUTH, "kerberos: credentials accepted!");
								krb5_free_ap_rep_enc_part(context, rep_ret);
								krb5_auth_con_free(context, auth_context);
								success = 1;
							} else {
								debug(D_AUTH, "kerberos: couldn't authenticate to server");
							}
							krb5_free_principal(context, server);
						} else {
							debug(D_AUTH, "kerberos: server couldn't load credentials");
						}
					} else {
						debug(D_AUTH, "kerberos: couldn't build server principal");
						auth_barrier(link, "no\n", stoptime);
					}
					krb5_free_principal(context, client);
				} else {
					debug(D_AUTH, "kerberos: couldn't retrieve my credentials");
					auth_barrier(link, "no\n", stoptime);
				}
				krb5_cc_close(context, ccdef);
			} else {
				debug(D_AUTH, "kerberos: couldn't open the credential cache");
				auth_barrier(link, "no\n", stoptime);
			}
			krb5_free_context(context);
		} else {
			debug(D_AUTH, "kerberos: couldn't create a context");
			auth_barrier(link, "no\n", stoptime);
		}
	} else {
		debug(D_AUTH, "kerberos: couldn't determine name of %s", addr);
		auth_barrier(link, "no\n", stoptime);
	}

	rc = 0;
	goto out;
out:
	if (rc) {
		auth_barrier(link, "no\n", stoptime);
	}
	return RCUNIX(rc);
}

int auth_kerberos_accept(struct link *link, char **subject, time_t stoptime)
{
	krb5_context context;
	krb5_auth_context auth_context = NULL;
	krb5_ticket *ticket;
	krb5_principal principal;
	krb5_keytab keytab;
	krb5_kt_cursor cursor;

	int success = 0;

	debug(D_AUTH, "kerberos: creating a context");
	if(!krb5_init_context(&context)) {

		debug(D_AUTH, "kerberos: computing my service name");
		if(!krb5_sname_to_principal(context, NULL, SERVICE, KRB5_NT_SRV_HST, &principal)) {
			char *name;
			krb5_unparse_name(context, principal, &name);
			debug(D_AUTH, "kerberos: I am %s", name);
			free(name);

			debug(D_AUTH, "kerberos: looking for a keytab");
			if(!krb5_kt_default(context, &keytab)) {
				debug(D_AUTH, "kerberos: attempting to open keytab");
				if(!krb5_kt_start_seq_get(context, keytab, &cursor)) {
					krb5_kt_close(context, keytab);

					debug(D_AUTH, "kerberos: waiting for client");
					if(auth_barrier(link, "yes\n", stoptime) == 0) {

						debug(D_AUTH, "kerberos: receiving client credentials");
						int fd = link_fd(link);
						link_nonblocking(link, 0);
						int result = krb5_recvauth(context, &auth_context, &fd, VERSION, principal, 0, 0, &ticket);
						link_nonblocking(link, 1);
						if(result == 0) {

							char myrealm[AUTH_SUBJECT_MAX];
							char userrealm[AUTH_SUBJECT_MAX];
							char username[AUTH_SUBJECT_MAX];

							debug(D_AUTH, "kerberos: parsing client name");

							strncpy(myrealm, principal->realm.data, principal->realm.length);
							myrealm[principal->realm.length] = 0;

							strncpy(userrealm, ticket->enc_part2->client->realm.data, ticket->enc_part2->client->realm.length);
							userrealm[ticket->enc_part2->client->realm.length] = 0;

							strncpy(username, ticket->enc_part2->client->data->data, ticket->enc_part2->client->data->length);
							username[ticket->enc_part2->client->data->length] = 0;

							debug(D_AUTH, "kerberos: user is %s@%s\n", username, userrealm);
							debug(D_AUTH, "kerberos: my realm is %s\n", myrealm);

							if(strcmp(myrealm, userrealm)) {
								debug(D_AUTH, "kerberos: sorry, you come from another realm\n");
							} else {
								debug(D_AUTH, "kerberos: local user is %s\n", username);
								*subject = xxstrdup(username);
								success = 1;
							}
							krb5_auth_con_free(context, auth_context);
						} else {
							debug(D_AUTH, "kerberos: couldn't receive client credentials");
						}
					} else {
						debug(D_AUTH, "kerberos: client couldn't load credentials");
					}
				} else {
					debug(D_AUTH, "kerberos: couldn't find Kerberos keytab");
					auth_barrier(link, "no\n", stoptime);
				}
			} else {
				debug(D_AUTH, "kerberos: couldn't find Kerberos keytab");
				auth_barrier(link, "no\n", stoptime);
			}
		} else {
			debug(D_AUTH, "kerberos: couldn't figure out my service name");
			auth_barrier(link, "no\n", stoptime);
		}
	} else {
		debug(D_AUTH, "kerberos: couldn't create kerberos context");
		auth_barrier(link, "no\n", stoptime);
	}

	if(getuid() != 0) {
		debug(D_AUTH, "kerberos: perhaps this didn't work because I am not run as root.");
	}

	return success;
}

int auth_kerberos_register(void)
{
	debug(D_AUTH, "kerberos: registered");
	return auth_register("kerberos", auth_kerberos_assert, auth_kerberos_accept);
}

#else

#include "debug.h"

int auth_kerberos_register(void)
{
	debug(D_AUTH, "kerberos: not compiled in");
	return 0;
}

#endif

/* vim: set noexpandtab tabstop=8: */
