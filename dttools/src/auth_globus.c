/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifdef HAS_GLOBUS_GSS

#include "auth.h"
#include "catch.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdio.h>
#include <unistd.h>
#include <string.h>

#undef IOV_MAX

#include "globus_common.h"
#include "globus_gss_assist.h"

static gss_cred_id_t delegated_credential = GSS_C_NO_CREDENTIAL;
static int use_delegated_credential = 0;

static int read_token(void *link, void **bufp, size_t * sizep)
{
	char line[AUTH_LINE_MAX];
	time_t stoptime = time(0) + 3600;
	int result;

	if(link_readline(link, line, sizeof(line), stoptime)) {
		*sizep = atoi(line);
		*bufp = malloc(*sizep);
		if(*bufp) {
			result = link_read(link, *bufp, *sizep, stoptime);
			if(result == (int) *sizep) {
				return GLOBUS_SUCCESS;
			}
			free(*bufp);
		}
	}

	return GLOBUS_GSS_ASSIST_TOKEN_EOF;
}

static int write_token(void *link, void *buf, size_t size)
{
	time_t stoptime = time(0) + 3600;

	link_printf(link, stoptime, "%zu\n", size);
	if(link_putlstring(link, buf, size, stoptime) == (int) size) {
		return GLOBUS_SUCCESS;
	} else {
		return GLOBUS_GSS_ASSIST_TOKEN_EOF;
	}
}

static int auth_globus_assert(struct link *link, time_t stoptime)
{
	int rc;
	gss_cred_id_t credential = GSS_C_NO_CREDENTIAL;
	gss_ctx_id_t context = GSS_C_NO_CONTEXT;
	OM_uint32 major, minor, flags = 0;
	int token;
	char *reason = NULL;

	globus_module_activate(GLOBUS_GSI_GSS_ASSIST_MODULE);

	if(use_delegated_credential && delegated_credential != GSS_C_NO_CREDENTIAL) {
		debug(D_AUTH, "globus: using delegated credential");
		credential = delegated_credential;
		major = GSS_S_COMPLETE;
	} else {
		debug(D_AUTH, "globus: loading my credentials");
		major = globus_gss_assist_acquire_cred(&minor, GSS_C_INITIATE, &credential);
	}

	if(major == GSS_S_COMPLETE) {
		debug(D_AUTH, "globus: waiting for server to get ready");
		if(auth_barrier(link, "yes\n", stoptime) == 0) {
			debug(D_AUTH, "globus: authenticating with server");
			major = globus_gss_assist_init_sec_context(&minor, credential, &context, "GSI-NO-TARGET", 0, &flags, &token, read_token, link, write_token, link);
			if(major == GSS_S_COMPLETE) {
				debug(D_AUTH, "globus: credentials accepted!");
				gss_delete_sec_context(&minor, &context, GSS_C_NO_BUFFER);
			} else {
				globus_gss_assist_display_status_str(&reason, "", major, minor, token);
				debug(D_AUTH, "globus: credentials rejected: %s", reason ? reason : "unknown reason");
				THROW_QUIET(EACCES);
			}
		} else {
			debug(D_AUTH, "globus: server couldn't load credentials");
			THROW_QUIET(EACCES);
		}
	} else {
		debug(D_AUTH, "globus: couldn't load my credentials; did you grid-proxy-init?");
		auth_barrier(link, "no\n", stoptime);
		THROW_QUIET(EACCES);
	}

	rc = 0;
	goto out;
out:
	if(!use_delegated_credential) {
		gss_release_cred(&major, &credential);
	}
	globus_module_deactivate(GLOBUS_GSI_GSS_ASSIST_MODULE);
	free(reason);
	return RCUNIX(rc);
}

static int auth_globus_accept(struct link *link, char **subject, time_t stoptime)
{
	gss_cred_id_t credential = GSS_C_NO_CREDENTIAL;
	gss_ctx_id_t context = GSS_C_NO_CONTEXT;
	OM_uint32 major, minor, flags = 0;
	int token;
	int success = 0;

	globus_module_activate(GLOBUS_GSI_GSS_ASSIST_MODULE);

	*subject = 0;

	debug(D_AUTH, "globus: loading my credentials");
	major = globus_gss_assist_acquire_cred(&minor, GSS_C_ACCEPT, &credential);
	if(major == GSS_S_COMPLETE) {

		debug(D_AUTH, "globus: waiting for client to get ready");
		if(auth_barrier(link, "yes\n", stoptime) == 0) {

			delegated_credential = GSS_C_NO_CREDENTIAL;
			debug(D_AUTH, "globus: authenticating client");
			major = globus_gss_assist_accept_sec_context(&minor, &context, credential, subject, &flags, 0, &token, &delegated_credential, read_token, link, write_token, link);
			if(major == GSS_S_COMPLETE) {
				debug(D_AUTH, "globus: accepted client %s", *subject);
				if(delegated_credential != GSS_C_NO_CREDENTIAL) {
					debug(D_AUTH, "globus: client delegated its credentials");
				}
				success = 1;
				gss_delete_sec_context(&minor, &context, GSS_C_NO_BUFFER);
			} else {
				char *reason;
				globus_gss_assist_display_status_str(&reason, "", major, minor, token);
				if(!reason)
					reason = xxstrdup("unknown reason");
				debug(D_AUTH, "globus: couldn't authenticate client: %s", reason);
				if(reason)
					free(reason);
			}
		} else {
			debug(D_AUTH, "globus: client couldn't load credentials");
		}
		gss_release_cred(&major, &credential);
	} else {
		debug(D_AUTH, "globus: couldn't load my credentials: did you run grid-proxy-init?");
		auth_barrier(link, "no\n", stoptime);
	}

	globus_module_deactivate(GLOBUS_GSI_GSS_ASSIST_MODULE);

	return success;
}

void auth_globus_use_delegated_credential(int yesno)
{
	use_delegated_credential = yesno;
}

int auth_globus_has_delegated_credential(void)
{
	return delegated_credential != GSS_C_NO_CREDENTIAL;
}

int auth_globus_register(void)
{
	debug(D_AUTH, "globus: registered");
	return auth_register("globus", auth_globus_assert, auth_globus_accept);
}

/*
Ugly hack: Globus 4.0 relies on the dynamic linker to
add extension modules, even when statically linked.
This supresses certain linker errors.
*/

int _dl_load_lock = 0;

#else

#include "debug.h"

int auth_globus_register(void)
{
	debug(D_AUTH, "globus: not compiled in");
	return 0;
}

int auth_globus_has_delegated_credential(void)
{
	return 0;
}

void auth_globus_use_delegated_credential(int yesno)
{
}

#endif

/* vim: set noexpandtab tabstop=8: */
