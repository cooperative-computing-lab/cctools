/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef AUTH_TICKET_H
#define AUTH_TICKET_H

typedef char * (*auth_ticket_server_callback_t)(const char *);

int auth_ticket_register(void);

/* Callback to lookup a ticket. Returns free()able char * to ticket public key */
void auth_ticket_server_callback(auth_ticket_server_callback_t sc);

/* Add tickets to client side tickets to try or, if NULL, load
 * tickets from current working directory.
 */
void auth_ticket_load(const char *tickets);

#endif
