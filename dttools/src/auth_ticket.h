/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

int auth_ticket_register(void);

/* Clear all tickets stored for server authentication. */
void auth_ticket_clear(void);

/* Add a digest/ticket pair for server authentication. */
void auth_ticket_add(const char *digest, const char *ticket);

/* Add tickets to client side tickets to try or, if NULL, load
 * tickets from current working directory.
 */
void auth_ticket_load(const char *tickets);
