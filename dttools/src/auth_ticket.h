/*
Copyright (C) 2010- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_table.h"

int auth_ticket_register(void);

/* Generates a hash table full of <public key digest, private key>
 * pairs to be stored in the auth hash table (with key "ticket").
 *
 * The tickets argument is a comma separated list of (private key)
 * ticket files. Whitespace is not ignored.
 *
 * All value strings normally must be freed when authentication finishes.
 */
//struct *hash_table auth_ticket_gather (const char *tickets);
