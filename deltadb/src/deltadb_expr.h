/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_EXPR_H
#define DELTADB_EXPR_H

#include "jx.h"

struct deltadb_expr * deltadb_expr_create( const char *str, struct deltadb_expr *next );
void deltadb_expr_delete( struct deltadb_expr *e );
int deltadb_expr_matches( struct deltadb_expr *e, struct jx *jobject );


#endif
