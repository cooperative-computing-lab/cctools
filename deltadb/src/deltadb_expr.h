/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DELTADB_EXPR_H
#define DELTADB_EXPR_H

struct deltadb_expr * deltadb_expr_create( const char *str, struct deltadb_expr *next );
int deltadb_expr_matches( struct deltadb_expr *e, struct jx *jobject );


#endif
