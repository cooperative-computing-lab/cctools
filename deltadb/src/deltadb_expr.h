#ifndef DELTADB_EXPR_H
#define DELTADB_EXPR_H

struct deltadb_expr * deltadb_expr_create( const char *str, struct deltadb_expr *next );
int deltadb_expr_matches( struct deltadb_expr *e, struct jx *jobject );


#endif
