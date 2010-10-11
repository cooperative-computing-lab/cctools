#ifndef TEXT_LIST_H
#define TEXT_LIST_H

struct text_list  *text_list_create();
struct text_list  *text_list_load( const char *path );
char              *text_list_get( struct text_list *t, int i);
int                text_list_append( struct text_list *t, const char *item );
int		   text_list_size( struct text_list *t );
void               text_list_delete( struct text_list *t );


#endif
