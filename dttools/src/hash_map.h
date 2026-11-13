#ifndef HASH_TABLE_H
#define HASH_TABLE_H

typedef unsigned (*hash_func_t) (const char *key);

unsigned hash_string(const char *s);

struct hash_table *hash_table_create( unsigned bucket_count, hash_func_t func);

int    hash_table_insert( struct hash_table *table, const char *key, void *value );
void * hash_table_lookup( struct hash_table *table, const char *key );
void * hash_table_remove( struct hash_table *table, const char *key );

int    hash_table_size( struct hash_table *table );
double hash_table_load( struct hash_table *table );
void   hash_table_clear(struct hash_table *table, void (*delete_func)( void *) );
void   hash_table_delete( struct hash_table *table );

char ** hash_table_keys( struct hash_table *table );
void    hash_table_keys_delete( char **keys );
void    hash_table_keys_delete_ppp( char ***keys );

struct hash_table_cursor * hash_table_cursor_create( struct hash_table *table );

void hash_table_cursor_seek( struct hash_table_cursor *cursor, unsigned pos );
int  hash_table_cursor_get( struct hash_table_cursor *cursor, char **key, void **value );
void * hash_table_cursor_remove( struct hash_table_cursor *cursor );
int  hash_table_cursor_next( struct hash_table_cursor *cursor );
void hash_table_cursor_delete( struct hash_table_cursor *cursor );
void hash_table_cursor_delete_pp( struct hash_table_cursor **cursor );

#define HASH_TABLE_ITERATE_READONLY( table, key, value ) \
  for( struct hash_table_cursor *__cursor  __attribute__((cleanup(hash_table_cursor_delete_pp))) = hash_table_cursor_create(table), *__keep_going=__cursor; __keep_going; __keep_going=0) \
    for( hash_table_cursor_seek(__cursor,0); hash_table_cursor_get(__cursor,&key,(void**)&value); hash_table_cursor_next(__cursor) )

#define HASH_TABLE_ITERATE_SNAPSHOT( table, key, value ) \
  for( char **__keys __attribute__((cleanup(hash_table_keys_delete_ppp))) = hash_table_keys(table), **__keep_going=__keys; __keep_going; __keep_going=0) \
    for( int __i=0; key=__keys[__i], value=hash_table_lookup(table,key); __i++ )

#endif
