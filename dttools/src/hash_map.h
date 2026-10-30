#ifndef HASH_MAP_H
#define HASH_MAP_H

typedef unsigned (*hash_func_t) (const char *key);

unsigned hash_string(const char *s);

struct hash_map *hash_map_create( unsigned bucket_count, hash_func_t func);

int    hash_map_insert( struct hash_map *map, const char *key, void *value );
void * hash_map_lookup( struct hash_map *map, const char *key );
void * hash_map_remove( struct hash_map *map, const char *key );
void * hash_map_remove_stable( struct hash_map *map, const char *key );

int    hash_map_size( struct hash_map *map );
double hash_map_load( struct hash_map *map );
void   hash_map_clear(struct hash_map *map, void (*delete_func)( void *) );
void   hash_map_delete( struct hash_map *map );

char ** hash_map_keys( struct hash_map *map );
void    hash_map_keys_delete( char **keys );
void    hash_map_keys_delete_vppp( char ***keys );

struct hash_map_cursor * hash_map_cursor_create( struct hash_map *map );

void hash_map_cursor_seek( struct hash_map_cursor *cursor, unsigned pos );
int  hash_map_cursor_get( struct hash_map_cursor *cursor, char **key, void **value );
void * hash_map_cursor_remove( struct hash_map_cursor *cursor );
int  hash_map_cursor_next( struct hash_map_cursor *cursor );
void hash_map_cursor_delete( struct hash_map_cursor *cursor );
void hash_map_cursor_delete_vpp( struct hash_map_cursor **cursor );

#define HASH_MAP_ITERATE_READONLY( map, key, value ) \
  for( struct hash_map_cursor *__cursor  __attribute__((cleanup(hash_map_cursor_delete_vpp))) = hash_map_cursor_create(map), *__keep_going=__cursor; __keep_going; __keep_going=0) \
    for( hash_map_cursor_seek(__cursor,0); hash_map_cursor_get(__cursor,&key,(void**)&value); hash_map_cursor_next(__cursor) )

#define HASH_MAP_ITERATE_SNAPSHOT( map, key, value ) \
  for( char **__keys __attribute__((cleanup(hash_map_keys_delete_vppp))) = hash_map_keys(map), **__keep_going=__keys; __keep_going; __keep_going=0) \
    for( int __i=0; key=__keys[__i], value=hash_map_lookup(map,key); __i++ )

#endif
