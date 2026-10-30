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
void   hash_map_delete( struct hash_map *map );

struct hash_map_cursor * hash_map_cursor_create( struct hash_map *map );

void hash_map_cursor_seek( struct hash_map_cursor *cursor, unsigned pos );
int  hash_map_cursor_get( struct hash_map_cursor *cursor, void **value );
int  hash_map_cursor_next( struct hash_map_cursor *cursor );
void hash_map_cursor_delete( struct hash_map_cursor *cursor );
void hash_map_cursor_delete_vpp( struct hash_map_cursor **cursor );

#define HASH_MAP_ITERATE( map, key, item )					\
  for( struct hash_map_cursor *cursor  __attribute__((cleanup(hash_map_cursor_delete_vpp))) = hash_map_cursor_create(map), *__keep_going=cursor; __keep_going; __keep_going=0) \
    for( hash_map_cursor_seek(cursor,0); hash_map_cursor_get(cursor,(void**)&item); hash_map_cursor_next(cursor) )

#endif
