
#ifndef INT_HASH_H
#define INT_HASH_H

typedef struct int_hash_s int_hash;
typedef unsigned (*int_hash_func_t) ( unsigned long key );

int_hash * int_hash_create(unsigned long buckets, int_hash_func_t func);
void int_hash_delete(int_hash * h);
int int_hash_insert(int_hash * h, unsigned long key, const void * value);
void * int_hash_lookup(int_hash * h, unsigned long key);
void * int_hash_remove(int_hash * h, unsigned long key);
void int_hash_firstkey(int_hash * h);
int int_hash_nextkey(int_hash * h, unsigned long * key, void ** value);
int int_hash_remove_curr(int_hash * h);
int int_hash_size(int_hash * h);
unsigned hash_unsigned_long(unsigned long k);

#endif // INT_HASH_H_
