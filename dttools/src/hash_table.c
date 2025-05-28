/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "hash_table.h"
#include "debug.h"

#include <stdlib.h>
#include <string.h>

#define DEFAULT_SIZE 127
#define DEFAULT_MAX_LOAD 0.75
#define DEFAULT_MIN_LOAD 0.125
#define DEFAULT_FUNC hash_string

struct entry {
	char *key;
	void *value;
	unsigned hash;
	struct entry *next;
};

struct hash_table {
	hash_func_t hash_func;
	int bucket_count;
	int size;
	struct entry **buckets;
	int ibucket;
	struct entry *ientry;

	/* for memory safety, hash_table_nextkey cannot be called in the same
	 * iteration if hash_table_insert or hash_table_remove has been called.
	 * In such case, the executable will be terminated with a fatal message.
	 * If the table should be modified during iterations, consider
	 * using the array keys from hash_table_keys_array. (If so, remember
	 * to free it afterwards with hash_table_free_keys_array.) */
	int cant_iterate_yet;
};

struct hash_table *hash_table_create(int bucket_count, hash_func_t func)
{
	struct hash_table *h;

	h = (struct hash_table *)malloc(sizeof(struct hash_table));
	if (!h)
		return 0;

	if (bucket_count < 1)
		bucket_count = DEFAULT_SIZE;
	if (!func)
		func = DEFAULT_FUNC;

	h->size = 0;
	h->cant_iterate_yet = 0;
	h->hash_func = func;
	h->bucket_count = bucket_count;
	h->buckets = (struct entry **)calloc(bucket_count, sizeof(struct entry *));
	if (!h->buckets) {
		free(h);
		return 0;
	}

	return h;
}

void hash_table_clear(struct hash_table *h, void (*delete_func)(void *))
{
	struct entry *e, *f;
	int i;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			if (delete_func)
				delete_func(e->value);
			free(e->key);
			free(e);
			e = f;
		}
	}

	for (i = 0; i < h->bucket_count; i++) {
		h->buckets[i] = 0;
	}

	/* buckets went away, thus a nextkey would be invalid */
	h->cant_iterate_yet = 1;
}

void hash_table_delete(struct hash_table *h)
{
	hash_table_clear(h, 0);
	free(h->buckets);
	free(h);
}

char **hash_table_keys_array(struct hash_table *h)
{
	char **keys = (char **)malloc(sizeof(char *) * (h->size + 1));
	int ikey = 0;

	struct entry *e, *f;
	int i;

	for (i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			keys[ikey] = strdup(e->key);
			ikey++;
			f = e->next;
			e = f;
		}
	}

	keys[h->size] = NULL;

	return keys;
}

void hash_table_free_keys_array(char **keys)
{
	int i = 0;
	while (keys[i]) {
		free(keys[i]);
		i++;
	}

	free(keys);
}

void *hash_table_lookup(struct hash_table *h, const char *key)
{
	struct entry *e;
	unsigned hash, index;

	hash = h->hash_func(key);
	index = hash % h->bucket_count;
	e = h->buckets[index];

	while (e) {
		if (hash == e->hash && !strcmp(key, e->key)) {
			return e->value;
		}
		e = e->next;
	}

	return 0;
}

int hash_table_size(struct hash_table *h)
{
	return h->size;
}

double hash_table_load(struct hash_table *h)
{
	return ((double)h->size) / h->bucket_count;
}

static int insert_to_buckets_aux(struct entry **buckets, int bucket_count, struct entry *new_entry)
{
	unsigned index;
	struct entry *e;

	index = new_entry->hash % bucket_count;
	e = buckets[index];

	while (e) {
		/* check that this key does not already exist in the table */
		if (new_entry->hash == e->hash && !strcmp(new_entry->key, e->key)) {
			return 0;
		}
		e = e->next;
	}

	new_entry->next = buckets[index];
	buckets[index] = new_entry;

	return 1;
}

static int hash_table_double_buckets(struct hash_table *h)
{
	int new_count = (2 * (h->bucket_count + 1)) - 1;
	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			e->next = NULL;
			insert_to_buckets_aux(new_buckets, new_count, e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	/* structure of hash table changed completely, thus a nextkey would be incorrect. */
	h->cant_iterate_yet = 1;

	return 1;
}

static int hash_table_reduce_buckets(struct hash_table *h)
{
	int new_count = ((h->bucket_count + 1) / 2) - 1;

	/* DEFAULT_SIZE is the minimum size */
	if (new_count <= DEFAULT_SIZE) {
		return 1;
	}

	/* Table cannot be reduced above DEFAULT_MAX_LOAD */
	if (((float)h->size / new_count) >= DEFAULT_MAX_LOAD) {
		return 1;
	}

	struct hash_table *hn = hash_table_create(new_count, h->hash_func);
	if (!hn) {
		return 0;
	}

	struct entry **new_buckets = (struct entry **)calloc(new_count, sizeof(struct entry *));
	if (!new_buckets) {
		return 0;
	}

	struct entry *e, *f;
	for (int i = 0; i < h->bucket_count; i++) {
		e = h->buckets[i];
		while (e) {
			f = e->next;
			e->next = NULL;
			insert_to_buckets_aux(new_buckets, new_count, e);
			e = f;
		}
	}

	/* Make the old point to the new */
	free(h->buckets);
	h->buckets = new_buckets;
	h->bucket_count = new_count;

	/* structure of hash table changed completely, thus a nextkey would be incorrect. */
	h->cant_iterate_yet = 1;

	return 1;
}

int hash_table_insert(struct hash_table *h, const char *key, const void *value)
{
	if (((float)h->size / h->bucket_count) > DEFAULT_MAX_LOAD)
		hash_table_double_buckets(h);

	struct entry *e = (struct entry *)malloc(sizeof(struct entry));
	if (!e) {
		return 0;
	}

	e->key = strdup(key);
	if (!e->key) {
		free(e);
		return 0;
	}

	e->value = (void *)value;
	e->hash = h->hash_func(e->key);

	int inserted = insert_to_buckets_aux(h->buckets, h->bucket_count, e);
	if (inserted) {
		h->size++;

		/* inserting cause different behaviours with nextkey (e.g., sometimes the new
		 * key would be included or skipped in the iteration */
		h->cant_iterate_yet = 1;
	}

	return inserted;
}

void *hash_table_remove(struct hash_table *h, const char *key)
{
	struct entry *e, *f;
	void *value;
	unsigned hash, index;

	hash = h->hash_func(key);
	index = hash % h->bucket_count;
	e = h->buckets[index];
	f = 0;

	while (e) {
		if (hash == e->hash && !strcmp(key, e->key)) {
			if (f) {
				f->next = e->next;
			} else {
				h->buckets[index] = e->next;
			}
			value = e->value;
			free(e->key);
			free(e);
			h->size--;
			return value;
		}
		f = e;
		e = e->next;
	}

	if (((float)h->size / h->bucket_count) < DEFAULT_MIN_LOAD) {
		hash_table_reduce_buckets(h);
	}

	return 0;
}

int hash_table_fromkey(struct hash_table *h, const char *key)
{
	h->cant_iterate_yet = 0;

	if (!key) {
		/* treat NULL as a special case equivalent to firstkey */
		hash_table_firstkey(h);
		return 1;
	}

	unsigned hash = h->hash_func(key);
	h->ibucket = hash % h->bucket_count;
	h->ientry = h->buckets[h->ibucket];

	while (h->ientry) {
		if (hash == h->ientry->hash && !strcmp(key, h->ientry->key)) {
			return 1;
		}
		h->ientry = h->ientry->next;
	}

	hash_table_firstkey(h);
	return 0;
}

void hash_table_firstkey(struct hash_table *h)
{
	h->cant_iterate_yet = 0;

	h->ientry = 0;
	for (h->ibucket = 0; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry)
			break;
	}
}

int hash_table_nextkey(struct hash_table *h, char **key, void **value)
{
	if (h->cant_iterate_yet) {
		fatal("cctools bug: the hash table iteration has not been reset since last modification");
	}

	if (h->ientry) {
		*key = h->ientry->key;
		*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if (!h->ientry) {
			h->ibucket++;
			for (; h->ibucket < h->bucket_count; h->ibucket++) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry)
					break;
			}
		}
		return 1;
	} else {
		return 0;
	}
}

void hash_table_randomkey(struct hash_table *h, int *offset_bookkeep)
{
	h->cant_iterate_yet = 0;

	h->ientry = 0;
	if (h->bucket_count < 1) {
		return;
	}

	int ibucket_start = random() % h->bucket_count;

	for (h->ibucket = ibucket_start; h->ibucket < h->bucket_count; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry) {
			*offset_bookkeep = h->ibucket;
			return;
		}
	}

	for (h->ibucket = 0; h->ibucket < ibucket_start; h->ibucket++) {
		h->ientry = h->buckets[h->ibucket];
		if (h->ientry) {
			*offset_bookkeep = h->ibucket;
			return;
		}
	}
}

int hash_table_nextkey_with_offset(struct hash_table *h, int offset_bookkeep, char **key, void **value)
{
	if (h->cant_iterate_yet) {
		fatal("cctools bug: the hash table iteration has not been reset since last modification");
	}

	if (h->bucket_count < 1) {
		return 0;
	}

	offset_bookkeep = offset_bookkeep % h->bucket_count;

	if (h->ientry) {
		*key = h->ientry->key;
		*value = h->ientry->value;

		h->ientry = h->ientry->next;
		if (!h->ientry) {
			h->ibucket = (h->ibucket + 1) % h->bucket_count;
			for (; h->ibucket != offset_bookkeep; h->ibucket = (h->ibucket + 1) % h->bucket_count) {
				h->ientry = h->buckets[h->ibucket];
				if (h->ientry) {
					break;
				}
			}
		}
		return 1;
	}

	return 0;
}

typedef unsigned long int ub4; /* unsigned 4-byte quantities */
typedef unsigned char ub1;     /* unsigned 1-byte quantities */

#define hashsize(n) ((ub4)1 << (n))
#define hashmask(n) (hashsize(n) - 1)

/*
--------------------------------------------------------------------
mix -- mix 3 32-bit values reversibly.
For every delta with one or two bits set, and the deltas of all three
  high bits or all three low bits, whether the original value of a,b,c
  is almost all zero or is uniformly distributed,
* If mix() is run forward or backward, at least 32 bits in a,b,c
  have at least 1/4 probability of changing.
* If mix() is run forward, every bit of c will change between 1/3 and
  2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
mix() was built out of 36 single-cycle latency instructions in a
  structure that could supported 2x parallelism, like so:
	  a -= b;
	  a -= c; x = (c>>13);
	  b -= c; a ^= x;
	  b -= a; x = (a<<8);
	  c -= a; b ^= x;
	  c -= b; x = (b>>13);
	  ...
  Unfortunately, superscalar Pentiums and Sparcs can't take advantage
  of that parallelism.  They've also turned some of those single-cycle
  latency instructions into multi-cycle latency instructions.  Still,
  this is the fastest good hash I could find.  There were about 2^^68
  to choose from.  I only looked at a billion or so.
--------------------------------------------------------------------
*/

#define mix(a, b, c) \
	{ \
		a -= b; \
		a -= c; \
		a ^= (c >> 13); \
		b -= c; \
		b -= a; \
		b ^= (a << 8); \
		c -= a; \
		c -= b; \
		c ^= (b >> 13); \
		a -= b; \
		a -= c; \
		a ^= (c >> 12); \
		b -= c; \
		b -= a; \
		b ^= (a << 16); \
		c -= a; \
		c -= b; \
		c ^= (b >> 5); \
		a -= b; \
		a -= c; \
		a ^= (c >> 3); \
		b -= c; \
		b -= a; \
		b ^= (a << 10); \
		c -= a; \
		c -= b; \
		c ^= (b >> 15); \
	}

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  len     : the length of the key, counting by bytes
  initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 6*len+35 instructions. The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements. If you are hashing n strings (ub1 **)k, do it like
this: for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h); By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may
use this code any way you wish, private, educational, or commercial.  It's free. See
http://burtleburtle.net/bob/hash/evahash.html Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

/* Ignoring implicit-fallthrough warnings, as fallthrough is what we want in the following switch-case */
/* *k the key */
/* length the length of the key */
/* initval the previous hash, or an arbitrary value */
static ub4 jenkins_hash(register const ub1 *k, register ub4 length, register ub4 initval)
{
	register ub4 a, b, c, len; /* Set up the internal state */
	len = length;
	a = b = 0x9e3779b9;			   /* the golden ratio; an arbitrary value */
	c = initval; /* the previous hash value */ /*---------------------------------------- handle most of the key */
	while (len >= 12) {
		a += (k[0] + ((ub4)k[1] << 8) + ((ub4)k[2] << 16) + ((ub4)k[3] << 24));
		b += (k[4] + ((ub4)k[5] << 8) + ((ub4)k[6] << 16) + ((ub4)k[7] << 24));
		c += (k[8] + ((ub4)k[9] << 8) + ((ub4)k[10] << 16) + ((ub4)k[11] << 24));
		mix(a, b, c);
		k += 12;
		len -= 12;
	}
	/*------------------------------------- handle the last 11 bytes */
	c += length;
	switch (len) { /* all the case statements fall through */
	case 11:
		c += ((ub4)k[10] << 24);
		/* falls through */
	case 10:
		c += ((ub4)k[9] << 16);
		/* falls through */
	case 9:
		/* the first byte of c is reserved for the length */
		c += ((ub4)k[8] << 8);
		/* falls through */
	case 8:
		b += ((ub4)k[7] << 24);
		/* falls through */
	case 7:
		b += ((ub4)k[6] << 16);
		/* falls through */
	case 6:
		b += ((ub4)k[5] << 8);
		/* falls through */
	case 5:
		b += k[4];
		/* falls through */
	case 4:
		a += ((ub4)k[3] << 24);
		/* falls through */
	case 3:
		a += ((ub4)k[2] << 16);
		/* falls through */
	case 2:
		a += ((ub4)k[1] << 8);
		/* falls through */
	case 1:
		a += k[0];
		/* falls through */
	case 0:
	default:
		/* case 0: nothing left to add */
		break;
	}
	mix(a, b, c);
	/*-------------------------------------------- report the result */
	return c;
}

unsigned hash_string(const char *s)
{
	return jenkins_hash((const ub1 *)s, strlen(s), 0);
}

/* vim: set noexpandtab tabstop=8: */
