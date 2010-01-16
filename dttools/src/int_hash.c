#include <stdio.h>
#include <stdlib.h>
#include "int_hash.h"

#define DEFAULT_FUNC hash_unsigned_long

struct entry_s {
	unsigned long key;
	void * value;
	struct entry_s * next;
};
typedef struct entry_s entry;

struct int_hash_s {
	int_hash_func_t hash_func;
	entry ** buckets;
	int bucket_count_pow;
	int size;
	int ibucket;
	entry *ientry;
	entry *ipreventry;
	int iremovedprev;
	int collisions;
};

typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)
#define hashbucket(h, key) (h->hash_func(key) & hashmask(h->bucket_count_pow))

int_hash * int_hash_create(unsigned long bucket_power, int_hash_func_t func)
{
	int_hash * h = malloc(sizeof(*h));
	if (!h) return 0;

	h->buckets = calloc(hashsize(bucket_power), sizeof(entry));
	if (!h->buckets) return 0;

	h->bucket_count_pow = bucket_power;
	h->size = 0;
	h->ibucket = 0;
	h->ientry = 0;
	h->ipreventry = 0;
	h->iremovedprev = 0;
	h->collisions = 0;

	if (!func) h->hash_func = DEFAULT_FUNC;
	else h->hash_func = func;

	return h;
	
}

int int_hash_size(int_hash * h)
{
	if (!h) return 0;
	return h->size;
}

int int_hash_collisions(int_hash * h)
{
	if (!h) return 0;
	return h->collisions;
}

void free_entry(entry * e)
{
	entry * next;
	while (e)
	{
		next = e->next;
		free(e);
		e = next;
	}
}

void int_hash_delete(int_hash * h)
{
	if (!h) return;

	int i;
	for (i=0; i<hashsize(h->bucket_count_pow); i++)
	{
		free_entry(h->buckets[i]);
	}
	if (h->buckets) free(h->buckets);

	free(h);
}

entry * get_entry(int_hash * h, unsigned long key)
{
	unsigned bucket = h->hash_func(key) & hashmask(h->bucket_count_pow);
	entry * e = h->buckets[bucket];

	if (e) h->collisions++;
	while (e)
	{
		if (e->key == key) return e;
		e = e->next;
	}

	return 0;
}

int int_hash_insert(int_hash * h, unsigned long key, const void * value)
{
	// Assume the user is in charge of passing good pointers.
	//if (!h) return 0;
	//unsigned bucket = h->hash_func(key) & hashmask(h->bucket_count_pow);
	unsigned bucket = hashbucket(h, key);
	entry *e = h->buckets[bucket];
	while (e)
	{
		if (e->key == key) return 0;
		e = e->next;
	}

	// We did not find e, so insert it.
	e = malloc(sizeof(entry));
	e->key = key;
	e->value = (void *) value;
	e->next = h->buckets[bucket];
	h->buckets[bucket] = e;
	h->size++;
	return 1;

}

void * int_hash_lookup(int_hash * h, unsigned long key)
{
	// Assume the user is in charge of passing good pointers.
	//if (!h) return 0;
	unsigned bucket = h->hash_func(key) & hashmask(h->bucket_count_pow);

	entry *e = h->buckets[bucket];
	while (e)
	{
		if (e->key == key) return e->value;
		e = e->next;
	}

	return 0;

}

// Assume the user already did the lookup and knows there is
// nothing to check. This is basically to prevent lookup from
// happening twice to help performance.
// Should be used with caution.
int int_hash_insert_no_lookup(int_hash * h, unsigned long key, void * value)
{
	//unsigned bucket = h->hash_func(key) & hashmask(h->bucket_count_pow);
	unsigned bucket = hashbucket(h, key);

	// We did not find e, so insert it.
	entry * e = malloc(sizeof(entry));
	e->key = key;
	e->value = (void *) value;
	e->next = h->buckets[bucket];
	h->buckets[bucket] = e;
	h->size++;
	return 1;

}

/* Try to optimize performance at the potential cost of readability
int int_hash_insert(int_hash * h, unsigned long key, const void * value)
{
	if (!h) return 0;
	unsigned bucket;

	entry * e = get_entry(h, key);
	if (e)
	{
		return 0;
	}
	else
	{
		bucket = h->hash_func(key) % h->bucket_count;
		e = malloc(sizeof(*e));
		e->key = key;
		e->value = (void *) value;
		e->next = 0;
		if (!h->buckets[bucket])
		{
			h->buckets[bucket] = e;
		}
		else
		{
			e->next = h->buckets[bucket];
			h->buckets[bucket] = e;
		}
		h->size++;
		return 1;
	}

}

void * int_hash_lookup(int_hash * h, unsigned long key)
{
	if (!h) return 0;

	entry * e = get_entry(h, key);
	if (e) return e->value;
	else return 0;
}
*/

void * int_hash_remove(int_hash * h, unsigned long key)
{
	if (!h) return 0;

	unsigned bucket = h->hash_func(key) & hashmask(h->bucket_count_pow);
	void * value;
	entry * e = h->buckets[bucket];

	if (!e) return 0;

	// Delete the first one if that matches.
	if (e->key == key)
	{
		h->buckets[bucket] = e->next;
		value = e->value;
		free(e);
		h->size--;
		return value;
	}

	// Otherwise scroll through, keeping track
	// of previous ones for to keep the list intact.
	entry * prev_e = e;
	e = e->next;
	while (e)
	{
		if (e->key == key)
		{
			value = e->value;
			prev_e->next = e->next;
			free(e);
			return value;
		}
		else
		{
			prev_e = e;
			e = e->next;
		}
	}

	return 0;
}

void int_hash_nextbucket(int_hash * h, int start_bucket)
{
	for (h->ibucket = start_bucket; h->ibucket < hashsize(h->bucket_count_pow); h->ibucket++)
	{
		if (h->buckets[h->ibucket])
		{
			h->ipreventry = 0;
			h->iremovedprev = 0;
			h->ientry = h->buckets[h->ibucket];
			break;
		}
	}
}

void int_hash_firstkey(int_hash * h)
{
	if (!h) return;

	h->ientry = 0;
	h->ipreventry = 0;
	h->iremovedprev = 0;
	h->ibucket = -1;
	// Don't get the first one with this new method.
	//int_hash_nextbucket(h, 0);
}

int int_hash_nextkey(int_hash * h, unsigned long * key, void ** value)
{
/*
The problem is that we get the value and move it forward, so when we
say "remove curr", it's actually removing the next one in the bucket.

Instead, hold ientry as the previous, and in this function increment
it and THEN get the value. This means you need to call next_bucket
at the beginning instead of the end, and firstkey only has to set
ientry to 0 rather than the first entry.

Oh, but then how do you know you've hit the end. Crap, maybe use
the size, or the number of buckets?
*/
	if (h->iremovedprev)
	{
		// We removed the last one in the list and
		// now we are at the end.
		if (!h->ientry) return 0;

		*key = h->ientry->key;
		*value = h->ientry->value;
		h->iremovedprev = 0;
		return 1;
	}

	h->ipreventry = h->ientry;

	// If we have a current entry, get the next one in the list.
	if (h->ientry) h->ientry = h->ientry->next;

	// If we did not have a current entry, or we came to the end of
	// a bucket, get the first entry in the next populated bucket.
	if (!h->ientry) int_hash_nextbucket(h, h->ibucket+1);

	// If we still don't have an entry, then we're at the end.
	if (!h->ientry) return 0;

	// Now, we know we're good.
	*key = h->ientry->key;
	*value = h->ientry->value;

	return 1;
/*
	if (h->ientry)
	{
		// If the last we just removed one, then the "next"
		// from the user's perspective is the current by
		// our reckoning.
		if (h->iremovedprev)
		{
			*key = h->ientry->key;
			*value = h->ientry->value;
			h->iremovedprev = 0;
			return 1;
		}
		*key = h->ientry->key;
		*value = h->ientry->value;

		h->ipreventry = h->ientry;
		h->ientry = h->ientry->next;
		if (!h->ientry)
		{
			int_hash_nextbucket(h, h->ibucket+1);
		}

		return 1;
	}
	else
	{
		return 0;
	}
*/
}

int int_hash_remove_curr(int_hash * h)
{
	if (h->ientry)
	{
		// This is the first entry in the bucket
		if (!h->ipreventry)
		{
			h->buckets[h->ibucket] = h->ientry->next;
			free(h->ientry);
			h->ientry = h->buckets[h->ibucket];
			if (!h->ientry) int_hash_nextbucket(h, h->ibucket+1);
			h->iremovedprev = 1;
			h->size--;
			return 1;
		}
		else
		{
			h->ipreventry->next = h->ientry->next;
			free(h->ientry);
			h->ientry = h->ipreventry->next;
			if (!h->ientry) int_hash_nextbucket(h, h->ibucket+1);
			h->iremovedprev = 1;
			h->size--;
			return 1;
		}
	}
	return 0;
}

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
#define mix(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>13); \
  b -= c; b -= a; b ^= (a<<8); \
  c -= a; c -= b; c ^= (b>>13); \
  a -= b; a -= c; a ^= (c>>12);  \
  b -= c; b -= a; b ^= (a<<16); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>3);  \
  b -= c; b -= a; b ^= (a<<10); \
  c -= a; c -= b; c ^= (b>>15); \
}

/*
--------------------------------------------------------------------
hash() -- hash a variable-length key into a 32-bit value
  k       : the key (the unaligned variable-length array of bytes)
  len     : the length of the key, counting by bytes
  initval : can be any 4-byte value
Returns a 32-bit value.  Every bit of the key affects every bit of
the return value.  Every 1-bit and 2-bit delta achieves avalanche.
About 6*len+35 instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 32 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (ub1 **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.  You may use this
code any way you wish, private, educational, or commercial.  It's free.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^^32 is
acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

ub4 hash( k, length, initval)
register ub1 *k;        /* the key */
register ub4  length;   /* the length of the key */
register ub4  initval;  /* the previous hash, or an arbitrary value */
{
   register ub4 a,b,c,len;

   /* Set up the internal state */
   len = length;
   a = b = 0x9e3779b9;  /* the golden ratio; an arbitrary value */
   c = initval;         /* the previous hash value */

   /*---------------------------------------- handle most of the key */
   while (len >= 12)
   {
      a += (k[0] +((ub4)k[1]<<8) +((ub4)k[2]<<16) +((ub4)k[3]<<24));
      b += (k[4] +((ub4)k[5]<<8) +((ub4)k[6]<<16) +((ub4)k[7]<<24));
      c += (k[8] +((ub4)k[9]<<8) +((ub4)k[10]<<16)+((ub4)k[11]<<24));
      mix(a,b,c);
      k += 12; len -= 12;
   }

   /*------------------------------------- handle the last 11 bytes */
   c += length;
   switch(len)              /* all the case statements fall through */
   {
   case 11: c+=((ub4)k[10]<<24);
   case 10: c+=((ub4)k[9]<<16);
   case 9 : c+=((ub4)k[8]<<8);
      /* the first byte of c is reserved for the length */
   case 8 : b+=((ub4)k[7]<<24);
   case 7 : b+=((ub4)k[6]<<16);
   case 6 : b+=((ub4)k[5]<<8);
   case 5 : b+=k[4];
   case 4 : a+=((ub4)k[3]<<24);
   case 3 : a+=((ub4)k[2]<<16);
   case 2 : a+=((ub4)k[1]<<8);
   case 1 : a+=k[0];
     /* case 0: nothing left to add */
   }
   mix(a,b,c);
   /*-------------------------------------------- report the result */
   return c;
}

unsigned hash_unsigned_long( unsigned long k)
{
	return hash(&k, sizeof(k), 0);
}
