/*
Copyright (C) 2013- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "set.h"

#define SET_INS(s, f, n) set_insert(s, (void *) n ); f[n] = 1;

int main(int argc, char **argv)
{
  uintptr_t N = 11;
  int set_membership[N];

  struct set *s = set_create(0);
  bzero(set_membership, sizeof(int) * N);

  SET_INS(s, set_membership, 1);
  assert( set_size(s) == 1 );

  SET_INS(s, set_membership, 1);
  assert( set_size(s) == 1 );

  SET_INS(s, set_membership, 2);
  assert( set_size(s) == 2 );

  set_remove(s, (void *) 3);
  assert( set_size(s) == 2 );

  set_remove(s, (void *) 1);
  assert( set_size(s) == 1 );

  set_remove(s, (void *) 1);
  assert( set_size(s) == 1 );

  /* Check that the remaining element is 2 */
  assert( set_pop(s) == (void *) 2 );

  uintptr_t i;
  for(i = 1; i < N; i++)
    SET_INS(s, set_membership, i);

  uintptr_t sum   = 0;
  uintptr_t sum_z = set_size(s);

  set_first_element(s);
  while( (i = (uintptr_t) set_pop(s)) )
  {
    sum   += i;
    sum_z += set_size(s);
  }

  assert( sum   == ((N * (N - 1)) / 2) );
  assert( sum_z == ((N * (N - 1)) / 2) );

  return 0;
}

