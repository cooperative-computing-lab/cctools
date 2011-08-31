/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MERGESORT_H
#define MERGESORT_H

/** @file mergesort.h Mergesort implementation.
This provides an in-place iterative implementation of MergeSort that works on
the linked lists defined in @ref list.h.
*/

#include "list.h"

typedef int (*cmp_op_t) (const void *, const void *);

/** Sort linked list using MergeSort.
This allows for a custom comparator that takes two data pointers and returns a
number less than 0 if the first object is less than the second, 0 if the the
objects are equal, and a number greater than 0 if the first object is greater
than the second.  An example of this is <tt>strcmp</tt>.
@param lst Linked list.
@param cmp Comparator function.
*/

void mergesort_list(struct list *lst, cmp_op_t cmp);

#endif
