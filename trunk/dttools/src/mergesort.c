/*
Copyright (C) 2009- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "mergesort.h"
#include "list.h"

#include <stdlib.h>

static void merge(struct list_node *lh, struct list_node *rh, struct list_node *sen, cmp_op_t cmp)
{
	struct list_node *lp = lh;
	struct list_node *rp = rh;
	struct list_node *tp;

	while(lp != rp && rp != sen) {
		if(cmp(lp->data, rp->data) <= 0) {
			lp = lp->next;
		} else {
			if(rp->prev)
				rp->prev->next = rp->next;
			if(rp->next)
				rp->next->prev = rp->prev;

			tp = rp->next;
			rp->next = lp;
			rp->prev = lp->prev;

			if(lp->prev)
				lp->prev->next = rp;
			lp->prev = rp;
			rp = tp;
		}
	}
}

void mergesort_list(struct list *lst, cmp_op_t cmp)
{
	struct list_node *left_head;
	struct list_node *right_head;
	struct list_node *left_tail;
	struct list_node *right_tail;
	int n, first_set;

	if(!lst || lst->size <= 1)
		return;

	for(n = 1; n < lst->size; n *= 2) {
		left_head = lst->head;

		first_set = 1;
		while(left_head) {
			int i;

			right_head = left_head;
			for(i = 0; (i < n) && right_head->next; i++)
				right_head = right_head->next;

			left_tail = right_head->prev;
			right_tail = right_head;
			for(i = 0; (i < n) && right_tail; i++) {
				if(right_tail->next == NULL)
					lst->tail = cmp(left_tail->data, right_tail->data) > 0 ? left_tail : right_tail;
				right_tail = right_tail->next;
			}

			merge(left_head, right_head, right_tail, cmp);

			if(first_set) {
				lst->head = cmp(left_head->data, right_head->data) <= 0 ? left_head : right_head;
				first_set = 0;
			}
			left_head = right_tail;
		}
	}
}
