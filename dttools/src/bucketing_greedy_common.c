#include "bucketing_greedy_common.h"
#include "bucketing.h"
#include "list.h"
#include "debug.h"
#include "xxmalloc.h"

#include <stdlib.h>

bucketing_cursor_w_pos_t *bucketing_cursor_w_pos_create(struct list_cursor *lc, int pos)
{
    bucketing_cursor_w_pos_t *cursor_pos = xxmalloc(sizeof(*cursor_pos));

    cursor_pos->lc = lc;
    cursor_pos->pos = pos;

    return cursor_pos;
}

void bucketing_cursor_w_pos_delete(bucketing_cursor_w_pos_t *cursor_pos)
{
    if (cursor_pos) {
        list_cursor_destroy(cursor_pos->lc);
        free(cursor_pos);
    }
}

bucketing_bucket_range_t *bucketing_bucket_range_create(int lo, int hi, struct list *l)
{
	bucketing_bucket_range_t *range = xxmalloc(sizeof(*range));

	struct list_cursor *cursor_lo = list_cursor_create(l);
	if (!list_seek(cursor_lo, lo))
		return 0;

	bucketing_cursor_w_pos_t *cursor_pos_lo = bucketing_cursor_w_pos_create(cursor_lo, lo);
	if (!cursor_pos_lo)
		return 0;

	range->lo = cursor_pos_lo;

	struct list_cursor *cursor_hi = list_cursor_create(l);
	if (!list_seek(cursor_hi, hi))
		return 0;

	bucketing_cursor_w_pos_t *cursor_pos_hi = bucketing_cursor_w_pos_create(cursor_hi, hi);
	if (!cursor_pos_hi)
		return 0;

	range->hi = cursor_pos_hi;

	return range;
}

void bucketing_bucket_range_delete(bucketing_bucket_range_t *range)
{
    if (range) {
        bucketing_cursor_w_pos_delete(range->lo);
        bucketing_cursor_w_pos_delete(range->hi);
        free(range);
    } else {
        warn(D_BUCKETING, "ignoring command to delete a null pointer to bucket range\n");
    }
}

void bucketing_cursor_pos_list_clear(struct list *l, void (*f)(bucketing_cursor_w_pos_t *))
{
    if (!l)
        return;

    bucketing_cursor_w_pos_t *tmp;

    while ((tmp = list_pop_head(l)))
        f(tmp);
}

void bucketing_bucket_range_list_clear(struct list *l, void (*f)(bucketing_bucket_range_t *))
{
    if (!l)
        return;

    bucketing_bucket_range_t *tmp;

    while ((tmp = list_pop_head(l)))
        f(tmp);
}

struct list *bucketing_cursor_pos_list_sort(struct list *l, int (*f)(const void *, const void *))
{
	if (!l)
		return 0;

	unsigned int size = list_length(l);
	unsigned int i = 0;
	bucketing_cursor_w_pos_t **arr = xxmalloc(size * sizeof(*arr));
	if (!arr) {
		fatal("Cannot create temp array\n");
		return 0;
	}

	struct list_cursor *lc = list_cursor_create(l);

	if (!list_seek(lc, 0)) {
		fatal("Cannot seek list\n");
		return 0;
	}

	/* Save all elements to array */
	while (list_get(lc, (void **)&arr[i])) {
		++i;
		list_next(lc);
	}

	/* Destroy the list but not its elements */
	list_cursor_destroy(lc);
	list_delete(l);

	/* Qsort the array */
	qsort(arr, size, sizeof(*arr), f);

	struct list *ret = list_create();
	lc = list_cursor_create(ret);

	/* Put back elements to a new list */
	for (i = 0; i < size; ++i)
		list_insert(lc, arr[i]);

	list_cursor_destroy(lc);
	free(arr);

	return ret;
}

int bucketing_compare_break_points(const void *p1, const void *p2)
{
    if (!p1 || !p2) {
        fatal("Cannot compare empty break points\n");
        return 0;
    }
    return (*((bucketing_cursor_w_pos_t **)p1))->pos - (*((bucketing_cursor_w_pos_t **)p2))->pos;
}
