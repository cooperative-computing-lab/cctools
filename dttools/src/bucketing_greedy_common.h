#ifndef BUCKETING_GREEDY_COMMON_H
#define BUCKETING_GREEDY_COMMON_H

#include "bucketing.h"
#include "list.h"

/* List cursor with its position in a list */
typedef struct {
    struct list_cursor *lc;
    int pos;
} bucketing_cursor_w_pos_t;

/* Range defined by a low cursor and a high cursor pointing to a list */
typedef struct {
    bucketing_cursor_w_pos_t *lo;
    bucketing_cursor_w_pos_t *hi;
} bucketing_bucket_range_t;

/* Cursor but with position in list
 * @param lc pointer to list cursor
 * @param pos position of list cursor in a list
 * @return pointer to bucketing_cursor_w_pos_t structure if success
 * @return 0 if failure */
bucketing_cursor_w_pos_t *bucketing_cursor_w_pos_create(struct list_cursor *lc, int pos);

/* Delete a bucketing_cursor_w_pos_t structure
 * @param cursor_pos the structure to be deleted */
void bucketing_cursor_w_pos_delete(bucketing_cursor_w_pos_t *cursor_pos);

/* Create a bucketing_bucket_range_t structure
 * @param lo low index
 * @param hi high index
 * @param l list that indices point to
 * @return pointer to a bucketing range if success
 * @return 0 if failure */
bucketing_bucket_range_t *bucketing_bucket_range_create(int lo, int hi, struct list *l);

/* Delete a bucketing_bucket_range_t
 * @param range the structure to be deleted */
void bucketing_bucket_range_delete(bucketing_bucket_range_t *range);

/* Free the list with the function used to free a bucketing_cursor_pos
 * This does not destroy the list, only the elements inside
 * @param l pointer to list to destroy
 * @param f function to free bucketing_cursor_pos */
void bucketing_cursor_pos_list_clear(struct list *l, void (*f)(bucketing_cursor_w_pos_t *));

/* Free the list with the function used to free a bucketing_bucket_range_t
 * This does not destroy the list, only the elements inside
 * @param l pointer to list to destroy
 * @param f function to free bucketing_bucket_range_t */
void bucketing_bucket_range_list_clear(struct list *l, void (*f)(bucketing_bucket_range_t *));

/* Sort a list of bucketing_cursor_pos
 * @param l the list to be sorted
 * @param f the compare function
 * @return pointer to a sorted list of bucketing_cursor_pos
 * @return 0 if failure */
struct list *bucketing_cursor_pos_list_sort(struct list *l, int (*f)(const void *, const void *));

/* Compare position of two break points
 * @param p1 first break point
 * @param p2 second break point
 * @return negative if p1 < p2, 0 if p1 == p2, positive if p1 > p2 */
int bucketing_compare_break_points(const void *p1, const void *p2);

#endif
