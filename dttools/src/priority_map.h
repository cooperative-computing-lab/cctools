#ifndef PRIORITY_MAP_H
#define PRIORITY_MAP_H

typedef char *(*pmap_key_generator_t)(const void *data);

struct priority_map *priority_map_create(int init_capacity, pmap_key_generator_t keygen);

int priority_map_push(struct priority_map *pmap, void *data, double priority);

int priority_map_push_or_update(struct priority_map *pmap, void *data, double priority);

int priority_map_update_priority(struct priority_map *pmap, const void *data, double new_priority);

double priority_map_peek_priority(struct priority_map *pmap, const void *data);

int priority_map_remove(struct priority_map *pmap, const void *data);

void *priority_map_peek_top(struct priority_map *pmap);

void *priority_map_pop(struct priority_map *pmap);

int priority_map_size(struct priority_map *pmap);

int priority_map_contains(struct priority_map *pmap, const void *data);

struct priority_map *priority_map_duplicate(struct priority_map *pmap);

void priority_map_delete(struct priority_map *pmap);

int priority_map_validate(struct priority_map *pmap);

/*
 * PRIORITY_MAP_ITERATE(pmap, idx, data_ptr, iter_count, iter_depth)
 *
 * Read-only, fast iteration over the priority map.
 * You may NOT modify the map during iteration.
 */
extern void *priority_map_internal_peek_data(struct priority_map *pmap, int idx);

#define PRIORITY_MAP_ITERATE(pmap, idx, data_ptr, iter_count, iter_depth) \
    for ((iter_count) = 0, (idx) = 0; \
         (iter_count) < (iter_depth) && (idx) < priority_map_size(pmap) && \
         ((data_ptr) = priority_map_internal_peek_data(pmap, idx), 1); \
         (iter_count)++, (idx)++)

#endif // PRIORITY_MAP_H
