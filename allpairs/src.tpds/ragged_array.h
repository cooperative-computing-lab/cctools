#ifndef RAGGED_ARRAY_H
#define RAGGED_ARRAY_H

struct ragged_array {
    char** arr;
    int row_count;
    int array_size;
};

struct ragged_array ragged_array_initialize(const int size);

struct ragged_array ragged_array_populate(const char *path, const char *prefix, const int line_max);

// Expand the capacity of the array, without adding new data.
int ragged_array_expand(struct ragged_array* set,const int new_size);

// Add a line, expanding if necessary.
int ragged_array_add_line(struct ragged_array* set, const char* line);

// Delete a line while retaining continuity of the array -- warning: potentially slow!
int ragged_array_delete_line(struct ragged_array* set, const int line_index);

void ragged_array_print( struct ragged_array *set );

#endif
