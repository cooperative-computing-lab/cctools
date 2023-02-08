/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_VARIABLE_H
#define DAG_VARIABLE_H

/*
 * dag_variable_value records a single instance of a variable's value and location
 */

struct dag_variable_value {
	int   nodeid;  /* The nodeid of the rule to which this value binding takes effect. */
	int   size;    /* memory size allocated for value */
	int   len;     /* records strlen(value) */
	char *value;   /* The value of the variable. */
};

struct dag_variable_value *dag_variable_value_create(const char *value);
struct dag_variable_value *dag_variable_value_append_or_create(struct dag_variable_value *v, const char *value);
void dag_variable_value_free(struct dag_variable_value *v);

/*
 * dag_variable tracks all of the locations of a variable's value
 */

struct dag_variable {
	int    count;
	struct dag_variable_value **values;
};

struct dag_variable *dag_variable_create(const char *name, const char *initial_value);
void dag_variable_add_value(const char *name, struct hash_table *current_table, int nodeid, const char *value);
struct dag_variable_value *dag_variable_get_value(const char *name, struct hash_table *t, int node_id);

/*
 * dag_lookup_set indicates all of the places where a variable might be bound.
 * To use, set all members to the items of interest (or null) and then call
 * dag_lookup to query those locations.
 */

struct dag_variable_lookup_set {
	struct dag *dag;
	struct category *category;
	struct dag_node *node;
	struct hash_table *table;
};

/* Count the number of times the variable was defined */
int dag_variable_count(const char *name, struct dag_variable_lookup_set *s);

/* Look up a variable in multiple scopes and return the (constant) structure. */
struct dag_variable_value *dag_variable_lookup(const char *name, struct dag_variable_lookup_set *s );

/* Look up a variable in multiple scopes and return the value, strdup'd. */
char *dag_variable_lookup_string(const char *name, struct dag_variable_lookup_set *s );

/* Look up a variable only at dag scope and return the value, strdup'd. */
char *dag_variable_lookup_global_string(const char *name, struct dag *d );

#endif
