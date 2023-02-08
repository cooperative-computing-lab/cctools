/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "dag.h"
#include "dag_variable.h"
#include "dag_resources.h"
#include "hash_table.h"
#include "xxmalloc.h"
#include "debug.h"

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

struct dag_variable_value *dag_variable_value_create(const char *value)
{
	struct dag_variable_value *v = malloc(sizeof(struct dag_variable_value));

	v->nodeid = 0;
	v->len    = strlen(value);
	v->size   = v->len + 1;

	v->value = malloc(v->size * sizeof(char));

	strcpy(v->value, value);

	return v;
}

void dag_variable_value_free(struct dag_variable_value *v)
{
	free(v->value);
	free(v);
}

struct dag_variable_value *dag_variable_value_append_or_create(struct dag_variable_value *v, const char *value)
{
	if(!v)
		return dag_variable_value_create(value);

	int nlen = strlen(value);
	int req  = v->len + nlen + 2;   // + 2 for ' ' and '\0'

	if( req > v->size )
	{
		//make size for string to be appended, plus some more, so we do not
		//need to reallocate for a while.
		int nsize = req > 2*(v->size) ? 2*req : 2*(v->size);
		char *new_val = realloc(v->value, nsize*sizeof(char));
		if(!new_val)
			fatal("Could not allocate memory for makeflow variable value: %s\n", value);

		v->size  = nsize;
		v->value = new_val;
	}

	//add separating space
	*(v->value + v->len) = ' ';

	//append new string
	strcpy(v->value + v->len + 1, value);
	v->len = v->len + nlen + 1;

	return v;
}

struct dag_variable *dag_variable_create(const char *name, const char *initial_value)
{
	struct dag_variable *var = malloc(sizeof(struct dag_variable));

	if(!initial_value && name)
	{
		initial_value = getenv(name);
	}

	if(initial_value)
	{
		var->count  = 1;
		var->values = malloc(sizeof(struct dag_variable_value *));
		var->values[0] = dag_variable_value_create(initial_value);
	}
	else
	{
		var->count  = 0;
		var->values = NULL;
	}

	return var;
}

void dag_variable_add_value(const char *name, struct hash_table *current_table, int nodeid, const char *value)
{
	struct dag_variable *var = hash_table_lookup(current_table, name);
	if(!var)
	{
		char *value_env = getenv(name);
		var = dag_variable_create(name, value_env);
		hash_table_insert(current_table, name, var);
	}

	struct dag_variable_value *v = dag_variable_value_create(value);
	v->nodeid = nodeid;

	if(var->count < 1 || var->values[var->count - 1]->nodeid != v->nodeid)
	{
		var->count++;
		var->values = realloc(var->values, var->count * sizeof(struct dag_variable_value *));
	} else {
		dag_variable_value_free(var->values[var->count-1]);
	}

	//possible memory leak...
	var->values[var->count - 1] = v;
}

static int variable_binary_search(struct dag_variable_value **values, int nodeid, int min, int max)
{
	if(nodeid < 0)
		return max;

	struct dag_variable_value *v;
	int mid;

	while(max >= min)
	{
		mid = (max + min)/2;
		v = values[mid];

		if(v->nodeid < nodeid)
		{
			min = mid + 1;
		}
		else if(v->nodeid > nodeid)
		{
			max = mid - 1;
		}
		else
		{
			return mid;
		}
	}

	//here max =< min, thus v[max] < nodeid < v[min]
	return max;
}

struct dag_variable_value *dag_variable_get_value(const char *name, struct hash_table *t, int node_id)
{
	struct dag_variable *var;

	var = (struct dag_variable *) hash_table_lookup(t, name);

	if(!var)
		return NULL;

	if(node_id < 0)
		return var->values[var->count - 1];

	int index = variable_binary_search(var->values, node_id, 0, var->count - 1);
	if(index < 0)
		return NULL;

	return var->values[index];
}

int dag_variable_count(const char *name, struct dag_variable_lookup_set *s )
{
	if(!s) {
		return 0;
	}

	struct hash_table *t = NULL;
	/* Try node variables table */
	if(s->node) {
		t = s->node->variables;
	} else if(!s->dag) {
		t = NULL;
	} else if(!s->category) {
		t = s->dag->default_category->mf_variables;
	} else if(s->category) {
		t = s->category->mf_variables;
	}

	if(t) {
		struct dag_variable *var = hash_table_lookup(t, name);
		if(var) {
			return var->count;
		}
	}

	return 0;
}


struct dag_variable_value *dag_variable_lookup(const char *name, struct dag_variable_lookup_set *s )
{
	struct dag_variable_value *v = NULL;

	if(!s)
		return NULL;

	/* Try node variables table */
	if(s->node) {
		v = dag_variable_get_value(name, s->node->variables, s->node->nodeid);
		if(v) {
			s->table = s->node->variables; //why this line?
			return v;
		}
	}

	if(!s->dag)
		return NULL;

	/* do not look further than the current location of the rule in the makeflow file, if given. */
	int nodeid = s->dag->nodeid_counter;
	if(s->node) {
		nodeid = s->node->nodeid;
	}

	/* Try the category variables table */
	if(s->category) {
		v = dag_variable_get_value(name, s->category->mf_variables, nodeid);
		if(v) {
			s->table = s->dag->default_category->mf_variables;
			return v;
		}
	}

	/* Try dag variables table */
	v = dag_variable_get_value(name, s->dag->default_category->mf_variables, nodeid);
	if(v) {
		s->table = s->dag->default_category->mf_variables;
		return v;
	}

	/* Try the environment last. If found, add it to the default dag variables table.*/
	const char *value = getenv(name);
	if(value) {
		s->table = s->dag->default_category->mf_variables;
		dag_variable_add_value(name, s->table, 0, value);
		return dag_variable_lookup(name, s);
	}

	return NULL;
}

char *dag_variable_lookup_string(const char *name, struct dag_variable_lookup_set *s )
{
	struct dag_variable_value *v = dag_variable_lookup(name, s);

	if(v)
		return xxstrdup(v->value);
	else
		return NULL;
}

char *dag_variable_lookup_global_string(const char *name, struct dag *d )
{
	struct dag_variable_lookup_set s = { d, NULL, NULL, NULL };
	return dag_variable_lookup_string(name, &s);
}

/* vim: set noexpandtab tabstop=4: */
