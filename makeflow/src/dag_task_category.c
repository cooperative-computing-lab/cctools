#include "dag_task_category.h"

#include <stdlib.h>
#include <string.h>

struct dag_task_category * dag_task_category_create(  const char *label )
{
	struct dag_task_category *c = malloc(sizeof(*c));
	c->label = strdup(label);
	c->nodes = list_create();
	return c;
}
