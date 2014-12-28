/*
Copyright (C) 2014- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef DAG_TASK_CATEGORY_H
#define DAG_TASK_CATEGORY_H

#include "list.h"

/*
* Data about each category of tasks, including each
* task that is a member of the category.
* In the future this will contain aggregate performance stats.
*/

struct dag_task_category
{
	char *label;
	struct list *nodes;
};

struct dag_task_category * dag_task_category_create( const char *label );

#endif
