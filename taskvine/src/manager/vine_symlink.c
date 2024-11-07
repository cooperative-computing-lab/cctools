
#include "vine_symlink.h"

#include "xxmalloc.h"
#include <stdlib.h>

struct vine_symlink *vine_symlink_create(const char *name, const char *target)
{
	struct vine_symlink *s = malloc(sizeof(*s));
	s->name = xxstrdup(name);
	s->target = xxstrdup(target);
	return s;
}

struct vine_symlink *vine_symlink_copy(struct vine_symlink *s)
{
	return vine_symlink_create(s->name, s->target);
}

void vine_symlink_delete(struct vine_symlink *s)
{
	if (!s)
		return;
	free(s->name);
	free(s->target);
	free(s);
}
