/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "ast.h"
#include "xmalloc.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define ALLOC(x) \
	x = xxmalloc( sizeof ( * x ) ); \
	memset(x,0,sizeof( * x ) );

struct ast_group * ast_group_create( struct ast_command *command, struct ast_group *next )
{
	struct ast_group *g;

	ALLOC(g);

	g->command = command;
	g->next = next;

	return g;
}

struct ast_function * ast_function_create( int function_line, int end_line, struct ast_word *name, struct ast_group *body )
{
	struct ast_function *f;

	ALLOC(f);

	f->function_line = function_line;
	f->end_line = end_line;
	f->name = name;
	f->body = body;

	return f;
}

struct ast_command * ast_command_create( int type, void *data )
{
	struct ast_command *s;

	ALLOC(s);

	s->type = type;
	s->u.data = data;

	return s;
}

struct ast_conditional * ast_conditional_create( int iline, int tline, int eline, int end_line, struct expr *expr, struct ast_group *positive, struct ast_group *negative )
{
	struct ast_conditional *c;

	ALLOC(c);

	c->if_line = iline;
	c->then_line = tline;
	c->else_line = eline;
	c->end_line = end_line;
	c->expr = expr;
	c->positive = positive;
	c->negative = negative;

	return c;
}
 
struct ast_try * ast_try_create( int try_line, int catch_line, int end_line, struct ast_try_limit *time_limit, struct ast_try_limit *loop_limit, struct ast_try_limit *every_limit, struct ast_group *body, struct ast_group *catch_block )
{
	struct ast_try *t;

	ALLOC(t);

	t->try_line = try_line;
	t->catch_line = catch_line;
	t->end_line = end_line;
	t->time_limit = time_limit;
	t->loop_limit = loop_limit;
	t->every_limit = every_limit;
	t->body = body;
	t->catch_block = catch_block;

	return t;
}

struct ast_try_limit * ast_try_limit_create( struct expr *expr, int units )
{
	struct ast_try_limit *l;

	ALLOC(l);

	l->expr = expr;
	l->units = units;

	return l;
}

struct ast_whileloop * ast_whileloop_create( int while_line, int do_line, int end_line, struct expr *expr, struct ast_group *body )
{
	struct ast_whileloop *f;

	ALLOC(f);

	f->while_line = while_line;
	f->end_line = end_line;
	f->expr = expr;
	f->body = body;

	return f;
}

struct ast_forloop * ast_forloop_create( int type, int for_line, int end_line, struct ast_word *name, struct expr *list, struct ast_group *body )
{
	struct ast_forloop *f;

	ALLOC(f);

	f->type = type;
	f->for_line = for_line;
	f->end_line = end_line;
	f->name = name;
	f->list = list;
	f->body = body;

	return f;
}

struct ast_shift * ast_shift_create( int line, struct expr *expr )
{
	struct ast_shift *r;

	ALLOC(r);

	r->line = line;
	r->expr = expr;

	return r;
}

struct ast_return * ast_return_create( int line, struct expr *expr )
{
	struct ast_return *r;

	ALLOC(r);

	r->line = line;
	r->expr = expr;

	return r;
}

struct ast_assign * ast_assign_create( int line, struct ast_word *name, struct expr *expr )
{
	struct ast_assign *r;

	ALLOC(r);

	r->line = line;
       	r->name = name;
	r->expr = expr;

	return r;
}

struct ast_simple * ast_simple_create( int line, struct ast_word *words, struct ast_redirect *redirects )
{
	struct ast_simple *s;

	ALLOC(s);

	s->line = line;
	s->words = words;
	s->redirects = redirects;

	return s;
}

struct ast_redirect * ast_redirect_create( int kind, int source, struct ast_word *target, int mode )
{
	struct ast_redirect *r;

	ALLOC(r);

	r->kind = kind;
	r->source = source;
	r->target = target;
	r->mode = mode;
	r->actual = -1;

	return r;
}

struct ast_word * ast_word_create( int line, const char *text )
{
	struct ast_word *w;

	ALLOC(w);

	w->line = line;
	w->text = xstrdup(text);
	w->next = 0;

	return w;
}

