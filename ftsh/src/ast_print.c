/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under a BSD-style license.
See the file COPYING for details.
*/

#include "ast.h"
#include "ast_print.h"

#include <stdio.h>
#include <stdlib.h>

static void indent( FILE *file, int level )
{
	int i;
	for(i=0;i<level;i++) {
		fprintf(file,"\t");
	}
}

void ast_group_print( FILE *file, struct ast_group *g, int level )
{
	while(g) {
		ast_command_print(file,g->command,level+1);
		g = g->next;
	}
}

void ast_command_print( FILE *file, struct ast_command *s, int level )
{
	if(s) switch(s->type) {
		case AST_COMMAND_FUNCTION:
			ast_function_print(file,s->u.function,level);
			break;
		case AST_COMMAND_CONDITIONAL:
			ast_conditional_print(file,s->u.conditional,level);
			break;
		case AST_COMMAND_TRY:
			ast_try_print(file,s->u.try,level);
			break;
		case AST_COMMAND_WHILELOOP:
			ast_whileloop_print(file,s->u.whileloop,level);
			break;
		case AST_COMMAND_FORLOOP:
			ast_forloop_print(file,s->u.forloop,level);
			break;
		case AST_COMMAND_SIMPLE:
			ast_simple_print(file,s->u.simple,level);
			break;
		case AST_COMMAND_SHIFT:
			ast_shift_print(file,s->u.shift,level);
			break;
		case AST_COMMAND_RETURN:
			ast_return_print(file,s->u.rtn,level);
			break;
		case AST_COMMAND_ASSIGN:
			ast_assign_print(file,s->u.assign,level);
			break;
		case AST_COMMAND_EMPTY:
			break;
	}
}

void ast_function_print( FILE *file, struct ast_function *f, int level )
{
	if(f) {
		indent(file,level);
		fprintf(file,"function ");
		ast_word_print(file,f->name);
		fprintf(file,"\n");
		ast_group_print(file,f->body,level);
		indent(file,level);
		fprintf(file,"end\n");
	}
}

void ast_conditional_print( FILE *file, struct ast_conditional *c, int level )
{
	if(c) {
		indent(file,level);
		fprintf(file,"if ");
		expr_print(file,c->expr);
		fprintf(file,"\n");
		if(c->positive) {
			ast_group_print(file,c->positive,level);
		}
		if(c->negative) {
			indent(file,level);
			fprintf(file,"else\n");
			ast_group_print(file,c->negative,level);
		}
		indent(file,level);
		fprintf(file,"end\n");
	}
}

void ast_try_print( FILE *file, struct ast_try *t, int level )
{
	if(t) {
		indent(file,level);
		fprintf(file,"try ");
		ast_try_limit_print(file,t->time_limit,0);
		ast_try_limit_print(file,t->loop_limit,0);
		ast_try_limit_print(file,t->every_limit,"every");
		fprintf(file,"\n");
		ast_group_print(file,t->body,level);
		if(t->catch_block) {
			indent(file,level);
			fprintf(file,"catch\n");
			ast_group_print(file,t->catch_block,level);
		}
		indent(file,level);
		fprintf(file,"end\n");
	}
}

char * units_name( int units )
{
	switch(units) {
		case 0: return "times";
		case 1: return "seconds";
		case 60: return "minutes";
		case 3600: return "hours";
		case 86400: return "days";
		default: return "???";
	}
}

void ast_try_limit_print( FILE *file, struct ast_try_limit *l, const char *prefix )
{
	if(l) {
		if(prefix) fprintf(file,"%s ",prefix);
		expr_print(file,l->expr);
		fprintf(file,"%s ",units_name(l->units));
	}
}

void ast_whileloop_print( FILE *file, struct ast_whileloop *l, int level )
{
	if(l) {
		indent(file,level);
		fprintf(file,"while ");
		expr_print(file,l->expr);
		fprintf(file,"\n");
		ast_group_print(file,l->body,level);
		indent(file,level);
		fprintf(file,"end\n");
	}
}

void ast_forloop_print( FILE *file, struct ast_forloop *f, int level )
{
	if(f) {
		indent(file,level);
		switch(f->type) {
			case AST_FOR:
				fprintf(file,"for ");
				break;
			case AST_FORANY:
				fprintf(file,"forany ");
				break;
			case AST_FORALL:
				fprintf(file,"forall ");
				break;
		}
		ast_word_print(file,f->name);
		fprintf(file,"in ");
		expr_print(file,f->list);
		fprintf(file,"\n");
		ast_group_print(file,f->body,level);
		indent(file,level);
		fprintf(file,"end\n");
	}
}

void ast_shift_print( FILE *file, struct ast_shift *s, int level )
{
	if(s) {
		indent(file,level);
		fprintf(file,"shift ");
		if(s->expr) expr_print(file,s->expr);
		fprintf(file,"\n");
	}
}

void ast_return_print( FILE *file, struct ast_return *s, int level )
{
	if(s) {
		indent(file,level);
		fprintf(file,"return ");
		if(s->expr) expr_print(file,s->expr);
		fprintf(file,"\n");
	}
}

void ast_assign_print( FILE *file, struct ast_assign *s, int level )
{
	if(s) {
		indent(file,level);
		fprintf(file,"%s=",s->name->text);
		if(s->expr) expr_print(file,s->expr);
		fprintf(file,"\n");
	}
}

void ast_simple_print( FILE *file, struct ast_simple *s, int level )
{
	if(s) {
		indent(file,level);
		ast_word_print(file,s->words);
		ast_redirect_print(file,s->redirects);
		fprintf(file,"\n");
	}
}

void ast_word_print( FILE *file, struct ast_word *w )
{
	while(w) {
		fprintf(file,"%s ",w->text);
		w = w->next;
	}
}

void ast_redirect_print( FILE *file, struct ast_redirect *r )
{
	if(r) {	
		char *s;
		switch(r->kind) {
			case AST_REDIRECT_FILE:
				switch(r->mode) {
					case AST_REDIRECT_INPUT:
						s = "<";
						break;
					case AST_REDIRECT_OUTPUT:
						s = ">";
						break;
					case AST_REDIRECT_APPEND:
						s = ">>";
						break;
				}
				break;
			case AST_REDIRECT_BUFFER:
				switch(r->mode) {
					case AST_REDIRECT_INPUT:
						s = "<-";
						break;
					case AST_REDIRECT_OUTPUT:
						s = "->";
						break;
					case AST_REDIRECT_APPEND:
						s = "->>";
						break;
				}
				break;
			case AST_REDIRECT_FD:
				switch(r->mode) {
					case AST_REDIRECT_INPUT:
						s = "<&";
						break;
					case AST_REDIRECT_OUTPUT:
						s = ">&";
						break;
					case AST_REDIRECT_APPEND:
						s = ">>&";
						break;
				}
				break;
			break;
		}
		fprintf(file,"%d%s %s ",r->source,s,r->target->text);
		ast_redirect_print(file,r->next);
	}
}		

