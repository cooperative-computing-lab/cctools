/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "jx_export.h"
#include "jx_print.h"

#include "stringtools.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static char * unquoted_string( struct jx *j )
{
	char *str;
	if(j->type==JX_STRING) {
		str = strdup(j->u.string_value);
	} else {
		str = jx_print_string(j);
	}
	return str;
}

/*
Export a JX object as environment variables in bash format.
*/

void jx_export_shell( struct jx *j, FILE *stream )
{
	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		char *str = unquoted_string(p->value);
		fprintf(stream,"export %s=%s\n",p->key->u.string_value,str);
		free(str);
	}
}


/*
The old nvpair format simply has unquoted data following the key.
*/

void jx_export_nvpair( struct jx *j, struct link *l, time_t stoptime )
{
	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		char *str = unquoted_string(p->value);
		link_printf(l,stoptime,"%s %s\n",p->key->u.string_value,str);
		free(str);
	}
	link_printf(l,stoptime,"\n");
}

/*
The old classad format has quoted strings, symbols, booleans, integers, but not objects or arrays.  So, we quote the latter two types.  Individual ads are separated by newlines.
*/

void jx_export_old_classads( struct jx *j, struct link *l, time_t stoptime )
{
	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		char *str = jx_print_string(p->value);
		if(p->value->type==JX_OBJECT || p->value->type==JX_ARRAY) {
			link_printf(l,stoptime,"%s = \"%s\"\n",p->key->u.string_value,str);
		} else {
			link_printf(l,stoptime,"%s = %s\n",p->key->u.string_value,str);
		}
		free(str);
	}
	link_printf(l,stoptime,"\n");
}

/*
For XML encoding, we use plain text for atomic types and tags to structure objects and arrays.
*/

void jx_export_xml( struct jx *j, struct link *l, time_t stoptime )
{
	struct jx_pair *p;
	struct jx_item *i;

	switch(j->type) {
	case JX_NULL:
		link_printf(l,stoptime,"null");
		break;
	case JX_BOOLEAN:
		link_printf(l,stoptime,j->u.boolean_value?"true":"false");
		break;
	case JX_INTEGER:
		link_printf(l,stoptime,"%lld",(long long)j->u.integer_value);
		break;
	case JX_DOUBLE:
		link_printf(l,stoptime,"%lf",j->u.double_value);
		break;
	case JX_STRING:
		link_printf(l,stoptime,"%s",j->u.string_value);
		break;
	case JX_SYMBOL:
		link_printf(l,stoptime,"%s",j->u.symbol_name);
		break;
	case JX_OBJECT:
		link_printf(l,stoptime,"<object>\n");
		for(p=j->u.pairs;p;p=p->next) {
			link_printf(l,stoptime,"<pair><key>%s</key>",p->key->u.string_value);
			link_printf(l,stoptime,"<value>");
			jx_export_xml(p->value,l,stoptime);
			link_printf(l,stoptime,"</value></pair>");
		}
		link_printf(l,stoptime,"</object>\n");
		break;
	case JX_ARRAY:
		link_printf(l,stoptime,"<array>\n");
		for (i = j->u.items; i; i = i->next) {
			link_printf(l,stoptime, "<item>");
			jx_export_xml(i->value,l,stoptime);
			link_printf(l,stoptime,"</item>");
		}
		link_printf(l,stoptime,"</array>\n");
		break;
	case JX_OPERATOR:
		link_printf(l,stoptime,"<expr>\n");
		jx_print_link(j,l,stoptime);
		link_printf(l,stoptime,"</expr>\n");
		break;
	case JX_ERROR:
		link_printf(l,stoptime,"<error>\n");
		jx_print_link(j,l,stoptime);
		link_printf(l,stoptime,"</error>\n");
		break;
	}
}

/*
New classads are quite similar to json, except that the use of [] and {} is reversed.
*/

void jx_export_new_classads( struct jx *j, struct link *l, time_t stoptime )
{
	struct jx_pair *p;
	struct jx_item *i;

	switch(j->type) {
		case JX_OBJECT:
			link_printf(l,stoptime,"[\n");
			for(p=j->u.pairs;p;p=p->next) {
				link_printf(l,stoptime,"%s=",p->key->u.string_value);
				jx_print_link(p->value,l,stoptime);
				link_printf(l,stoptime,";\n");
			}
			link_printf(l,stoptime,"]\n");
			break;
		case JX_ARRAY:
			link_printf(l,stoptime,"{\n");
			for(i=j->u.items;i;i=i->next) {
				jx_print_link(i->value,l,stoptime);
				if(i->next) link_printf(l,stoptime,",");
			}
			link_printf(l,stoptime,"}\n");
			break;
		default:
			jx_print_link(j,l,stoptime);
			break;
	}
}

#define COLOR_ONE "#aaaaff"
#define COLOR_TWO "#bbbbbb"

static int color_counter = 0;

static const char *align_string(struct jx_table *h)
{
	if(h->align == JX_TABLE_ALIGN_RIGHT) {
		return "right";
	} else {
		return "left";
	}
}

void jx_export_html_solo(struct jx *j, struct link *l, time_t stoptime )
{
	link_printf(l,stoptime, "<table bgcolor=%s>\n", COLOR_TWO);
	link_printf(l,stoptime, "<tr bgcolor=%s>\n", COLOR_ONE);

	color_counter = 0;

	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		link_printf(l,stoptime, "<tr bgcolor=%s>\n", color_counter % 2 ? COLOR_ONE : COLOR_TWO);
		color_counter++;
		link_printf(l,stoptime, "<td align=left><b>%s</b>\n", p->key->u.string_value);
		char *str = unquoted_string(p->value);
		if(!strcmp(p->key->u.string_value, "url")) {
			link_printf(l,stoptime, "<td align=left><a href=%s>%s</a>\n",str,str);
		} else {
			link_printf(l,stoptime, "<td align=left>%s\n",str);
		}
		free(str);
	}
	link_printf(l,stoptime, "</table>\n");
}

void jx_export_html_header( struct link *l, struct jx_table *h, time_t stoptime )
{
	link_printf(l,stoptime,"<table bgcolor=%s>\n", COLOR_TWO);
	link_printf(l,stoptime,"<tr bgcolor=%s>\n", COLOR_ONE);
	while(h->name) {
		link_printf(l,stoptime,"<td align=%s><b>%s</b>\n", align_string(h), h->title);
		h++;
	}
	color_counter = 0;
}

void jx_export_html( struct jx *n, struct link *l, struct jx_table *h, time_t stoptime )
{
	jx_export_html_with_link(n, l, h, 0, 0, stoptime);
}

void jx_export_html_with_link( struct jx *n, struct link *l, struct jx_table *h, const char *linkname, const char *linktext, time_t stoptime )
{
	link_printf(l,stoptime,"<tr bgcolor=%s>\n", color_counter % 2 ? COLOR_ONE : COLOR_TWO);
	color_counter++;
	while(h->name) {
		struct jx *value = jx_lookup(n,h->name);
		char *text;
		if(value) {
			text = unquoted_string(value);
		} else {
			text = strdup("???");
		}
		link_printf(l,stoptime,"<td align=%s>", align_string(h));
		if(h->mode == JX_TABLE_MODE_URL) {
			link_printf(l,stoptime,"<a href=%s>%s</a>\n", text, text);
		} else if(h->mode == JX_TABLE_MODE_METRIC) {
			char line[1024];
			string_metric(atof(text), -1, line);
			link_printf(l,stoptime,"%sB\n", line);
		} else {
			if(linkname && !strcmp(linkname, h->name)) {
				link_printf(l,stoptime,"<a href=%s>%s</a>\n", linktext, text);
			} else {
				link_printf(l,stoptime,"%s\n", text);
			}
		}
		free(text);
		h++;
	}
}

void jx_export_html_footer( struct link *l, struct jx_table *h, time_t stoptime )
{
	link_printf(l,stoptime,"</table>\n");
}
