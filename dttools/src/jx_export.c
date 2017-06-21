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
The old nvpair format simply has unquoted data following the key.
*/

void jx_export_nvpair( struct jx *j, FILE *stream )
{
	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		char *str = unquoted_string(p->value);
		fprintf(stream,"%s %s\n",p->key->u.string_value,str);
		free(str);
	}
	fprintf(stream,"\n");
}

/*
The old classad format has quoted strings, symbols, booleans, integers, but not objects or arrays.  So, we quote the latter two types.  Individual ads are separated by newlines.
*/

void jx_export_old_classads( struct jx *j, FILE *stream )
{
	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		char *str = jx_print_string(p->value);
		if(p->value->type==JX_OBJECT || p->value->type==JX_ARRAY) {
			fprintf(stream,"%s = \"%s\"\n",p->key->u.string_value,str);
		} else {
			fprintf(stream,"%s = %s\n",p->key->u.string_value,str);
		}
		free(str);
	}
	fprintf(stream,"\n");
}

/*
For XML encoding, we use plain text for atomic types and tags to structure objects and arrays.
*/

void jx_export_xml( struct jx *j, FILE *stream )
{
	struct jx_pair *p;
	struct jx_item *i;

	switch(j->type) {
	case JX_NULL:
		fprintf(stream,"null");
		break;
	case JX_BOOLEAN:
		fprintf(stream,j->u.boolean_value?"true":"false");
		break;
	case JX_INTEGER:
		fprintf(stream,"%lld",(long long)j->u.integer_value);
		break;
	case JX_DOUBLE:
		fprintf(stream,"%lf",j->u.double_value);
		break;
	case JX_STRING:
		fprintf(stream,"%s",j->u.string_value);
		break;
	case JX_SYMBOL:
		fprintf(stream,"%s",j->u.symbol_name);
		break;
	case JX_OBJECT:
		fprintf(stream,"<object>\n");
		for(p=j->u.pairs;p;p=p->next) {
			fprintf(stream,"<pair><key>%s</key>",p->key->u.string_value);
			fprintf(stream,"<value>");
			jx_export_xml(p->value,stream);
			fprintf(stream,"</value></pair>");
		}
		fprintf(stream,"</object>\n");
		break;
	case JX_ARRAY:
		fprintf(stream,"<array>\n");
		if (j->u.items && j->u.items->variable) {
			fprintf(stream, "<var>");
			fprintf(stream, j->u.items->variable);
			fprintf(stream, "</var>");
		}
		if (j->u.items && j->u.items->list) {
			fprintf(stream, "<list>");
			jx_export_xml(j->u.items->list, stream);
			fprintf(stream, "</list>");
		}
		for(i=j->u.items;i;i=i->next) {
			fprintf(stream,"<item>");
			jx_export_xml(i->value,stream);
			fprintf(stream,"</item>");
		}
		fprintf(stream,"</array>\n");
		break;
	case JX_OPERATOR:
		fprintf(stream,"<expr>\n");
		jx_print_stream(j,stream);
		fprintf(stream,"</expr>\n");
		break;
	case JX_FUNCTION:
		fprintf(stream, "<func>");
		fprintf(stream, "<name>");
		fprintf(stream, j->u.func.name);
		fprintf(stream, "</name>");
		for (struct jx_item *i = j->u.func.params; i; i = i->next) {
			fprintf(stream, "<param>");
			jx_export_xml(i->value, stream);
			fprintf(stream, "</param>");
		}
		fprintf(stream, "<body>");
		jx_export_xml(j->u.func.body, stream);
		fprintf(stream, "</body>");
		fprintf(stream, "</func>");
		break;
	case JX_ERROR:
		fprintf(stream,"<error>\n");
		jx_print_stream(j,stream);
		fprintf(stream,"</error>\n");
		break;
	}
}

/*
New classads are quite similar to json, except that the use of [] and {} is reversed.
*/

void jx_export_new_classads( struct jx *j, FILE *stream )
{
	struct jx_pair *p;
	struct jx_item *i;

	switch(j->type) {
		case JX_OBJECT:
			fprintf(stream,"[\n");
			for(p=j->u.pairs;p;p=p->next) {
				fprintf(stream,"%s=",p->key->u.string_value);
				jx_print_stream(p->value,stream);
				fprintf(stream,";\n");
			}
			fprintf(stream,"]\n");
			break;
		case JX_ARRAY:
			fprintf(stream,"{\n");
			for(i=j->u.items;i;i=i->next) {
				jx_print_stream(i->value,stream);
				if(i->next) fprintf(stream,",");
			}
			fprintf(stream,"}\n");
			break;
		default:
			jx_print_stream(j,stream);
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

void jx_export_html_solo(struct jx *j, FILE * stream)
{
	fprintf(stream, "<table bgcolor=%s>\n", COLOR_TWO);
	fprintf(stream, "<tr bgcolor=%s>\n", COLOR_ONE);

	color_counter = 0;

	struct jx_pair *p;
	for(p=j->u.pairs;p;p=p->next) {
		fprintf(stream, "<tr bgcolor=%s>\n", color_counter % 2 ? COLOR_ONE : COLOR_TWO);
		color_counter++;
		fprintf(stream, "<td align=left><b>%s</b>\n", p->key->u.string_value);
		char *str = unquoted_string(p->value);
		if(!strcmp(p->key->u.string_value, "url")) {
			fprintf(stream, "<td align=left><a href=%s>%s</a>\n",str,str);
		} else {
			fprintf(stream, "<td align=left>%s\n",str);
		}
		free(str);
	}
	fprintf(stream, "</table>\n");
}

void jx_export_html_header(FILE * s, struct jx_table *h)
{
	fprintf(s, "<table bgcolor=%s>\n", COLOR_TWO);
	fprintf(s, "<tr bgcolor=%s>\n", COLOR_ONE);
	while(h->name) {
		fprintf(s, "<td align=%s><b>%s</b>\n", align_string(h), h->title);
		h++;
	}
	color_counter = 0;
}

void jx_export_html(struct jx *n, FILE * s, struct jx_table *h)
{
	jx_export_html_with_link(n, s, h, 0, 0);
}

void jx_export_html_with_link(struct jx *n, FILE * s, struct jx_table *h, const char *linkname, const char *linktext)
{
	fprintf(s, "<tr bgcolor=%s>\n", color_counter % 2 ? COLOR_ONE : COLOR_TWO);
	color_counter++;
	while(h->name) {
		struct jx *value = jx_lookup(n,h->name);
		char *text;
		if(value) {
			text = unquoted_string(value);
		} else {
			text = strdup("???");
		}
		fprintf(s, "<td align=%s>", align_string(h));
		if(h->mode == JX_TABLE_MODE_URL) {
			fprintf(s, "<a href=%s>%s</a>\n", text, text);
		} else if(h->mode == JX_TABLE_MODE_METRIC) {
			char line[1024];
			string_metric(atof(text), -1, line);
			fprintf(s, "%sB\n", line);
		} else {
			if(linkname && !strcmp(linkname, h->name)) {
				fprintf(s, "<a href=%s>%s</a>\n", linktext, text);
			} else {
				fprintf(s, "%s\n", text);
			}
		}
		free(text);
		h++;
	}
}

void jx_export_html_footer(FILE * s, struct jx_table *h)
{
	fprintf(s, "</table>\n");
}
