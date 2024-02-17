/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#include "catalog_export.h"
#include "jx_print.h"

#include "stringtools.h"

#include <stdlib.h>
#include <string.h>

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

void catalog_export_nvpair( struct jx *j, struct link *l, time_t stoptime )
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
New classads are quite similar to json, except that the use of [] and {} is reversed.
*/

void catalog_export_new_classads( struct jx *j, struct link *l, time_t stoptime )
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

void catalog_export_html_solo(struct jx *j, struct link *l, time_t stoptime )
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

void catalog_export_html_header( struct link *l, struct jx_table *h, time_t stoptime )
{
	link_printf(l,stoptime,"<table bgcolor=%s>\n", COLOR_TWO);
	link_printf(l,stoptime,"<tr bgcolor=%s>\n", COLOR_ONE);
	while(h->name) {
		link_printf(l,stoptime,"<td align=%s><b>%s</b>\n", align_string(h), h->title);
		h++;
	}
	color_counter = 0;
}

void catalog_export_html( struct jx *n, struct link *l, struct jx_table *h, time_t stoptime )
{
	catalog_export_html_with_link(n, l, h, 0, 0, stoptime);
}

void catalog_export_html_with_link( struct jx *n, struct link *l, struct jx_table *h, const char *linkname, const char *linktext, time_t stoptime )
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

void catalog_export_html_footer( struct link *l, struct jx_table *h, time_t stoptime )
{
	link_printf(l,stoptime,"</table>\n");
}

void catalog_export_html_datetime_picker( struct link *l, time_t stoptime, time_t current) {
	struct tm *t = localtime(&current);
	struct tm *tm_yesterday, *tm_tomorrow;
	time_t yesterday, tomorrow;

	int year = t->tm_year + 1900;
	int month = t->tm_mon + 1;
	int day = t->tm_mday;
	int hour = t->tm_hour;
	int minute = t->tm_min;

	t->tm_hour = t->tm_min = t->tm_sec = 0;

	tm_yesterday = t;
	tm_yesterday->tm_mday = day - 1;
	yesterday = mktime(tm_yesterday);

	tm_tomorrow  = t;
	tm_tomorrow->tm_mday = day + 1;
	tomorrow = mktime(tm_tomorrow);

	link_printf(l,stoptime,
	"<script>"
		"function redirect() {"
			"var day = document.getElementById('day').value;"
			"var time = document.getElementById('time').value;"
			"var timestamp = new Date(`${day} ${time}`).getTime() / 1000;"
			"window.location = `/history/${timestamp}/`;"
		"}"
	"</script>"
	"<p>"
		"<a href='/history/%ld' style='padding: 0 10px' title='Move backward one day'></a>"
		"<input type='date' id='day' name='day' value='%d-%02d-%02d'>"
		"<input type='time' id='time' name='time' value='%02d:%02d'>"
		"<button type='button' onclick='redirect()'>Go To</button>"
		"<a href='/history/%ld' style='padding: 0 10px' title='Move forward one day'></a>"
	"</p>",
	yesterday, year, month, day, hour, minute, tomorrow);
}
