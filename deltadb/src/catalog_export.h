/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CATALOG_EXPORT_H
#define CATALOG_EXPORT_H

#include "jx.h"
#include "jx_table.h"
#include "link.h"
#include <stdio.h>
#include <time.h>

void catalog_export_nvpair( struct jx *j, struct link *l, time_t stoptime );
void catalog_export_new_classads( struct jx *j, struct link *l, time_t stoptime );

void catalog_export_html_solo( struct jx *j, struct link *l, time_t stoptime );
void catalog_export_html_header( struct link *l, struct jx_table *h, time_t stoptime );
void catalog_export_html( struct jx *j, struct link *l, struct jx_table *h, time_t stoptime );
void catalog_export_html_footer( struct link *l, struct jx_table *h, time_t stoptime );

void catalog_export_html_with_link(struct jx *j, struct link *l, struct jx_table *h, const char *linkname, const char *linktext, time_t stoptime );

void catalog_export_html_datetime_picker( struct link *l, time_t stoptime, time_t current);

#endif
