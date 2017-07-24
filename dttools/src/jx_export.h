/*
Copyright (C) 2015- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_EXPORT_H
#define JX_EXPORT_H

#include "jx.h"
#include "jx_table.h"
#include <stdio.h>

void jx_export_shell( struct jx *j, FILE *stream );
void jx_export_nvpair( struct jx *j, FILE *stream );
void jx_export_old_classads( struct jx *j, FILE *stream );
void jx_export_new_classads( struct jx *j, FILE *stream );
void jx_export_xml( struct jx *j, FILE *stream );

void jx_export_html_solo( struct jx *j, FILE *stream );
void jx_export_html_header(FILE * s, struct jx_table *h);
void jx_export_html( struct jx *j, FILE * s, struct jx_table *h);
void jx_export_html_footer(FILE * s, struct jx_table *h);

void jx_export_html_with_link(struct jx *j, FILE * s, struct jx_table *h, const char *linkname, const char *linktext);

#endif
