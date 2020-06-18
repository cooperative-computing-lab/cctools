/*
Copyright (C) 2020- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef JX_QUERYTOOLS_H
#define JX_QUERYTOOLS_H

#include "jx.h"

/** Perform JX query upon a JSON document.
@param q The string representation of the JX query expression to evaluate.
@param c The JSON context upon which to evaluate as string.
@return The output value of the evaluated expression as JX.
Returns NULL on error.
*/
struct jx * jx_evaluate_query ( struct jx *c );

/** Fetch a JSON document from a URL within a JX expression.
Fetched JSON is parsed stored in a JX object.
@param u The URL from which to fetch the document.
@return The JX object of the fetched and parsed JSON.
*/
struct jx * jx_fetch_from_url ( const char *u );

/** Parses JSON from an HTML document.
The HTML document is assumed to be formatted as a
server response from the TLQ troubleshooting tool.
@param d The path to the HTML document to parse.
@return  The JSON document contained within the HTML as string.
*/
const char * jx_parse_from_html ( const char *d );

/** PLACEHOLDER
@param c The context upon which to project via an expression.
@param e The expression from which to perform the projection.
@return The projected JX expression.
*/
struct jx * jx_query_project ( struct jx *c, struct jx *e );

/** PLACEHOLDER
@param c The context upon which to select via an expression.
@param e The expression from which to perform the selection.
@return The selected JX expression.
*/
struct jx * jx_query_select ( struct jx *c, struct jx *e );

#endif
