/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef NVPAIR_H
#define NVPAIR_H

#include <stdio.h>
#include "int_sizes.h"

/** @file nvpair.h
An nvpair object is a collection of name-value pairs that might
describe a complex object such as a host or a job.  An nvpair object
is a subset of the full generality of an XML document or a ClassAd.
In fact, and nvpair can easily be exported into these and other formats.
We use an nvapir object instead of these other database, because it
has a dramatically simpler implementation that these other complex datatypes
and removes any dependence on external software.
*/

/** Create an empty nvpair.
@return A pointer to an nvpair.
*/
struct nvpair *nvpair_create();

/** Delete an nvpair.  Also deletes all contained names and values.
@param n The nvpair to delete.
*/
void nvpair_delete(struct nvpair *n);

/** Load in an nvpair from ASCII text.
@param n An nvpair from @ref nvpair_create.
@param text The ASCII text to parse.
*/
void nvpair_parse(struct nvpair *n, const char *text);

/** Load in an nvpair from a standard I/O stream.
@param n An nvpair from @ref nvpair_create.
@param stream The I/O stream to read.
*/
int nvpair_parse_stream(struct nvpair *n, FILE * stream);

/** Print an nvpair to ASCII text with a limit.
@param n The npvair to print.
@param text The buffer to print to.
@param length The length of the buffer in bytes.
@return The actual length of buffer needed, in bytes.
*/
int nvpair_print(struct nvpair *n, char *text, int length);

/** Print an nvpair to ASCII text, allocating the needed buffer.
@param n The npvair to print.
@param text A pointer to a buffer pointer that will be allocated to the needed size.
@return The actual number of bytes written.
*/
int nvpair_print_alloc(struct nvpair *n, char **text);

/** Insert a property in string form.
@param n The nvpair to modify.
@param name The name of the property to insert.
@param value The value of the property to insert.
*/
void nvpair_insert_string(struct nvpair *n, const char *name, const char *value);

/** Insert a property in integer form.
@param n The nvpair to modify.
@param name The name of the property to insert.
@param value The value of the property to insert.
*/ void nvpair_insert_integer(struct nvpair *n, const char *name, INT64_T value);

/** Lookup a property in string form.
@param n The nvpair to examine.
@param name The name of the property to return.
@return A pointer to the property string, if present, otherwise null.
*/
const char *nvpair_lookup_string(struct nvpair *n, const char *name);

/** Lookup a property in integer form.
@param n The nvpair to examine.
@param name The name of the property to return.
@return The integer value of the property, or zero if not present or not an integer.
*/
INT64_T nvpair_lookup_integer(struct nvpair *n, const char *name);

typedef enum {
	NVPAIR_MODE_STRING,
	NVPAIR_MODE_INTEGER,
	NVPAIR_MODE_URL,
	NVPAIR_MODE_METRIC
} nvpair_mode_t;

typedef enum {
	NVPAIR_ALIGN_LEFT,
	NVPAIR_ALIGN_RIGHT
} nvpair_align_t;

struct nvpair_header {
	const char *name;
	nvpair_mode_t mode;
	nvpair_align_t align;
	int width;
};

/** Print an entire nvpair in text form.
@param n The nvpair to print.
@param stream The stream on which to print.
*/
void nvpair_print_text(struct nvpair *n, FILE * stream);

/** Print an entire nvpair in XML form.
@param n The nvpair to print.
@param stream The stream on which to print.
*/
void nvpair_print_xml(struct nvpair *n, FILE * stream);

/** Print an entire nvpair in new ClassAd form.
@param n The nvpair to print.
@param stream The stream on which to print.
*/
void nvpair_print_new_classads(struct nvpair *n, FILE * stream);

/** Print an entire nvpair in old ClassAd form.
@param n The nvpair to print.
@param stream The stream on which to print.
*/
void nvpair_print_old_classads(struct nvpair *n, FILE * stream);

/** Print an entire nvpair in HTML form.
@param n The nvpair to print.
@param stream The stream on which to print.
*/
void nvpair_print_html_solo(struct nvpair *n, FILE * stream);

void nvpair_print_html_header(FILE * stream, struct nvpair_header *h);
void nvpair_print_html(struct nvpair *n, FILE * stream, struct nvpair_header *h);
void nvpair_print_html_with_link(struct nvpair *n, FILE * stream, struct nvpair_header *h, const char *linkname, const char *linktext);

void nvpair_print_html_footer(FILE * stream, struct nvpair_header *h);

void nvpair_print_table_header(FILE * stream, struct nvpair_header *h);
void nvpair_print_table(struct nvpair *n, FILE * stream, struct nvpair_header *h);
void nvpair_print_table_footer(FILE * stream, struct nvpair_header *h);

#endif
