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
#include "hash_table.h"

/** @file nvpair.h

This module is deprecated, please use @ref jx.h for new code.

An nvpair object is a collection of name-value pairs that might
describe a complex object such as a host or a job.  An nvpair object
is a subset of the full generality of an XML document or a ClassAd.
In fact, an nvpair can easily be exported into these and other formats.
We use an nvapir object instead of these other database, because it
has a dramatically simpler implementation that these other complex datatypes
and removes any dependence on external software.
*/

/**
@deprecated {
**/

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

/** Remove a property from an nvpair.
@param n The nvpair to modify.
@param name The name of the property to remove.
*/
void nvpair_remove( struct nvpair *n, const char *name );

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

/** Insert a property in floating point form.
@param n The nvpair to modify.
@param name The name of the property to insert.
@param value The value of the property to insert.
*/
void nvpair_insert_float(struct nvpair *n, const char *name, double value);

/** Lookup a property in string form.
@param n The nvpair to examine.
@param name The name of the property to return.
@return A pointer to the property string, if present, otherwise null.
*/
const char *nvpair_lookup_string(struct nvpair *n, const char *name);

/** Lookup a property in integer form.
@param n The nvpair to examine.
@param name The name of the property to return.
@return The integer value of the property, or zero if not present or not a number.
*/
INT64_T nvpair_lookup_integer(struct nvpair *n, const char *name);

/** Lookup a property in floating point form.
@param n The nvpair to examine.
@param name The name of the property to return.
@return The floating point value of the property, or zero if not present or not a number.
*/
double nvpair_lookup_float(struct nvpair *n, const char *name);

/**
Export all items in the nvpair to the environment with setenv.
@param nv The nvpair to be exported.
*/
void nvpair_export( struct nvpair *nv );


/** Begin iteration over all items.
This function begins a new iteration over an nvpair,
allowing you to visit every name and value in the table.
Next, invoke @ref nvpair_next_item to retrieve each value in order.
@param nv A pointer to an nvpair.
*/

void nvpair_first_item(struct nvpair *nv);

/** Continue iteration over all items.
This function returns the next name and value in the iteration.
@param nv A pointer to an nvpair.
@param name A pointer to a name pointer.
@param value A pointer to a value pointer.
@return Zero if there are no more elements to visit, one otherwise.
*/

int nvpair_next_item(struct nvpair *nv, char **name, char **value);

/** } **/

#endif
