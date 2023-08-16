/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef CHIRP_ARRAY_H
#define CHIRP_ARRAY_H

#include "chirp_types.h"

/** @file chirp_matrix.h Stores very large distributed matrices.
This module manages the storage of very large matrices
(ranging from gigabytes to terabytes) by distributing the data across multiple
Chirp servers.  By harnessing the aggregate memory and storage of multiple
machines, the time to process data is dramatically reduced.
*/

/** Create a new distributed matrix.
The host and path specified here will be used to store a small <i>index file</i> that contains the configuration of the matrix.
The actual pieces of the matrix will be spread across multiple hosts.
To tell Chirp where to store those pieces, you must create a <i>hosts file</i> that is a simple list of host names separated by newlines.  Chirp will look for this file in $CHIRP_HOSTS, then in $HOME/.chirp/hosts.  If neither is available, this call will fail.
@param host The hostname and optional port of the index file.
@param path The path to the index file.
@param width The number of elements in one row.
@param height The number of elements in one column.
@param element_size The size in bytes of each element in the matrix.
@param nhosts The number of hosts on which to spread the data.
@param stoptime The absolute time at which to abort.
@return On success, a pointer to a struct chirp_matrix.  On failure, returns zero and sets errno appropriately.
@see chirp_matrix_open, chirp_matrix_delete
*/

struct chirp_matrix *chirp_matrix_create(const char *host, const char *path, int width, int height, int element_size, int nhosts, time_t stoptime);

/** Open an existing matrix.
@param host The hostname and optional port of the index file.
@param path The path to the index file.
@param stoptime The absolute time at which to abort.
@return On success, a pointer to a struct chirp_matrix.  On failure, returns zero and sets errno appropriately.
@see chirp_matrix_close
 */

struct chirp_matrix *chirp_matrix_open(const char *host, const char *path, time_t stoptime);

/** Get all values in a row.
This is the most efficient way to access data in a matrix.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param y The y position of the row.
@param data A pointer to a buffer where to store the data.
@param stoptime The absolute time at which to abort.
*/

int chirp_matrix_get_row(struct chirp_matrix *matrix, int y, void *data, time_t stoptime);

/** Set all values in a row.
This is the most efficient way to access data in a matrix.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param y The y position of the row.
@param data A pointer to a buffer containing the data to write.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_set_row(struct chirp_matrix *matrix, int y, const void *data, time_t stoptime);

/** Get all values in a column.
Note that accessing columns is not as efficient as accessing rows.
If possible, use @ref chirp_matrix_get_row instead.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param x The x position of the column.
@param data A pointer to a buffer where to store the data.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_get_col(struct chirp_matrix *matrix, int x, void *data, time_t stoptime);

/** Set all values in a column.
Note that accessing columns is not as efficient as accessing rows.
If possible, use @ref chirp_matrix_set_row instead.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param x The x position of the column.
@param data A pointer to a buffer containing the data to write.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_set_col(struct chirp_matrix *matrix, int x, const void *data, time_t stoptime);

/** Get a range of data.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param x The starting x position of the range.
@param y The starting y position of the range;
@param width The width of the range in cells.
@param height The width of the range in cells.
@param data A pointer to a buffer where to store the data.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_get_range(struct chirp_matrix *matrix, int x, int y, int width, int height, void *data, time_t stoptime);

/** Set a range of data.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param x The starting x position of the range.
@param y The starting y position of the range;
@param width The width of the range in cells.
@param height The width of the range in cells.
@param data A pointer to a buffer containing the data to write.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_set_range(struct chirp_matrix *matrix, int x, int y, int width, int height, const void *data, time_t stoptime);

/** Get a single element.
Note: Reading a single element at a time is very inefficient.
If possible, get multiple elements at once using @ref chirp_matrix_get_row.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param x The x position of the element.
@param y The y position of the element.
@param data A pointer to a buffer where to store the element.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_get(struct chirp_matrix *matrix, int x, int y, void *data, time_t stoptime);

/** Set a single element.
Note: Writing a single element at a time is very inefficient.
If possible, set multiple elements at once using @ref chirp_matrix_set_row.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param x The x position of the element.
@param y The y position of the element.
@param data A pointer to a buffer where to store the element.
@param stoptime The absolute time at which to abort.
@return Greater than or equal to zero on success, negative on failure.
*/

int chirp_matrix_set(struct chirp_matrix *matrix, int x, int y, const void *data, time_t stoptime);

/** Set the acls on a matrix.
*/

int chirp_matrix_setacl(const char *host, const char *path, const char *subject, const char *rights, time_t stoptime);


/** Get the width of a matrix
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@return The width of the matrix, measured in elements.
*/

int chirp_matrix_width(struct chirp_matrix *matrix);

/** Get the height of a matrix
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@return The height of the matrix, measured in elements.
*/

int chirp_matrix_height(struct chirp_matrix *matrix);

/** Get the element size of a matrix.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@return The size of each element in the matrix, measured in bytes.
*/

int chirp_matrix_element_size(struct chirp_matrix *matrix);

/** Get the number of hosts used by a matrix.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@return The number of hosts used by the matrix.
*/

int chirp_matrix_nhosts(struct chirp_matrix *matrix);

/** Get the number of files used by a matrix.
This value might be greater than the number of hosts, if the matrix is very large and it is necessary
to break it into multiple files of one gigabyte or less.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@return The number of files used by the matrix.
*/

int chirp_matrix_nfiles(struct chirp_matrix *matrix);

/** Force all data to disk.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param stoptime The absolute time at which to abort.
*/

void chirp_matrix_fsync(struct chirp_matrix *matrix, time_t stoptime);

/** Close a matrix and free all related resources.
@param matrix A pointer to a chirp_matrix returned by @ref chirp_matrix_create or @ref chirp_matrix_open
@param stoptime The absolute time at which to abort.
*/

void chirp_matrix_close(struct chirp_matrix *matrix, time_t stoptime);

/** Delete a matrix.
@param host The hostname and optional port of the index file.
@param path The path to the index file.
@param stoptime The absolute time at which to abort.
*/

int chirp_matrix_delete(const char *host, const char *path, time_t stoptime);

#endif

/* vim: set noexpandtab tabstop=8: */
