/*
Copyright (C) 2017- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef STATS_H
#define STATS_H

#include <stdio.h>
#include <stdint.h>
#include "buffer.h"

/** Collect statistics for the current program.
 */
void stats_enable ();

/** Clear a statistic.
 * @param name The key to clear.
 */
void stats_unset (const char *name);

/** Set an integer statistic.
 * Any previous value will be cleared.
 * @param name The key to set.
 * @param value The value to set.
 */
void stats_set (const char *name, int64_t value);

/** Increment an integer statistic.
 * Adding a negative number is fine.
 * If the given key does not exist, it will be initialized to zero.
 * @param name The key to set.
 * @param offset The signed quantity to add.
 */
void stats_inc (const char *name, int64_t offset);

/** Record an event by value
 * For frequent events like read()s, it would be expensive to record the size
 * of each and every one. Instead, this function records a histogram with
 * logarithmic bins, to give an idea of the distribution of event values.
 * @param name The key to log.
 * @param value The value log.
 */
void stats_log (const char *name, uint64_t value);

/** Write a human-readable representation of the program statistics to a buffer.
 * @param buf The initialized buffer to use.
 */
void stats_print_buffer (buffer_t *b);

/** Write a human-readable representation of the program statistics to a stream.
 * @param file The stdio stream to write to.
 */
void stats_print_stream (FILE *file);

/** Write a human-readable representation of the program statistics to a string.
 * The caller is responsible for freeing the returned string.
 */
char *stats_print_string ();

#endif
