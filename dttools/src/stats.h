/*
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef STATS_H
#define STATS_H

#include <stdio.h>
#include <stdint.h>
#include "jx.h"

/** Collect statistics for the current program.
 */
void stats_enable (void);

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

/** Record an event, binned by value
 * For frequent events like read()s, it would be expensive to record the size
 * of each and every one. Instead, this function records a histogram with
 * logarithmic bins, to give an idea of the distribution of event values.
 * @param name The key to log.
 * @param value The value log.
 */
void stats_bin (const char *name, uint64_t value);

/** Get the current statistics in JSON format.
 * The returned object is a mapping of key names to values. For simple
 * counters, the value is a number. A histogram is represented as an
 * array of counts.
 */
struct jx *stats_get (void);

#endif
