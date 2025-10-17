/*
Copyright (C) 2025 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file progress_bar.h
Terminal progress bar API with multiple parts.
*/

#ifndef PROGRESS_BAR_H
#define PROGRESS_BAR_H

#include "list.h"
#include "timestamp.h"
#include <time.h>
#include <stdint.h>

/** A part of a progress bar. */
struct ProgressBarPart {
	char *label;
	uint64_t total;
	uint64_t current;
};

/** Progress bar object. */
struct ProgressBar {
	char *label;
	struct list *parts;
	timestamp_t start_time;
    timestamp_t last_draw_time;
	int has_drawn_once;
};

/* Progress Bar Part API */

/** Create a progress bar.
@param label Progress bar label (internally duplicated).
@return New progress bar.
*/
struct ProgressBar *progress_bar_init(const char *label);

/** Create a new part.
@param label Part label (internally duplicated).
@param total Total units for the part.
@return New part.
*/
struct ProgressBarPart *progress_bar_create_part(const char *label, uint64_t total);

/** Bind a part to the progress bar.
@param bar Progress bar.
@param part Part to bind.
*/
void progress_bar_bind_part(struct ProgressBar *bar, struct ProgressBarPart *part);

/** Set the total for a part.
@param bar Progress bar.
@param part Part to update.
@param new_total New total units.
*/
void progress_bar_set_part_total(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t new_total);

/** Update the current value for a part, redraw if needed.
@param bar Progress bar.
@param part Part to advance.
@param increment Amount to add.
*/
void progress_bar_update_part(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t increment);

/** Set the start time for the progress bar.
@param bar Progress bar.
@param start_time Start timestamp.
*/
void progress_bar_set_start_time(struct ProgressBar *bar, timestamp_t start_time);

/** Finish the progress bar: draw once and print a newline.
@param bar Progress bar.
*/
void progress_bar_finish(struct ProgressBar *bar);

/** Delete the progress bar and free all parts.
@param bar Progress bar.
*/
void progress_bar_delete(struct ProgressBar *bar);

#endif
