/*
Copyright (C) 2025 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

/** @file progress_bar.c
Implementation of a terminal progress bar with multiple parts.
*/

#include "progress_bar.h"
#include "xxmalloc.h"
#include "macros.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <assert.h>
#include <stdint.h>
#include <inttypes.h>

/* Max bar width (in block characters) for single-line rendering. */
#define MAX_BAR_WIDTH 30
/* Typed time constants (microseconds). */
static const timestamp_t SECOND_US      = 1000000ULL;
static const timestamp_t MILLISECOND_US = 1000ULL;
static const timestamp_t MICROSECOND_US = 1ULL;

/* Minimum redraw interval to avoid flicker (200ms). */
#define PROGRESS_BAR_UPDATE_INTERVAL_US (SECOND_US / 5)

#define COLOR_RESET "\033[0m"
#define COLOR_GREEN "\033[32m"
#define COLOR_CYAN "\033[38;2;0;255;255m"
#define COLOR_ORANGE "\033[38;2;255;165;0m"
#define COLOR_PURPLE "\033[38;2;128;0;128m"
#define COLOR_PINK "\033[38;2;255;192;203m"
#define COLOR_YELLOW "\033[38;2;255;255;0m"

/** Get terminal width in columns; return 80 on failure. */
static int get_terminal_width()
{
	struct winsize w;

	if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == -1) {
		return 80;
	}

	return w.ws_col;
}

/** Compute bar width based on terminal and labels; clamp to bounds. */
static int compute_bar_width(const char *label, int part_text_len)
{
	if (!label) {
		return 0;
	}

	int term_width = get_terminal_width();
	int label_len = strlen(label);
	int bar_width = term_width - label_len - part_text_len - 28;

	if (bar_width > MAX_BAR_WIDTH) {
		bar_width = MAX_BAR_WIDTH;
	}

	if (bar_width < 10) {
		bar_width = 10;
	}

	return (int)(bar_width * 0.8);
}

/** Render one-line progress bar with aggregated totals, progress, and elapsed time. */
static void print_progress_bar(struct ProgressBar *bar)
{
	if (!bar) {
		return;
	}

	bar->last_draw_time_us = timestamp_get();

	char part_text[256];
	char *ptr = part_text;
	int remain = sizeof(part_text);
	int written = snprintf(ptr, remain, "[");
	ptr += written;
	remain -= written;

	uint64_t total_sum = 0;
	uint64_t current_sum = 0;

	bool first = true;
	struct ProgressBarPart *p;
	LIST_ITERATE(bar->parts, p)
	{
		total_sum += p->total;
		current_sum += p->current;

		if (!first) {
			written = snprintf(ptr, remain, ", ");
			ptr += written;
			remain -= written;
		}

		written = snprintf(ptr, remain, "%s: %" PRIu64 "/%" PRIu64, p->label, p->current, p->total);
		ptr += written;
		remain -= written;

		first = false;
	}
	snprintf(ptr, remain, "]");
	part_text[sizeof(part_text) - 1] = '\0';

	float progress = (total_sum > 0) ? ((float)current_sum / total_sum) : 0.0f;
	if (progress > 1.0f) {
		progress = 1.0f;
	}

	timestamp_t elapsed = timestamp_get() - bar->start_time_us;
	int h = elapsed / (3600LL * SECOND_US);
	int m = (elapsed % (3600LL * SECOND_US)) / (60LL * SECOND_US);
	int s = (elapsed % (60LL * SECOND_US)) / SECOND_US;

	if (bar->has_drawn_once) {
		printf("\r\033[2K");
	} else {
		bar->has_drawn_once = 1;
	}

	int part_text_len = (int)(ptr - part_text) + 1;
	int bar_width = compute_bar_width(bar->label, part_text_len);
	int filled = (int)(progress * bar_width);

	char bar_line[MAX_BAR_WIDTH * 3 + 1];
	int offset = 0;
	const char *block = "━";

	for (int i = 0; i < filled; ++i) {
		memcpy(bar_line + offset, block, 3);
		offset += 3;
	}

	memset(bar_line + offset, ' ', (bar_width - filled));
	offset += (bar_width - filled);
	bar_line[offset] = '\0';

	printf("%s " COLOR_GREEN "%s %" PRIu64 "/%" PRIu64 COLOR_YELLOW " %s" COLOR_CYAN " %.1f%%" COLOR_ORANGE " %02d:%02d:%02d" COLOR_RESET,
			bar->label ? bar->label : "",
			bar_line,
			current_sum,
			total_sum,
			part_text,
			progress * 100,
			h,
			m,
			s);

	fflush(stdout);
}

/** Create and initialize a progress bar. */
struct ProgressBar *progress_bar_init(const char *label)
{
	if (!label) {
		return NULL;
	}

	struct ProgressBar *bar = xxmalloc(sizeof(struct ProgressBar));

	bar->label = xxstrdup(label);
	bar->parts = list_create();
	bar->start_time_us = timestamp_get();
	bar->last_draw_time_us = 0;
	bar->update_interval_us = PROGRESS_BAR_UPDATE_INTERVAL_US;
	bar->update_interval_sec = (double)bar->update_interval_us / SECOND_US;
	bar->has_drawn_once = 0;

	return bar;
}

/** Set the update interval for the progress bar. */
void progress_bar_set_update_interval(struct ProgressBar *bar, double update_interval_sec)
{
	if (!bar) {
		return;
	}

	if (update_interval_sec < 0) {
		update_interval_sec = 0;
	}
	bar->update_interval_sec = update_interval_sec;
	/* Convert seconds to microseconds with saturation to avoid overflow. */
	if (update_interval_sec >= (double)UINT64_MAX / (double)SECOND_US) {
		bar->update_interval_us = (timestamp_t)UINT64_MAX;
	} else {
		bar->update_interval_us = (timestamp_t)(update_interval_sec * (double)SECOND_US);
	}
}

/** Create a new part. */
struct ProgressBarPart *progress_bar_create_part(const char *label, uint64_t total)
{
	if (!label) {
		return NULL;
	}

	struct ProgressBarPart *part = xxmalloc(sizeof(struct ProgressBarPart));

	part->label = xxstrdup(label);
	part->total = total;
	part->current = 0;

	return part;
}

/** Bind a part to the progress bar. */
void progress_bar_bind_part(struct ProgressBar *bar, struct ProgressBarPart *part)
{
	if (!bar || !part) {
		return;
	}

	list_push_tail(bar->parts, part);
	print_progress_bar(bar);
}

/** Set the total for a part. */
void progress_bar_set_part_total(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t new_total)
{
	if (!bar || !part) {
		return;
	}

	part->total = new_total;
}

/** Advance a part's current value, redraw if needed. */
void progress_bar_update_part(struct ProgressBar *bar, struct ProgressBarPart *part, uint64_t increment)
{
	if (!bar || !part) {
		return;
	}

	part->current += increment;
	if (part->current > part->total) {
		part->current = part->total;
	}

	timestamp_t now_us = timestamp_get();
	if (!bar->has_drawn_once || (now_us - bar->last_draw_time_us) >= bar->update_interval_us) {
		print_progress_bar(bar);
	}
}

/** Set the start time for the progress bar. */
void progress_bar_set_start_time(struct ProgressBar *bar, timestamp_t start_time)
{
	if (!bar) {
		return;
	}

	bar->start_time_us = start_time;
}

/** Final render and newline. */
void progress_bar_finish(struct ProgressBar *bar)
{
	if (!bar) {
		return;
	}

	print_progress_bar(bar);
	printf("\n");
}

/** Free the progress bar, its parts, and internal resources. */
void progress_bar_delete(struct ProgressBar *bar)
{
	if (!bar) {
		return;
	}

	free(bar->label);
	struct ProgressBarPart *p;
	LIST_ITERATE(bar->parts, p)
	{
		free(p->label);
		free(p);
	}
	list_delete(bar->parts);
	free(bar);
}
