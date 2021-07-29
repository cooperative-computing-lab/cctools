/*
Copyright (C) 2016- The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include "int_sizes.h"

/** @file histogram.h Keep counts of doubles that fall in some given bucket size.
<pre>
struct histogram *h;
double bucket_size = .5;
h = histogram_create(bucket_size);

// same bucket
histogram_insert(h, 3.0);
histogram_insert(h, 3.1415);

// same bucket
histogram_insert(h, 21.999);

// same bucket
histogram_insert(h, 22.0);
histogram_insert(h, 22.2);
histogram_insert(h, 22.499);

// same bucket
histogram_insert(h, 22.5);

// same bucket
histogram_insert(h, -21.999);

// same bucket
histogram_insert(h, -22.0);
histogram_insert(h, -22.2);
histogram_insert(h, -22.499);

// same bucket
histogram_insert(h, -22.5);

double *buckets = histogram_buckets(h);
double b        = histogram_bucket_size(h);

int i;
for(i = 0; i < histogram_size(h); i++) {

	double start = buckets[i];

	fprintf(stdout, "[%lf, $lf) has %d elements.\n", start, start + b, histogram_count(h, start));
}

free(buckets);
histogram_delete(h);
</pre>
*/

/** Create a new histogram.
@param bucket_size Numbers are grouped according to [n*bucket_size, (n+1)*bucket_size), n in Z.
@return A pointer to a new histogram.
*/

struct histogram *histogram_create(double bucket_size);

/** Remove all entries from a histogram.
@param h The histogram to clear.
*/

void histogram_clear(struct histogram *h);

/** Delete a histogram.
@param h The histogram to delete.
*/

void histogram_delete(struct histogram *h);

/** Count the number of active buckets.
@return The number of active buckets in the histogram.
@param h A pointer to a histogram.
*/

int histogram_size(struct histogram *h);

/** Returns an ordered array with the start values of the active buckets. */
double *histogram_buckets(struct histogram *h);

/** Returns the bucket size. */
double histogram_bucket_size(struct histogram *h);

/** Add value to histogram.
@param h A pointer to a histogram.
@param value A number to add to the histogram.
@return The updated count of the respective bucket.
*/

int histogram_insert(struct histogram *h, double value);

/** Look up the count for the bucket of the given value.
@param h A pointer to a histogram.
@param value A number that would fall inside the desired bucket.
@return The count for the bucket.
*/

int histogram_count(struct histogram *h, double value);

/** Manually set the count for a bucket.
@param h A pointer to a histogram.
@param value A number that would fall inside the desired bucket.
@param count  The desired count.
*/

void histogram_set_bucket(struct histogram *h, double value, int count);

/** Attach custom data to bucket
@param h A pointer to a histogram.
@param value A number that would fall inside the desired bucket.
@param data  A pointer to external data.
*/

void histogram_attach_data(struct histogram *h, double value, void *data);

/** Retrieved custom data attached to the bucket
@param h A pointer to a histogram.
@param value A number that would fall inside the desired bucket.
@return A pointer to external data.
*/

void *histogram_get_data(struct histogram *h, double value);

/** Return the total number of samples in the histogram.
@param h A pointer to a histogram.
@return Count of all the samples.
*/

int histogram_total_count(struct histogram *h);

/** Return the maximum value inserted in the histogram.
@param h A pointer to a histogram.
@return Maximum value inserted.
*/

double histogram_max_value(struct histogram *h);

/** Return the minimum value inserted in the histogram.
@param h A pointer to a histogram.
@return Minimum value inserted.
*/

double histogram_min_value(struct histogram *h);


/** Return the largest value of the bucket that test_value would faill in.
@param h A pointer to a histogram.
@param test_value value to round up
@return test_value rounded up according to bucket size.
*/
double histogram_round_up(struct histogram *h, double test_value);


/** Return the mode of the histogram.
@param h A pointer to a histogram.
@return Histogram mode.
*/

double histogram_mode(struct histogram *h);

#endif
