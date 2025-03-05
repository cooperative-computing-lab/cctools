/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2022 The University of Notre Dame
This software is distributed under the GNU General Public License.
See the file COPYING for details.
*/

#ifndef MACROS_H
#define MACROS_H

#ifndef MAX
#define MAX(a,b) ( ((a)>(b)) ? (a) : (b) )
#endif

#ifndef MIN
#define MIN(a,b) ( ((a)<(b)) ? (a) : (b) )
#endif

/* treat negative numbers  as 'nulls' */
#ifndef MIN_POS
#define MIN_POS(a,b) ((a) < 0 ? (b) : ((b) < 0 ? (a) : MIN((a), (b))))
#endif

#ifndef ABS
#define ABS(x) ( ((x)>=0) ? (x) : (-(x)) )
#endif

#define DIV_INT_ROUND_UP(a, b) ((__typeof__(a)) ((int64_t) (((((double) (a)) + ((double) (b)) - 1) / (b)))))

#define KILO 1024
#define MEGA (KILO*KILO)
#define GIGA (KILO*MEGA)
#define TERA (KILO*GIGA)
#define PETA (KILO*TERA)

#define KILOBYTE KILO
#define MEGABYTE MEGA
#define GIGABYTE GIGA
#define TERABYTE TERA
#define PETABYTE PETA

#define BYTES_TO_STORAGE_UNIT(x, unit) (ceil(((double) x) / unit))
#define BYTES_TO_KILOBYTES(x) BYTES_TO_STORAGE_UNIT(x, KILOBYTE)
#define BYTES_TO_MEGABYTES(x) BYTES_TO_STORAGE_UNIT(x, MEGABYTE)
#define BYTES_TO_GIGABYTES(x) BYTES_TO_STORAGE_UNIT(x, GIGABYTE)
#define BYTES_TO_TERABYTES(x) BYTES_TO_STORAGE_UNIT(x, TERABYTE)
#define BYTES_TO_PETABYTES(x) BYTES_TO_STORAGE_UNIT(x, PETABYTE)

#define USECOND 1000000

#endif
