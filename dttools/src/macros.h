/*
Copyright (C) 2003-2004 Douglas Thain and the University of Wisconsin
Copyright (C) 2005- The University of Notre Dame
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

#define DIV_INT_ROUND_UP(a, b) ((__typeof__(a)) ((int64_t) ((((a) + (b) - 1) / (b)))))

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

#define USECOND 1000000

#endif
