#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
/* Eventually this file should disappear, and only inttypes.h should be used.*/

#ifndef INT_SIZES_H
#define INT_SIZES_H

#define INT8_T   int8_t
#define INT16_T  int16_t
#define INT32_T  int32_t
#define INT64_T  int64_t
#define PTRINT_T intptr_t

#define INT8_FORMAT  "%" PRId8
#define INT16_FORMAT "%" PRId16
#define INT32_FORMAT "%" PRId32
#define INT64_FORMAT "%" PRId64
#define PTR_FORMAT   "%" PRIxPTR

#define UINT8_T   uint8_t
#define UINT16_T  uint16_t
#define UINT32_T  uint32_t
#define UINT64_T  uint64_t
#define UPTRINT_T uintptr_t

#define UINT8_FORMAT  "%" PRIu8
#define UINT16_FORMAT "%" PRIu16
#define UINT32_FORMAT "%" PRIu32
#define UINT64_FORMAT "%" PRIu64
#define UPTR_FORMAT   "%" UPRIxPTR

#endif
