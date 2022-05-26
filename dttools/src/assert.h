#ifndef CCTOOLS_ASSERT_H
#define CCTOOLS_ASSERT_H

#include <stdio.h>
#include <stdlib.h>

/* Tests use this assert and do not have CCTOOLS_SOURCE defined. */
#ifndef CCTOOLS_SOURCE
#   define CCTOOLS_SOURCE "test"
#endif

#ifndef NDEBUG
#   define cctools_assert(exp) \
	do {\
		if (!(exp)) {\
			fprintf(stderr, "%s: %s:%d[%s]: Assertion '%s' failed.\n", __func__, __FILE__, __LINE__, CCTOOLS_SOURCE, #exp);\
			fflush(stderr);\
			abort();\
		}\
	} while (0)
#else
#   define cctools_assert(exp) ((void)0)
#endif

#undef assert
#define assert(expr) cctools_assert(expr)

#endif /* CCTOOLS_ASSERT_H */
