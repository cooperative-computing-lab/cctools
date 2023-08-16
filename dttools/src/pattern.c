#include "debug.h"

#include <ctype.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#undef lua_State
#define lua_State void
#undef LUA_MAXCAPTURES
#define LUA_MAXCAPTURES 256
#undef MAXCCALLS
#define MAXCCALLS 200
#undef LUA_QL
#define LUA_QL(x) "'" x "'"
#undef uchar
#define uchar(c) ((unsigned char)(c))

static int luaL_error (void *ms, const char *fmt, ...)
{
	va_list va;
	va_start(va, fmt);
	vdebug(D_FATAL|D_NOTICE|D_DEBUG, fmt, va);
	va_end(va);
	abort();
	return 0;
}

#include "luapatt.c"

ptrdiff_t pattern_vmatch (const char *str, const char *patt, va_list va)
{
	MatchState ms;
	int anchor = (*patt == '^');
	if (anchor) {
		patt++; /* skip anchor character */
	}
	ms.matchdepth = MAXCCALLS;
	ms.src_init = str;
	ms.src_end = str + strlen(str);
	ms.p_end = patt + strlen(patt);
	do {
		const char *rest;
		ms.level = 0;
		if ((rest = match(&ms, str, patt))) {
			int i;
			for (i = 0; i < ms.level; i++) {
				ptrdiff_t l = ms.capture[i].len;
				if (l == CAP_UNFINISHED)
					luaL_error(ms.L, "unfinished capture");
				else if (l == CAP_POSITION) {
					size_t *capture = va_arg(va, size_t *);
					*capture = ms.capture[i].init - ms.src_init;
				} else {
					char **capture = va_arg(va, char **);
					*capture = malloc(l+1);
					if (*capture == NULL)
						luaL_error(ms.L, "out of memory");
					strncpy(*capture, ms.capture[i].init, l);
					(*capture)[l] = '\0';
				}
			}
			return str-ms.src_init;
		}
	} while (str++ < ms.src_end && !anchor);
	return -1;
}

ptrdiff_t pattern_match (const char *str, const char *patt, ...)
{
	ptrdiff_t rc;
	va_list va;
	va_start(va, patt);
	rc = pattern_vmatch(str, patt, va);
	va_end(va);
	return rc;
}

/* vim: set noexpandtab tabstop=8: */
