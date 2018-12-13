/* chirp.i */
%module chirp

/* next is a perl keyword. rename it to next_entry */
%rename(next_entry) chirp_dirent::next;

/* silent const char leaking memory, as we do not leak memory */
%warnfilter(451) chirp_searchstream::current;

%{
	#include <time.h>
	#include "debug.h"
	#include "int_sizes.h"
	#include "timestamp.h"
	#include "auth_all.h"
	#include "auth_ticket.h"
	#include "chirp_recursive.h"
	#include "chirp_reli.h"
	#include "chirp_types.h"
	#include "chirp_swig_wrap.h"
%}

%typemap(in) off_t = int;

/*Function name to camelcase which follows Golang convention*/
%rename("%(camelcase)s") "";

%typemap(in) time_t
{
#ifdef SWIGPYTHON
	if (PyLong_Check($input))
		$1 = (time_t) PyLong_AsLong($input);
	else if (PyInt_Check($input))
		$1 = (time_t) PyInt_AsLong($input);
	else if (PyFloat_Check($input))
		$1 = (time_t) PyFloat_AsDouble($input);
	else {
		PyErr_SetString(PyExc_TypeError,"Expected a number");
		return NULL;
	}
#endif
#ifdef SWIGPERL
	$1 = (uint64_t) SvIV($input);
#endif
}

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;
%ignore debug;

/* %ignore fname..; */

%include "stdint.i"
%include "debug.h"
%include "int_sizes.h"
%include "timestamp.h"
%include "auth_all.h"
%include "auth_ticket.h"
%include "chirp_reli.h"
%include "chirp_recursive.h"
%include "chirp_types.h"
%include "chirp_recursive.h"
%include "chirp_swig_wrap.h"

/* vim: set noexpandtab tabstop=4: */
