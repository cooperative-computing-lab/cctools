/* chirp.i */
%module cchirp

/* next is a perl keyword. rename it to next_entry */
%rename(next_entry) chirp_dirent::next;

/* type is a go keyword. rename it to type_io */
%rename(type_io) chirp_bulkio::type;

/* silent const char leaking memory, as we do not leak memory */
%warnfilter(451) chirp_searchstream::current;

%begin %{
	#define SWIG_PYTHON_2_UNICODE
%}


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
