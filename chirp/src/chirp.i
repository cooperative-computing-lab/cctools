/* chirp.i */
%module CChirp

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










