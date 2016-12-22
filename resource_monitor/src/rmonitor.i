/* ResourceMonitor.i */

/* Copyright (C) 2016- The University of Notre Dame This software is
 * distributed under the GNU General Public License.  See the file COPYING for
 * details. */

%module cResourceMonitor

%{
	#include "debug.h"
	#include "int_sizes.h"
	#include "timestamp.h"
	#include "category_internal.h"
	#include "category.h"
	#include "rmonitor_poll.h"
	#include "rmsummary.h"
%}

%typemap(in) off_t = int;

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;
%ignore debug;

%include "stdint.i"
%include "debug.h"
%include "int_sizes.h"
%include "timestamp.h"
%include "category_internal.h"
%include "category.h"
%include "rmonitor_poll.h"
%include "rmsummary.h"

