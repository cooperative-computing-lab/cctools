/* work_queue.i */
%module work_queue

%include carrays.i
%array_functions(struct rmsummary *, rmsummayArray);

/* type is a go keyword. rename it to value_type */
%rename(value_type) rmsummary_field::type;

%begin %{
	#define SWIG_PYTHON_2_UNICODE
%}

%{
	#include "debug.h"
	#include "int_sizes.h"
	#include "timestamp.h"
	#include "work_queue.h"
	#include "rmsummary.h"
%}

/* We compile with -D__LARGE64_FILES, thus off_t is at least 64bit.
long long int is guaranteed to be at least 64bit. */
%typemap(in) off_t = long long int;

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;
%ignore debug;

/* These return pointers to lists defined in list.h. We aren't
 * wrapping methods in list.h and so ignore these. */
%ignore work_queue_cancel_all_tasks;
%ignore input_files;
%ignore output_files;

%include "stdint.i"
%include "debug.h"
%include "int_sizes.h"
%include "timestamp.h"
%include "work_queue.h"
%include "rmsummary.h"
%include "category.h"


