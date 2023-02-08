/* taskvine.i */
%module taskvine

%include carrays.i
%array_functions(struct rmsummary *, rmsummayArray);

/* type is a go keyword. rename it to value_type */
%rename(value_type) rmsummary_field::type;

%begin %{
	#define SWIG_PYTHON_2_UNICODE
%}

%{
	#include "int_sizes.h"
	#include "timestamp.h"
	#include "taskvine.h"
	#include "rmsummary.h"
    #include "vine_task.h"
%}

/* We compile with -D__LARGE64_FILES, thus off_t is at least 64bit.
long long int is guaranteed to be at least 64bit. */
%typemap(in) off_t = long long int;

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;
%ignore debug;

/* returns a char*, enable automatic free */
%newobject work_queue_status;

/* These return pointers to lists defined in list.h. We aren't
 * wrapping methods in list.h and so ignore these. */
%ignore vine_cancel_all_tasks;
%ignore input_files;
%ignore output_files;

/* When we enounter buffer_length in the prototype of vine_task_get_output_buffer,
treat it as an output parameter to be filled in. */

%apply int *OUTPUT { int *buffer_length };

%include "stdint.i"
%include "int_sizes.h"
%include "timestamp.h"
%include "taskvine.h"
%include "rmsummary.h"
%include "category.h"
%include "vine_task.h"



