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
    #include "vine_runtime_dir.h"
%}

/* We compile with -D__LARGE64_FILES, thus off_t is at least 64bit.
long long int is guaranteed to be at least 64bit. */
%typemap(in) off_t = long long int;

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;
%ignore debug;

/* return a char*, enable automatic free */
%newobject vine_get_status;
%newobject vine_get_runtime_path_staging;

/* These return pointers to lists defined in list.h. We aren't
 * wrapping methods in list.h and so ignore these. */
%ignore vine_cancel_all_tasks;
%ignore input_files;
%ignore output_files;

/* When we enounter buffer_length in the prototype of vine_task_get_output_buffer,
treat it as an output parameter to be filled in. */
%apply int *OUTPUT { int *buffer_length };


/* Convert a python buffer into a vine buffer */
/* Note!! This changes any C function with the signature f(const char *data, int length) into
a swig function f(data) */
%typemap(in, numinputs=1) (const char *data, int length) {
    if ($input == Py_None) {
        $1 = NULL;
        $2 = 0;
    } else {
        Py_buffer view;
        if (PyObject_GetBuffer($input, &view, PyBUF_SIMPLE) != 0) {
            PyErr_SetString(
                    PyExc_TypeError,
                    "in method '$symname', argument $argnum is not a simple buffer");
            SWIG_fail;
        }
        $1 = view.buf;
        $2 = view.len;
        PyBuffer_Release(&view);
    }
}
%typemap(doc) const char *data, int length "$1_name: a readable buffer (e.g. a bytes object)"

%include "stdint.i"
%include "int_sizes.h"
%include "timestamp.h"
%include "taskvine.h"
%include "rmsummary.h"
%include "category.h"
%include "vine_task.h"
%include "vine_runtime_dir.h"



