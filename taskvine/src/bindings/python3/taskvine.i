/* taskvine.i */
%module cvine

%include carrays.i
%array_functions(struct rmsummary *, rmsummayArray);

%begin %{
	#define SWIG_PYTHON_2_UNICODE
%}

%{
	#include "int_sizes.h"
	#include "taskvine.h"
%}

/* We compile with -D__LARGE64_FILES, thus off_t is at least 64bit.
long long int is guaranteed to be at least 64bit. */
%typemap(in) off_t = long long int;
%typemap(in) size_t = unsigned long long;

/* return a char*, enable automatic free */
%newobject vine_get_status;
%newobject vine_get_path_staging;
%newobject vine_get_path_cache;
%newobject vine_get_path_log;
%newobject vine_version_string;

/* These return pointers to lists defined in list.h. We aren't
 * wrapping methods in list.h and so ignore these. */
%ignore vine_cancel_all_tasks;
%ignore input_files;
%ignore output_files;

/* When we enounter buffer_length in the prototype of vine_task_get_output_buffer,
treat it as an output parameter to be filled in. */
%apply int *OUTPUT { int *buffer_length };


/* Convert a python buffer into a vine buffer */
/* Note!! This changes any C function with the signature f(struct vine_manager *m, const char *buffer, size_t size)
into a swig function f(data) */
%typemap(in, numinputs=1) (const char *buffer, size_t size) {
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

/* Convert a C array of binary data to Python bytes. */
%inline %{
    PyObject *vine_file_contents_as_bytes(struct vine_file *f) {
        return PyBytes_FromStringAndSize(vine_file_contents(f), vine_file_size(f));
    }
%}

%include "stdint.i"
%include "int_sizes.h"
%include "taskvine.h"
