/* work_queue.i */
%module work_queue

%{
    #include "debug.h"
    #include "int_sizes.h"
    #include "timestamp.h"
    #include "work_queue.h"
%}

/* vdebug() takes va_list as arg but SWIG can't wrap such functions. */
%ignore vdebug;  

%include "stdint.i"
%include "debug.h"
%include "int_sizes.h"
%include "timestamp.h"
%include "work_queue.h"

