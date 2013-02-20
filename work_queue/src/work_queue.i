/* work_queue.i */
%module work_queue

%{
    #include "debug.h"
    #include "int_sizes.h"
    #include "timestamp.h"
    #include "work_queue.h"
%}

%include "stdint.i"
%include "debug.h"
%include "int_sizes.h"
%include "timestamp.h"
%include "work_queue.h"
