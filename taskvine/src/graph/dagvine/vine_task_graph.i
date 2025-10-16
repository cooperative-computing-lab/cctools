/* SWIG interface for local dagvine graph bindings */
%module cdagvine

%{
#include "int_sizes.h"
#include "vine_task_graph.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

/* Expose only the dagvine task graph APIs */
%include "vine_task_graph.h"


