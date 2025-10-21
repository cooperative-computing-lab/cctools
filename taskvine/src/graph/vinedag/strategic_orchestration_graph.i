/* SWIG interface for local vinedag graph API bindings */
%module capi

%{
#include "int_sizes.h"
#include "strategic_orchestration_graph.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

%include "strategic_orchestration_graph.h"
