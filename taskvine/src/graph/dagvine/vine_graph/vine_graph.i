/* SWIG interface for local dagvine graph API bindings */
%module vine_graph_capi

%{
#include "int_sizes.h"
#include "vine_graph.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

%include "vine_graph.h"
