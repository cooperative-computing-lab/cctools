/* SWIG interface for local executor graph API bindings */
%module graph_capi

%{
#include "int_sizes.h"
#include "graph.h"
#include "executor.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* uint64_t[] + length: not mapped yet; expose via ExecutorGraph when needed. */
%ignore graph_supernode_register;
%ignore graph_supernode_nonleader_members;

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

%include "graph.h"
%include "executor.h"
