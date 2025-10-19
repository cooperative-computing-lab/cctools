/* SWIG interface for local dagvine graph bindings */
%module cdagvine

%{
#include "int_sizes.h"
#include "strategic_orchestration_node.h"
#include "strategic_orchestration_graph.h"
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

%include "strategic_orchestration_node.h"
%include "strategic_orchestration_graph.h"


