/* SWIG interface for local dagvine graph bindings */
%module cdagvine

%{
#include "int_sizes.h"
#include "vine_task_graph.h"
#include "vine_task_node.h"  /* expose outfile type enum to SWIG */
%}

%include "stdint.i"
%include "int_sizes.h"

/* Import existing SWIG interface for type information (do not wrap again) */
%import "../../bindings/python3/taskvine.i"

/* Expose only the dagvine task graph APIs */
%ignore vine_task_node_checkpoint_outfile;  /* avoid exporting unimplemented/optional symbol */
%include "vine_task_node.h"    /* export vine_task_node_outfile_type_t values */
%include "vine_task_graph.h"


