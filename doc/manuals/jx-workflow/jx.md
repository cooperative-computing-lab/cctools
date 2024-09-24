# JX Workflow Language Reference

## Overview

[Makeflow](../makeflow/index.md) allows for workflows to be expressed in pure [JSON](http://json.org)
or in an extended language known as JX which can be evaluated to produce pure
JSON. This document provides the detailed reference for this language. If you
are just getting started, see the [JX Tutorial](jx-tutorial.md) before
reading this reference manual.

## JSON Workflow Representation

### Workflow

A workflow is encoded as a JSON object with the following keys:

```json
{
    "rules":[ <rule>, <rule>, ... ],

    "define":{ <string>:<string>, <string>:<string>, ... },

    "environment":{ <string>:<string>, <string>:<string>, ... },

    "categories": { <string>:<category>, <string>:<category>, ... },

    "default_category": <string>
}

```

| Key | Required | Description |
|-----|:--------:|-------------|
|[**rules**](#rules)|  yes     | Unordered array of rules comprising the workflow.<br>  Each `<rule>` corresponds to a single job represented as a JSON object to be executed in the workflow.
|[**define**](#computed-values) | no | Defines [expression substitutions](#computed-values) that can be used when defining rules, environments, and categories.|
|[**environment**](#environments) | no | Defines environment variables to be set when executing all rules.|
|[**categories**](#categories)| no  | Rules are grouped into categories. Rules inside a category are run with the same [environment variables values](#environments), and the same resource requirements.|
|default_category | no | Name of the category used if none is specified for a rule. <br>If there is no corresponding category defined, default values will be filled in. If not provided, the default category is `"default"`.|

### Rules

There are two types of rules. The first type describes single commands, and the second describes sub-workflows. 

A rule encoding **a single command** is a JSON object with the following keys.

```json
{
    "command" : <string>,

    "inputs" : [ <file>, <file>, ... ],

    "outputs" : [ <file>, <file>, ... ],
    
    "local_job" : <boolean>,
    
    "environment":{ <string>:<string>, <string>:<string>, ... },
    
    "category" : <string>,
    
    "resources" : <resources>,

    "allocation" : <string>
}
```

A rule specifying **a sub-workflow** is a JSON object similar to the one for a
single command, but it replaces the key `command` with keys `workflow` and
`args`:

```json
{
    "workflow" : <string>,

    "args" : { ... },

    # inputs, outputs, local_job, environment, category, resources, and allocation 
    # as for a single command.

    ...
}
```

| Key | Required | Description |
|-----|:--------:|-------------|
| command <br> _or_ <br> workflow | yes | Either `command`, which is a single Unix program to run, or a `workflow` which names another workflow to be run as a sub-job.
| args      | no | **Only used with workflow key.**  Gives arguments as a JSON array to be passed to a sub-workflow.
| inputs    | no | An array of [file specifications](#files) required by the command or sub-workflow.
| outputs   | no | An array of [file specifications](#files) produced by the command or sub-workflow.
| local_job | no | If `true` indicates that the job is best run locally by the workflow manager, rather than dispatched to a distributed system. This is a performance hint provided by the end user and not a functional requirement. Default is `false`.
| category  | no | Specifies the name of a job category. The name given should correspond to the key of a category object in the global workflow object.
| resources | no | Specifies the specific [resource requirements](#resources) of this job.
| allocation | no | Specifies the resource allocation policy: <ul> <li> `first` computes the best "first allocation" based on the prior history of jobs in this category. <li> `max` selects the maximum resources observed by any job in this category. <li> `error` attempts to execute the job and increases the resources offered if an error results. </ul>
| environment | no | Specifies [environment variables](#environments).


### Files

A file is encoded as either a JSON string or a JSON object. If a file is given
simply as a string, then the string gives the name of the file in all
contexts. For example:

```json
"output.txt"
```

If a file is given as an object, then it the object describes the (possibly
different) names of the file in the workflow context (DAG context) and the task
context. The file will be renamed as needed when dispatched from the workflow
to the task, and vice versa. For example:

```json
{
    "dag_name" : "output.5.txt",
    "task_name" : "output.txt"
}
```


### Categories

A **category** is encoded as a JSON object with the following keys:

```json
{
    "environment": <environment>,

    "resources": <resources>
}
```

The category describes the [environment variables](#environments) and
[resources required](#resources) per rule for all the rules that share that
category name.


### Environments

Environments are encoded as a JSON object where each key/value pair describes
the name and value of a Unix environment variable:

```json
{
    "PATH":"/opt/bin:/bin:/usr/bin",
    "LC_ALL":C.UTF-8"
}
```


An environment may be given at the global workflow level, in a category
description, or in an individual job, where it applies to the corresponding
object. If the same environment variable appears in multiple places, then the
job value overrides the category value, and the category value overrides the
global value.


### Resources

Resources are encoded as a JSON object with the following keys:

```json
{
    "cores" : <integer>,
    "memory" : <integer>,
    "disk" : <integer>,
    "gpus" : <integer>,
    "wall-time": <integer>
}
```

A resource specification may appear in an individual job or in a category
description. `"cores"` and `"gpus"` give the number of CPU cores and GPUs,
respectively, required to execute a rule. `"disk"` gives the disk space
required, in MB. `"memory"` gives the RAM required, in MB.

`"wall-time"` specifies the maximum allowed running time, in seconds.

`"mpi-processes"` specifies the number of MPI processes the job needs.
Currently this is only used when submitting SLURM jobs.

## Computed Values

To simplify the construction of complex workflows, you may also use any
of the expressions and operators found in the [JX Expression Language](../jx/index.md)
to compute JSON values at runtime.


