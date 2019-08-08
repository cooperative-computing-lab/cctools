# JX Reference Manual⇗

[Makeflow](makeflow.html) | [JX Tutorial](jx-tutorial.html) | [JX Quick
Reference](jx-quick.html) | [JX Full Reference](jx.html)

## Overview

Makeflow allows for workflows to be expressed in pure [JSON](http://json.org)
or in an extended language known as JX which can be evaluated to produce pure
JSON. This document provides the detailed reference for this language. If you
are just getting started, see the [JX Tutorial](jx-tutorial.html) before
reading this reference manual.

## JSON Workflow Representation

### Workflow

A workflow is encoded as a JSON object with the following keys:

` { "rules": [ ... ] "define": { ... } "environment": { ... } "categories": {
... } "default_category": <string> } `

Required: `rules` is an unordered collection of rules comprising the workflow.
Each entry corresponds to a single job to be executed in the workflow,
described below.

Optional: The `categories` object defines the categories of related rules,
described below.

Optional: `default_category` is the name of the category used if none is
specified for a rule. If not provided, the default category is `"default"`.
This string value _SHOULD_ match one of the keys of the ` "categories"`
object. If there is no corresponding category defined, default values will be
filled in.

`"environment"` specifies environment variables to be set when executing all
rules, described below

### Rules

A `Rule` is encoded as a JSON object with the following keys.

` { # To describe a single Unix command: "command" : <string> # To describe a
sub-workflow: "workflow" : <string> "args" : { ... } # Common to both commands
and sub-workflows: "inputs" : [ ... ] "outputs" : [ ... ] "local_job" :
<boolean> "environment" : { ... } "category" : <string> "resources" : { ... }
"allocation" : <string> } `

Required: The rule must either state a `command`, which is a single Unix
program to run, or a `workflow` which names another workflow to be run as a
sub-job.

Optional: `args` gives arguments in JSON format to be passed to a sub-
workflow, which can be used within expressions in that workflow

Required: The `inputs` and `outputs` arrays list the files required and
produced by the command. These arrays must be an accurate description of the
actual behavior of the command, otherwise the workflow cannot operate
correctly. Each entry in the array is described in the Files section below.

Optional: `local_job`, if `true` indicates that the job is best run locally by
the workflow manager, rather than dispatched to a distributed system. This is
a performance hint provided by the end user and not a functional requirement.

Optional: `category` specifies the name of a job category. The name given
should correspond to the key of a category object in the global workflow
object.

Optional: `resources` specifies the specific resource requirements of this
job, described below.

Optional: `allocation` specifies the resource allocation policy:

  * `first` computes the best "first allocation" based on the prior history of jobs in this category. 
  * `max` selects the maximum resources observed by any job in this category. 
  * `error` attempts to execute the job and increases the resources offered if an error results. 

Optional: `environment` specifies environment variables, described below.

### Files

A file is encoded as either a JSON string or a JSON object. If a file is given
simply as a string, then the string gives the name of the file in all
contexts. For example:

`"output.txt" `

If a file is given as an object, then it the object describes the (possibly
different) names of the file in the DAG (workflow) context and the task
context. The file will be renamed as needed when dispatched from the workflow
to the task, and vice versa. For example:

` { "dag_name" : "output.5.txt", "task_name" : "output.txt" } `

### Categories

A `Category` is encoded as a JSON object with the following keys:

`{ "environment" : { ... } "resources" : { ... } } `

The category describes the environment variables and resource consumption all
of rules that share that category name.

### Environments

Environments are encoded as a JSON object where each key/value pair describes
the name and value of a Unix environment variable. An enviroment may be given
at the global workflow level, in a category description, or in an individual
job, where it applies to the corresponding object. If the same environment
variable appears in multiple places, then the job value overrides the category
value, and the category value overrides the global value.

### Resources

Resources are encoded as a JSON object with the following keys:

`{ "cores" : <integer>, "memory" : <integer>, "disk" : <integer>, "wall-time"
: <integer>, "gpus" : <integer> } `

A resource specification may appear in an individual job or in a category
description. `"cores"` and `"gpus"` give the number of CPU cores and GPUs,
respectively, required to execute a rule. `"disk"` gives the disk space
required, in MB. `"memory"` gives the RAM required, in MB.

`"wall-time"` specifies the maximum allowed running time, in seconds.

## JX Expressions

JX is a superset of JSON with additional syntax for dynamically generating
data. The use case in mind when designing JX was writing job descriptions for
a workflow manager accepting JSON input. For workflows with a large number of
rules with similar structure, it is sometimes necessary to write a script in
another language like Python to generate the JSON output. It would be
desirable to have a special-purpose language that can dynamically generate
rules while still bearing a resemblance to JSON, and more importantly, still
being readable. JX is much lighter than a full-featured language like Python
or JavaScript, and is suited to embedding in other systems like a workflow
manager.

Standard JSON syntax is supported, with some additions from Python. JX allows
for expressions using Python's basic operators. These include the usual
arithmetic, comparison, and logical operators (e.g. `+`, `<=`, `and`, etc.).
JX also includes Python's `range()` function for generating sequences of
numbers.

    
    
    "123" + 4
    => Error{"source":"jx_eval","name":"mismatched types","message":"mismatched types for operator","operator":"123"+4,"code":2}
    
    "123" + "4"
    => "1234"
    
    123 + 4
    => 127
    

Evaluation will produce plain JSON types from JX, assuming the evaluation
succeeded. If an error occurs, evaluation stops and an error is returned to
the caller. Error objects can include additional information indicating where
the failure occurred and what the cause was. Details on the syntax and usage
of Errors is given in a following section. If a non-error type is returned,
then evaluation succeeded and the result contains only plain JSON types. JX
expressions are evaluated in a _context_ , which is an object mapping names to
arbitrary JX values. On evaluating a variable, the variable's name is looked
up in the current context. If found, the value from the context is substituted
in place of the variable. If the variable is not found in the context, an
error is returned.

JX supports comments, introduced by the `#` character and continuing for the
rest of the line.

## Unary Operators⇗

### Logical Complement⇗

>

>     not Boolean -> Boolean

>  

Computes the logical NOT of the given boolean. Unlike C, integers may _not_ be
used as truth values.

### Negation⇗

>

>     -A -> A

>  
>

> where A = Integer|Double

Computes the additive inverse of its operand.

### Positive Prefix⇗

>

>     +A -> A

>  
>

> where A = Integer|Double|String

Returns its operand unmodified.

## Binary Operators⇗

For complicated expressions, parentheses may be used as grouping symbols. JX
does not allow tuples. In the absence of parentheses, operators are evaluated
left to right in order of precedence. From highest to lowest precedence,

  * lookup, function call
  * `*`, `%`, `/`
  * `+`, `-`
  * `==`, `!=`, `<`, `<=`, `>`, `>=`
  * `not`
  * `and`
  * `or`

### Lookup⇗

>

>     A[B] -> C

>  
>

> where A,B = Array,Integer or A,B = Object,String

Gets the item at the given index/key in a collection type. A negative index
into an array is taken as the offset from the tail end.

Arrays also support slicing, with the index is given as `N:M` where `N` and
`M` are optional values that evaluate to integers. If either is absent, the
beginning/end of the array is used. This slice syntax is based on Python's.

    
    
    range(10)
    = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    
    range(10)[:3]
    = [0, 1, 2]
    
    range(10)[4:]
    = [4, 5, 6, 7, 8, 9]
    
    range(10)[3:7]
    = [3, 4, 5, 6]
    

### Addition⇗

>

>     A + A -> A

>  
>

> where A = Integer|Double|String|Array

The behaviour of the addition operator depends on the type of its operands.

  * Integer, Double: sum
  * String, Array: concatenation

### Subtraction⇗

>

>     A - A -> A

>  
>

> where A = Integer|Double

Computes the difference of its operands.

### Multiplication⇗

>

>     A * A -> A

>  
>

> where A = Integer|Double

Computes the product of its operands.

### Division⇗

>

>     A / A -> A

>  
>

> where A = Integer|Double

Computes the quotient of its operands. Division by zero is an error.

### Modulo⇗

>

>     A % A -> A

>  
>

> where A = Integer|Double

Computes the modulus of its operands. Division by zero is an error.

### Conjunction⇗

>

>     Boolean and Boolean -> Boolean

>  

Computes the logical conjunction of its operands.

### Disjunction⇗

>

>     Boolean or Boolean -> Boolean

>  

Computes the logical disjunction of its operands.

### Equality⇗

>

>     A == B -> Boolean

>  
>

> where A,B = Null|Boolean|Integer|Double|String|Array

Returns true iff its operands have the same value. All instances of null are
considered to be equal. For arrays and objects, equality is checked
recursively. Note that if x and y are of incompatible types, x == y returns
false.

### Inequality⇗

>

>     A != B -> Boolean

>  
>

> where A,B = Null|Boolean|Integer|Double|String|Array

Returns true iff its operands have different values. All instances of null are
considered to be equal. For arrays and objects, equality is checked
recursively. Note that if x and y are of incompatible types, x != y returns
true.

### Less than⇗

>

>     A < A -> Boolean

>  
>

> where A = Integer|Double|String

The behaviour of the less than operator depends on the type of its arguments.

  * Integer, Double: compares its operands numerically
  * String: compares its operands in lexicographical order (as given by strcmp(3))

### Less than or equal to⇗

>

>     A <= A -> Boolean

>  
>

> where A = Integer|Double|String

The behaviour of the less than or equal to operator depends on the type of its
arguments.

  * Integer, Double: compares its operands numerically
  * String: compares its operands in lexicographical order (as given by strcmp(3))

### Greater than⇗

>

>     A > A -> Boolean

>  
>

> where A = Integer|Double|String

The behaviour of the greater than operator depends on the type of its
arguments.

  * Integer, Double: compares its operands numerically
  * String: compares its operands in lexicographical order (as given by strcmp(3))

### Greater than or equal to⇗

>

>     A >= A -> Boolean

>  
>

> where A = Integer|Double|String and B = Boolean

The behaviour of the greater than or equal to operator depends on the type of
its arguments.

  * Integer, Double: compares its operands numerically
  * String: compares its operands in lexicographical order (as given by strcmp(3))

## Functions⇗

>

>     range(A) -> Array

>     range(A, A[, A]) -> Array

>  
>

> where A = Integer

Returns an array of integers ranging over the given values. This function is a
reimplementation of [Python's range
function](https://docs.python.org/2/library/functions.html#range). range has
two forms,

    
    
    range(stop)
    range(start, stop[, step])
    

The first form returns an array of integers from zero to stop (exclusive).

    
    
    range(10)
    => [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    

The second form returns an array of integers ranging from start (inclusive) to
stop (exclusive).

    
    
    range(3, 7)
    => [3, 4, 5, 6]
    
    range(7, 3)
    => []
    

The second form also allows an optional third parameter, step. If not given,
step defaults to one.

    
    
    range(-1, 10, 2)
    => [-1,1,3,5,7,9]
    
    range(5,0,-1)
    => [5, 4, 3, 2, 1]
    

Calling with step = 0 is an error.

>

>     format(A, ...) -> String

>  
>

> where A = String

Replaces format specifiers in the given string. This function is based on C's
`printf` function and provides the following format specifiers.

  * `%d`
  * `%i`
  * `%e`
  * `%E`
  * `%f`
  * `%F`
  * `%g`
  * `%G`
  * `%s`
  * `%%`

    
    
    format("file%d.txt", 10)
    = "file10.txt"
    
    format("SM%s_%d.sam", "10001", 23)
    = "SM10001_23.sam"
    

This function serves the same purpose as Python's format operator (%). Since
JX does not include tuples, it provides a function to allow formatting with
multiple fields. JX's format function could be written in Python as follows.

    
    
    def format(spec, *args):
        return spec % tuple(args)
    

## Comprehensions⇗

JX supports list comprehensions for expressing repeated structure. An entry in
a list can be postfixed with one or more `for` clauses to evaluate the list
item once for each entry in the specified list. A `for` clause consists of a
variable and a list, along with an optional condition.

    
    
    [x + x for x in ["a", "b", "c"]]
    = ["aa", "bb", "cc"]
    
    [3 * i for i in range(4)]
    = [0, 3, 6, 9]
    
    [i for i in range(10) if i%2 == 0]
    = [0, 2, 4, 6, 8]
    

If multiple clauses are specified, they are evaluated from left to right.

    
    
    [[i, j] for i in range(5) for j in range(4) if (i + j)%2 == 0]
    = [[0, 0], [0, 2], [1, 1], [2, 0], [2, 2], [3, 1]]
    

## Errors⇗

JX has a special type, Error, to indicate some kind of failure or exceptional
condition. If a function or operator is misapplied, jx_eval will return an
error indicating the cause of the failure. Errors are akin to Python's
exceptions, but JX does not allow Errors to be caught; they simply terminate
evaluation and report what happened. Errors do not evaluate in the same way as
other types. The additional information in an error is protected from
evaluation, so calling jx_eval on an Error simply returns a copy. It is
therefore safe to directly include the invalid function/operator in the body
of an error. If a function or operator encounters an error as an argument,
evaluation is aborted and the Error is returned as the result of evaluation.
Thus if an error occurs deeply nested within a structure, the error will be
passed up as the result of the entire evaluation. Errors are defined using the
keyword Error followed by a body enclosed in curly braces.

    
    
    Error{"source":"jx_eval","name":"undefined symbol","message":"undefined symbol","context":{"outfile":"results","infile":"mydata","a":true,"b":false,"f":0.5,"g":3.14159,"x":10,"y":20,"list":[100,200,300],"object":{"house":"home"}},"symbol":c,"code":0}
    

All errors _MUST_ include some special keys with string values.

  * "source": indicates where the error comes from, and the structure of the additional data.
  * "name": the general category of the error, e.g. "syntax error" or "timed out"
  * "message": some human-readable details about the conditions of the error.

Errors from "jx_eval" have some additional keys, described below, to aid in
debugging. Other sources are free to use any structure for their additional
error data.

### JX Errors⇗

Errors produced during evaluation of a JX structure all include some common
keys.

  * "source": "jx_eval"
  * "code": A numeric identifier for the type of error.

The following codes and names are used by jx_eval.

  * 0: "undefined symbol" The given symbol was not present in the evaluation context. This error includes two additional keys: 
    * "symbol": the symbol name being looked up
    * "context": the evaluation context
  * 1: "unsupported operator" The operator cannot be applied to the type given. This error also includes an "operator" key containing the operator that caused the error, as well as its operands.
  * 2: "mismatched types" The binary operator can only be applied to operands of the same type. This error also includes an "operator" key containing the operator that caused the error, as well as its operands.
  * 3: "key not found" Object lookup failed because the given object does not contain the requested key. This error also includes two additional keys: 
    * "key"
    * "object"
  * 4: "range error" Array lookup failed because the requested index is outside the given array's bounds. This error also includes two additional keys: 
    * "index"
    * "array"
  * 5: "arithmetic error" The the operands are outside the given arithmetic operator's range. This error also includes an "operator" key containing the operator that caused the error, as well as its operands.
  * 6: "invalid arguments" The function arguments are not valid for the given function. This error also includes a "function" key containing the function that caused the error, as well as its arguments.

