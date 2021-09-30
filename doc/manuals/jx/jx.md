# JX Workflow Language Reference

## Overview

[Makeflow](../../makeflow) allows for workflows to be expressed in pure [JSON](http://json.org)
or in an extended language known as JX which can be evaluated to produce pure
JSON. This document provides the detailed reference for this language. If you
are just getting started, see the [JX Tutorial](../jx-tutorial) before
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
|[**define**](#defining-values) | no | Defines [expression substitutions](#jx-expressions) that can be used when defining rules, environments, and categories.
|[**environment**](#environments) | no | Defines environment variables to be set when executing all rules.
|[**categories**](#categories)| no  | Rules are grouped into categories. Rules inside a category are run with the same [environment variables values](#environments), and the same resource requirements.
|default_category | no | Name of the category used if none is specified for a rule. <br>If there is no corresponding category defined, default values will be filled in. If not provided, the default category is `"default"`.

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
| inputs    | no | An array of [file specifications](#Files) required by the command or sub-workflow.
| outputs   | no | An array of [file specifications](#Files) produced by the command or sub-workflow.
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


## JX Expressions

JX is a superset of JSON with additional syntax for dynamically generating
data. The use case in mind when designing JX was writing job descriptions for
a workflow manager accepting JSON input.

For workflows with a large number of
rules with similar structure, it is sometimes necessary to write a script in
another language like Python to generate the JSON output. It is
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

```json
"123" + "4"
=> "1234"

123 + 4
=> 127

"123" + 4
=> Error{"source":"jx_eval",
         "name":"mismatched types",
         "message":"mismatched types for operator",
         "operator":"123"+4,
         "code":2}

```


Evaluation will produce plain JSON types from JX, assuming the evaluation
succeeded. If an error occurs, evaluation stops and an error is returned to
the caller. Error objects can include additional information indicating where
the failure occurred and what the cause was. Details on the syntax and usage
of Errors are given [below](#errors).

If a non-error type is returned, then evaluation succeeded and the result
contains only plain JSON types. JX expressions are evaluated in a _context_ ,
which is an object mapping names to arbitrary JX values. On evaluating a
variable, the variable's name is looked up in the current context. If found,
the value from the context is substituted in place of the variable. If the
variable is not found in the context, an error is returned.

!!! note
    JX supports comments, introduced by the `#` character and continuing for
    the rest of the line.

#### Unary Operators

##### Logical Complement

>     not <boolean> -> <boolean>

Computes the logical NOT of the given boolean. Unlike C, integers may _not_ be
used as truth values.


#### Negation

>     -A -> A     where A is <integer> or <double>

Computes the additive inverse of its operand.


#### Positive Prefix

>     +A -> A    where A is <integer>, <double>, or <string>

Returns its operand unmodified.



### Binary Operators

For complicated expressions, parentheses may be used as grouping symbols. JX
does not allow tuples. In the absence of parentheses, operators are evaluated
left to right in order of precedence. From highest to lowest precedence,

| |
|-|
|lookup, function call
|*, %, /
|+, -
|==, !=, <, <=, >, >=
|not
|and
|or


### Lookup

>     A[B] -> C    where A is <array> and B is <integer>,
>                     or A is <object> and B is <string>

Gets the item at the given index/key in a collection type. A negative index
into an array is taken as the offset from the tail end.

Arrays also support slicing, with the index is given as `N:M` where `N` and
`M` are optional values that evaluate to integers. If either is absent, the
beginning/end of the array is used. This slice syntax is based on Python's.

    
```python
range(10)
=> range [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

range(10)[:3]
=> [0, 1, 2]

range(10)[4:]
=> [4, 5, 6, 7, 8, 9]

range(10)[3:7]
=> [3, 4, 5, 6]
```
    
### Concatenation

>     A + A -> A    where A is <string>, or <array>

### Arithmetic operators

>     A + A -> A    (addition)
>     A - A -> A    (subtraction)
>     A * A -> A    (multiplication)
>     A / A -> A    (division)
>     A % A -> A    (modulo)
>     where A is <integer>, <double>

Division and modulo by zero generate an error.


### Logic operators

>     <boolean> and <boolean> -> <boolean>    (conjunction)
>     <boolean> or  <boolean> -> <boolean>    (disjunction)


### Comparison operators

>     A == B -> <boolean>                     (equality)
>     A != B -> <boolean>                     (inequality)
>     C <  C  -> <boolean>                    (less-than)
>     C <= C  -> <boolean>                    (less-than-or-equal)
>     C >  C  -> <boolean>                    (greater-than)
>     C >= C  -> <boolean>                    (greater-than-or-equal)

>     where A and B are any type, 
>     and C is <integer>, <double> or <string>

For equality operators, all instances of *null* are considered to be equal. For
arrays and objects, equality is checked recursively. Note that if `x` and `y`
are of incompatible types, `x == y` returns `false`, and `x != y` return
`true`.

For `<`, `<=`, `>`, and `>=` the behaviour depends on the type of its arguments:

| | |
|-|-|
integer _or_ double | Compares its operands numerically
string              | Compares its operands in lexicographical order (as given by strcmp(3))
|| It is an error to compare a string to an integer or double.



### Functions

#### range

>       range(A) -> <array>
>       range(A, A[, A]) -> <array>
>
>       where A is <integer>

Returns an array of integers ranging over the given values. This function is a
reimplementation of [Python's range
function](https://docs.python.org/2/library/functions.html#range). range has
two forms:

>   range(stop)
>   range(start, stop[, step])
    

The first form returns an array of integers from zero to stop (exclusive).

    
```python    
range(10)
=> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
    
The second form returns an array of integers ranging from start (inclusive) to
stop (exclusive).

    
    
```python    
range(3, 7)
=> [3, 4, 5, 6]
    
range(7, 3)
=> []
```
    
The second form also allows an optional third parameter, step. If not given,
step defaults to one.

```python
range(-1, 10, 2)
=> [-1,1,3,5,7,9]
    
range(5,0,-1)
=> [5, 4, 3, 2, 1]
```
    
Calling with step = 0 is an error.

#### format

>     format(A, ...) -> String
>     where A is <string>


Replaces format specifiers in the given string. This function is based on C's
`printf` function and provides the following conversion specifiers.

| | |
|-|-|
`%%` | the `%` character
`%s` | string
`%d`, `%i` | signed decimal integer
`%e` | scientific notation (mantissa/exponent) using 'e'
`%E` | scientific notation (mantissa/exponent) using 'E'
`%f`,`%F` | decimal floating point
`%g`,`%G` | double argument is converted in the style of `%f` or `%e` (`%F` or `%G`),<br> whichever is more compact.
 

```python
format("file%d.txt", 10)
= "file10.txt"

format("SM%s_%d.sam", "10001", 23)
= "SM10001_23.sam"
```
    

This function serves the same purpose as Python's format operator (%). Since
JX does not include tuples, it provides a function to allow formatting with
multiple fields. JX's format function could be written in Python as follows.

    
    
```python
def format(spec, *args):
    return spec % tuple(args)
```

#### template

>       template(A[,B]) -> String
>       where A = String and B = Object

template() replaces format placeholders in the given string.
This function is based on Python's sting formatting capabilities.
Variable names enclosed in curly braces are looked up in the current context.
Suppose that the ID variable is defined as 10.


```python
template("file{ID}.txt")
= "file10.txt"
```

It's also possible to specify values directly within the template expression.
The optional second argument is an object consisting of name-value pairs to use in addition to the local context.
This allows overriding defined variables and writing complex expressions.
Suppose that the variable N is defined as 48.

```python
template("SM{PLATE}_{ID}.sam", {"PLATE": "10001", "ID": N/2 - 1})
= "SM10001_23.sam"
```

#### len

>       len([1,2,3]) -> Integer

len() returns the length when passed in an array and errors when not an array. This function is
based on Python's own len() function.

```python
len([1,2,3])
= 3
```

#### fetch

>       fetch(A) -> Object
>       where A = URL/path

fetch() retrieves a JX document at the given URL or path.
This document is then parsed into a JX object.

```python
fetch("example.json")
= {"x": 0, "y": "test", "z": 1.0}
```

#### select

>       select([,A], B) -> Array
>       where A = Object and B = Boolean

select() returns an array of objects for which the boolean expression evaluates to true for that object.

```python
select([{"x": 0, "y": "test", "z": 1.0}, {"x": 1, "y": "example", "z": 0.0}], x==1)
= [{"x": 1, "y": "example", "z": 0.0}]
```

#### project

>       project([,A], B) -> Array
>       where A = Object and B = Expression

project() returns an array of objects resulting from evaluating the expression upon the array.

```python
project([{"x": 0, "y": "test", "z": 1.0}, {"x": 1, "y": "example", "z": 0.0}], x)
= [0, 1]
```

#### schema

>       schema(A) -> Object
>       where A = Object

schema() returns the types of each key in the object.

```python
schema({"x": 0, "y": "test", "z": 1.0})
= {"x": "integer", "y": "string", "z": "float"}
```

#### like

>       like(A, B) -> Boolean
>       where A = String (to be matched) and B = String (regex)

like() returns a boolean value representing whether the given string matches the given regex.

```python
like("test", ".es.*")
= true
```

### Comprehensions

JX supports list comprehensions for expressing repeated structure. An entry in
a list can be postfixed with one or more `for` clauses to evaluate the list
item once for each entry in the specified list. A `for` clause consists of a
variable and a list, along with an optional condition.

    
```python
[x + x for x in ["a", "b", "c"]]
= ["aa", "bb", "cc"]

[3 * i for i in range(4)]
= [0, 3, 6, 9]

[i for i in range(10) if i%2 == 0]
= [0, 2, 4, 6, 8]
```

If multiple clauses are specified, they are evaluated from left to right.

    
```python
[[i, j] for i in range(5) for j in range(4) if (i + j)%2 == 0]
= [[0, 0], [0, 2], [1, 1], [2, 0], [2, 2], [3, 1]]
```

### Anaphoric Expressions

JX supports anaphoric expressions via the following syntax
>       A.B()   where A is any type and B is a function
which simply places `A` as the first parameter in `B`.

This gives us the following examples:
```
"%d".format(10)
= 10

[1,2,3,4].len()
= 4

[{"a": 1}, {"a": 2}].project(a)
= [1, 2]

"abc".like("a.+")
= true
```

### Errors

JX has a special type, *Error*, to indicate some kind of failure or exceptional
condition.

If a function or operator is misapplied, jx_eval will return an
error indicating the cause of the failure. Errors are akin to Python's
exceptions, but JX does not allow Errors to be caught; they simply terminate
evaluation and report what happened.

Errors do not evaluate in the same way as other types. The additional
information in an error is protected from evaluation, so calling jx_eval on an
Error simply returns a copy. It is therefore safe to directly include the
invalid function/operator in the body of an error. If a function or operator
encounters an error as an argument, evaluation is aborted and the Error is
returned as the result of evaluation.  Thus if an error occurs deeply nested
within a structure, the error will be passed up as the result of the entire
evaluation.

Errors are defined using the keyword Error followed by a body enclosed in curly
braces.

```json
Error{
  "source": "jx_eval",
  "message": "undefined symbol",
  "context": {
    "outfile": "results",
    "infile": "mydata",
    "a": true,
    "b": false,
    "f": 0.5,
    "g": 3.14159,
    "x": 10,
    "y": 20,
    "list": [
      100,
      200,
      300
    ],
    "object": {
      "house": "home"
    }
  },
  "symbol": my-undefined-variable,
  "code": 0
}
```


All errors _MUST_ include these keys with string values:

| Required error keys | |
|-|-|
source  | Indicates where the error comes from, and the structure of the additional data.
message | Some human-readable details about the conditions of the error.

Errors from "jx_eval" have some additional keys, described below, to aid in
debugging. Other sources are free to use any structure for their additional
error data.

#### JX Errors

The following errors may produced by jx_eval.

|message|description|
|-|-|
undefined symbol     | The given symbol was not present in the evaluation context.
unsupported operator | The operator cannot be applied to the type given.
mismatched types     | The binary operator was applied to operands of incompatible types.
key not found        | Object lookup failed because the given object does not contain the requested key.
range error          | Array lookup failed because the requested index is outside the given array's bounds.
arithmetic error     | The operands are outside the given arithmetic operator's range.
division by zero     | Some arithmetic operation resulted in a division by zero.
invalid arguments    | The function arguments are not valid for the given function.


