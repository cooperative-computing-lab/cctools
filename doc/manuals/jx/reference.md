# JX Expression Language Reference

## Overview

The JX Expression language is an extension of the JSON data description language.
It combines familiar expression operators, function calls,
external data, and ordinary JSON contents to yield a powerful
data querying and manipulation language.  JX is used throughout
the CCTools to manage and query unstructured data.

Standard JSON syntax is supported, with some additions inspired by Python. JX allows
for expressions using Python's basic operators. These include the usual
arithmetic, comparison, and logical operators (e.g. `+`, `<=`, `and`, etc.).
A selection of functions is provided for common logical and string operations.

## Evaluation Model

```
json-result = evaluate( jx-expression, json-context )
```

A JX expression is evaluated relative to a *context*, which is a JSON document.
Variable names encountered in the JX expression are bound to their corresponding
values given by the context document.  The result of the evaluation is a JSON document.
A JX expression consisting only of constants is a valid JSON document,
and when evaluated, will produce the same JSON document.

## Constants and Constructors

JX permits the same atomic constant values as in JSON, including boolean values:

>     true
>     false

Decimal integers:

>     0
>     123
>     09631

Floating point numbers:

>     3.141592654

Strings:

>     "hello\nworld"

Constructors for lists:

>     [ 10, 9, 8 ]

And constructors for dictionaries:

>     { "name": "Fred", "age": 47, "temp": 98.6 }

## Symbols

An unquoted name is a symbol, and evaluates to the corresponding value found in the evaluation context.
If the context is this:

>     { "city": "South Bend", "zipcodes": [ 46601, 46613, 46614, 46615, 46616, 46617, 46619 ] }

Then this expression:

>     { "location": city, "count": len(zipcodes) }

Will evaluate to this expression:

>     { "location": "South Bend", "count": 7 }

## Comments

Comments are introduced by the `#` character and continue to the next newline:

>     10+20    # This is a comment.

## Operators

JX supports the following binary and unary operators, which have the
usual arithmetic and logical meanings. In the absence of parentheses,
operators are evaluated left to right in order of precedence.
From highest to lowest precedence:

| Operator | Symbol |
|---|---|
| Negation |-|
| Lookup, Function Call | [] , () |
| Multiply, Divide, Remainder | *, /, % |
| Add, Subtract | +, - |
| Comparison | ==, !=, <, <=, >, >= |
| Boolean-Not | not |
| Boolean-And | and |
| Boolean-Or  | or  |




### Negation Prefix

>     -A -> A     where A is <integer> or <double>

Computes the additive inverse of its operand.


### Positive Prefix

>     +A -> A    where A is <integer>, <double>, or <string>

Returns its operand unmodified.

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

### Arithmetic Operators

>     A + A -> A    (addition)
>     A - A -> A    (subtraction)
>     A * A -> A    (multiplication)
>     A / A -> A    (division)
>     A % A -> A    (modulo)
>     where A is <integer>, <double>

Division and modulo by zero generate an error.


### Boolean Logical Operators

>     <boolean> and <boolean> -> <boolean>    (conjunction)
>     <boolean> or  <boolean> -> <boolean>    (disjunction)
>     not <boolean> -> <boolean>              (complement)


### Comparison Operators

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
|---|---|
| integer _or_ double | Compares its operands numerically |
| string              | Compares its operands in lexicographical order (as given by strcmp(3)) |
|| It is an error to compare a string to an integer or double. |


## Functions

The following functions are built into JX.
Future implementations of the language may provide additional
functions or permit external function definitions.

### range

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

### format

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

### template

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

### len

>       len([1,2,3]) -> Integer

len() returns the length when passed in an array and errors when not an array. This function is
based on Python's own len() function.

```python
len([1,2,3])
= 3
```

### listdir

>       listdir(D) -> Array 
>       where D = local directory

listdir() retries the list of file names within a given
directory name in the local filesystem.

*Note:** This is an "external" function that accesses data
outside of the expression environment.  It can be enabled or
disabled in the C API via jx_eval_enable_external().

```python
listdir("data")
= [ "words.txt", "index.txt" ]
```

### fetch

>       fetch(A) -> Object
>       where A = URL/path

fetch() retrieves a JX document at the given URL or path.
This document is then parsed into a JX object.

**Note:** This is an "external" function that accesses data
outside of the expression environment.  It can be enabled or
disabled in the C API via jx_eval_enable_external(). 

```python
fetch("example.json")
= {"x": 0, "y": "test", "z": 1.0}
```

### where / select

>       where([,A], B) -> Array
>       select([,A], B) -> Array
>       where A = Object and B = Boolean

where() returns an array of objects for which the boolean expression evaluates to true for that object.  select() is a synonym for where().

```python
where([{"x": 0, "y": "test", "z": 1.0}, {"x": 1, "y": "example", "z": 0.0}], x==1)
= [{"x": 1, "y": "example", "z": 0.0}]
```

### project

>       project([,A], B) -> Array
>       where A = Object and B = Expression

project() returns an array of objects resulting from evaluating the expression upon the array.

```python
project([{"x": 0, "y": "test", "z": 1.0}, {"x": 1, "y": "example", "z": 0.0}], x)
= [0, 1]
```

### schema

>       schema(A) -> Object
>       where A = Object

schema() returns the types of each key in the object.

```python
schema({"x": 0, "y": "test", "z": 1.0})
= {"x": "integer", "y": "string", "z": "float"}
```

### like

>       like(A, B) -> Boolean
>       where A = String (to be matched) and B = String (regex)

like() returns a boolean value representing whether the given string matches the given regex.

```python
like("test", ".es.*")
= true
```

## List Comprehensions

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

## Functions as Chained Methods

JX supports a syntax for functions very similar to object methods of the form `object.method()`.

The syntax is constructed as follows:
>       A.B()   where A is any type and B is a function
In this example, A gets piped into B as the first argument.  As such, `A.B()` is logically equivalent to `B(A)`.

The same logic applies for functions with multiple parameters as well.
In these cases, the expression before the `.` simply gets inserted as
the first argument, and the others get shifted over.

This gives us the following examples:
```
[1,2,3,4].len()
= 4

"abc".like("a.+")
= true

"ceil(%f) -> %d".format(9.1, 10)
= ceil(9.1) -> 10

[{"a": 1}, {"a": 2}].select(a>0).project(a).len()
= 2
```

## Errors

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

### Error Types

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


