# jx_repl: A Tool for JX Language Exploration

## Overview
This is a brief guide on how to use the ***jx_repl*** command-line utility,
which acts as an explorer for the [JX workflow language](jx.md).  It's recommended
that you follow the [JX Tutorial](jx-tutorial.md) first or simultaneously when using this
tool.

## Usage
To use this interactive utility, simply run
>       $ jx_repl

## Commands
***jx_repl*** reserves a number of symbols in the context, which act as commands when entered:

| Command | Description |
|:-------:|:------------|
|help | display a help message which lists details on reserved commands |
|in_# | return the #'th input query |
|out_# | return the result of in_# |
|functions | display a list of functions supported by the JX language |
|values | display a list of values supported by the JX language |
|operators | display a list of operators supported by the JX language |
|catalog | fetch the catalog data from http://catalog.cse.nd.edu:9097 |
|exit | exit the program |
|quit | same as "exit" |

## Examples
Start the utility
```bash
$ jx_repl
Welcome to the JX Language Explorer.

Type 'help' for help

in_0  : ...
```

Enter any valid JX expression:

```bash
$ jx_repl
...
in_0  : [ "file" + x + ".txt" for x in range(3) ]
out_0 :
[
  "file0.txt",
  "file1.txt",
  "file2.txt"
]
```

Fetch the catalog data:

```
$ jx_repl
...
in_1  : catalog
out_1 :
[
  {
    "name":NAME,
    "lastheardfrom":1620308581,
    "address":ADDRESS,
    ...
    "type":"wq_factory"
  },
  ...
]
```

Perform operations on previous output

```
$ jx_repl
...
in_2  : select(out_1, type=="wq_master")
out_2 :
[

  {
    "name":NAME,
    "lastheardfrom":1620308755,
    "address":ADDRESS,
    "tasks_total_disk":0,
    "tasks_total_memory":0,
    ...
    "type": wq_master
  }
  ...
]
```

Do more operations on previous output

```
$ jx_repl
...
in_3  : join(project(out_2, name), ", ")
out_3 : NAME_1, NAME_2, NAME_3, ..., NAME_N
```

Now, fetch the query used to attain that output

```
$ jx_repl
...
in_4  : in_4
out_4 : join(project(select(fetch("http://catalog.cse.nd.edu:9097/query.json"),type=="wq_master"),name),", ")
```

Call one of reserved commands

```text
$ jx_repl
...
in_5  : help

  help          display this message
  functions     display a list of functions supported by the JX language
  values        display a list of values supported by the JX language
  operators     display a list of operators supported by the JX language
  in_#          the #'th input query
  out_#         result of in_#
  catalog       alias to fetch catalog data
  quit|exit     exit program

```
