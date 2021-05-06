# JX_REPL: a tool for JX Language exploration

## Overview
This is a brief guide on how to use the ***jx_repl*** command-line utility,
which acts as an explorer for the [JX workflow language](jx.md).  It's recommended
that you follow the [JX Tutorial](jx-tutorial.md) first or simultaneously when using this
tool.

## Usage
To use this interactive utility, simply run
```bash
$ jx_repl
```

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
|catalog | fetch the catalog data |
|exit | exit the program |
|quit | same as "exit" |

## Examples
Start the utility
```bash
$ jx_repl
Welcome to the JX Language Explorer.

Type 'help' for help

in_0  >>> ...
```

Enter any valid JX expression:

```bash
$ jx_repl
...
in_0  >>> [ "file" + x + ".txt" for x in range(3) ]
out_0 <<<
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
in_1  >>> catalog
out_1 <<<
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
in_2  >>> select(type=="wq_master", out_1)
out_2 <<<
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
in_4  >>> join(project(name, out_2), ", ")
out_4 <<< NAME_1, NAME_2, NAME_3, ..., NAME_N
```

Now, fetch the query used to attain that output

```
$ jx_repl
...
in_5  >>> in_4
out_5 <<< join(project(name,select(type=="wq_master",fetch("http://catalog.cse.nd.edu:9097/query.json"))),", ")
```

Call one of reserved commands

```
% jx_repl
...
in_7  >>> help

  help          display this message
  functions     display a list of functions supported by the JX language
  values        display a list of values supported by the JX language
  operators     display a list of operators supported by the JX language
  in_#          the #'th input query
  out_#         result of in_#
  catalog       alias to fetch catalog data
  quit|exit     exit program

```
