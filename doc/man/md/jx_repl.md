






















# jx_repl(1)

## NAME
**jx_repl** - interactive command line tool to explore the JX expression language.

## SYNOPSIS
**jx_repl**

## DESCRIPTION

**jx_repl** is an interactive tool to explore the JX expression language.  **jx_repl** will read in a JX expression, evaluate it, print the result, then save both the query and the result to the context.  Results of previous expressions can be referenced via the out_%d` symbol, and their corresponding query via the `in_%d` symbol.  The program will exit on EOF or when the user enters 'quit' or 'exit.

**jx_repl** also reserves certain symbols in the context to act as commands when entered:

- **help** display a help message which lists details on reserved commands.
- **functions** display a list of functions supported by JX.
- **values** display a list of values supported by JX.
- **operators** display a list of operators supported by JX.
- **catalog** fetch the catalog data.
- **exit** exit the program.
- **quit** same as 'exit'.


## EXIT STATUS
Will always return 0

## EXAMPLES

Simply run **jx_repl** with no parameters:

```
% jx_repl
Welcome to the JX Language Explorer.

Type 'help' for help

in_0 :
```

Enter any valid JX expression:

```
% jx_repl
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
% jx_repl
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
% jx_repl
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
% jx_repl
...
in_3  : join(project(out_2, name), ", ")
out_3 : NAME_1, NAME_2, NAME_3, ..., NAME_N
```

Now, fetch the query used to attain that output

```
% jx_repl
...
in_4  : in_4
out_4 : join(project(select(fetch("http://catalog.cse.nd.edu:9097/query.json"),type=="wq_master"),name),", ")
```

Call one of reserved commands

```
% jx_repl
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


## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2022 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

CCTools
