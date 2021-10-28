include(manual.h)dnl
HEADER(jx_repl)

SECTION(NAME)
BOLD(jx_repl) - interactive command line tool to explore the JX language.

SECTION(SYNOPSIS)
CODE(jx_repl)

SECTION(DESCRIPTION)

BOLD(jx_repl) is an interactive tool to explore the JX language.  BOLD(jx_repl) will read in a JX expression, evaluate it, print the result, then save both the query and the result to the context.  Results of previous expressions can be referenced via the `out_%d` symbol, and their corresponding query via the `in_%d` symbol.  The program will exit on EOF or when the user enters 'quit' or 'exit'.
PARA
BOLD(jx_repl) also reserves certain symbols in the context to act as commands when entered:

LIST_ITEM(BOLD(help) display a help message which lists details on reserved commands.)
LIST_ITEM(BOLD(functions) display a list of functions supported by the JX language.)
LIST_ITEM(BOLD(values) display a list of values supported by the JX language.)
LIST_ITEM(BOLD(operators) display a list of operators supported by the JX language.)
LIST_ITEM(BOLD(catalog) fetch the catalog data.)
LIST_ITEM(BOLD(exit) exit the program.)
LIST_ITEM(BOLD(quit) same as 'exit'.)
LIST_END

SECTION(EXIT STATUS)
Will always return 0

SECTION(EXAMPLES)

Simply run BOLD(jx_repl) with no parameters:

LONGCODE_BEGIN
% jx_repl
Welcome to the JX Language Explorer.

Type 'help' for help

in_0 :
LONGCODE_END

Enter any valid JX expression:

LONGCODE_BEGIN
% jx_repl
...
in_0  : [ "file" + x + ".txt" for x in range(3) ]
out_0 :
[
  "file0.txt",
  "file1.txt",
  "file2.txt"
]
LONGCODE_END

Fetch the catalog data:

LONGCODE_BEGIN
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
LONGCODE_END

Perform operations on previous output

LONGCODE_BEGIN
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
LONGCODE_END

Do more operations on previous output

LONGCODE_BEGIN
% jx_repl
...
in_3  : join(project(out_2, name), ", ")
out_3 : NAME_1, NAME_2, NAME_3, ..., NAME_N
LONGCODE_END

Now, fetch the query used to attain that output

LONGCODE_BEGIN
% jx_repl
...
in_4  : in_4
out_4 : join(project(select(fetch("http://catalog.cse.nd.edu:9097/query.json"),type=="wq_master"),name),", ")
LONGCODE_END

Call one of reserved commands

LONGCODE_BEGIN
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

LONGCODE_END


SECTION(COPYRIGHT)

COPYRIGHT_BOILERPLATE

FOOTER
