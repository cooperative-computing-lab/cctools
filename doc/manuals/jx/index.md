![](../logos/jx-logo.png)

# JX Expression Language - Overview

The JX Expression Language is an extension of the JSON data description language.
It combines familiar expression operators, function calls,
external data, and ordinary JSON contents to yield a powerful
data querying and manipulation language.  JX is used throughout
the CCTools to manage and query unstructured data.

For example, JX expressions can be used to describe jobs in a workflow:
```
{
    "command" : "collect.exe"
    "inputs" :  [ "input."+i+".txt" ]
    "outputs" : [ "output."+i+".txt" ]
} for i in range(1,100)

```

Or to write LINQ-style queries on remote data:

```
fetch(url).select(type=="wq_master").select(tasks_submitted>100).project([name,tasks_running+tasks_waiting])
```

Read about the full details here:

- [JX Expression Language Reference](reference.md)
- [JX REPL Tool](repl.md)

JX is used as the basic language underlying these tools and systems:

- [JX Workflow Language](../jx-workflow/index.md)
- [Catalog Server Queries](../catalog/index.md)
- [DeltaDB Time Series Database](../man_pages/deltadb_query.md)
