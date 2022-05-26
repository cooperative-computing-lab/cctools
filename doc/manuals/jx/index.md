# JX (JSON Extended) Introduction

JX is an extension of the JSON data description language.
It combines familiar expression operators, function calls,
external data, and ordinary JSON contents to yield a powerful
data querying and manipulation language.  JX is used throughout
the CCTools to manage and query unstructured data.

For example, given this JSON data context:

```
{ "temp_c": 17, "city": "Minneapolis", "state": "MN"}
```

You can write this JX expression:
```
{ "temp_f" : (temp_c/0.5556)+32, "location": city + ", " + state }
```

Yielding this result:
```
{ "temp_f" : 62.57553956834532, "location": "Minneapolis, MN" }
```

And you can read about the full details here:
- [JX REPL Tool](repl)
- [JX Language Reference](reference)

JX is used as the basic language underlying these tools and systems:
- [JX Workflow Language](../jx-workflow)
- [Catalog Server](../catalog)
- [DeltaDB Time Series Database](../man_pages/deltadb_query)
