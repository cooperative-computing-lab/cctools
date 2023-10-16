# The JX Workflow Language

The JX Workflow Language is a language for expressing workflows
that allows for easy manipulations to the structure and
partitioning of a workflow.  It makes use of the [JX expression language](../jx/index.md)
to create concise and expressive structures.

For example, you can specify 100 batch jobs compactly like this:
```
   {
      "command" : "python ./simulate.py --parameter " + N + " > output." + N + ".txt",
      "inputs"  : [ "simulate.py" ],
      "outputs" : [ "output." + N + ".txt" ]
   } for N in range(1,100)
```

## Getting Started

  * [JX Workflow Tutorial](jx-tutorial.md)

## Quick Reference

  * [JX Workflow Quick Reference](jx-quick.md)
  
## Complete Reference

  * [JX Workflow Reference](jx.md)


