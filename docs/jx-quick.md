# JX Workflow Language Quick Reference

[Makeflow](makeflow.html) | [JX Tutorial](jx-tutorial.html) | [JX Quick
Reference](jx-quick.html) | [JX Full Reference](jx.html)

## Workflow

    
    
    {
       # Required elements:
       "rules" : [
           # Array of rules goes here.
       ],
       # Optional elements:
       "define" : {
          "temp" : 32.5,
       },
       "environment" : {
          "PATH" : "/usr/local/bin:/bin",
       },
       "categories" : {
           "simulation" : {
                "resources" : { ... },
                "environment" : { ... },
                "allocation" : "auto" | "max" | "error"
           }
       }
    }
    

|

## Rules

    
    
    {
       # Required elements
    
       "command" : "simulation.py calib.dat >output.txt",
       "inputs" : [ "simulation.py", "calib.dat" ],
       "outputs" : [ "output.txt" ],
    
       # Optional elements
    
       "local_job" : true | false,
       "resources" : {
           "cores":4,
           "memory":8,
           "disk":16,
           "wall-time":3600
       },
       "environment" : { ... }
       "category" : "simulation",
       "allocation" : "first" | "max" | "error"
    }
    

|

## Values

    
    
    "string"
    42
    3.14159
    true | false
    
    [ 1, 2, 3 ]
    
    { "temp" : 32.5,
      "name" : "fred" }
    

|

## Operators

    
    
    a["b"]   func(x)
    * % /
    + -
    == != < <= > >=
    not
    and
    or
    
    expr for x in [1,2,3]
    

|

## Functions

    
    
    format( "str: %s int: %d float: %f",
            "hello", 42, 3.14159 )
    join( array, delim )
    range( start, stop, step )
    ceil( value )
    floor( value )
    basename( path )
    dirname( path )
    escape( string )
    

|

## Examples

    
    
    # Generate an array of 100 files named "output.1.txt", etc...
    [ "output."+x+".txt" for x in range(1,100) ]
    
    # Generate one string containing those 100 file names.
    join( [ "output."+x+".txt" for x in range(1,100) ], " ")
    
    # Generate five jobs that produce output files alpha.txt, beta.txt, ...
    {
      "command" : "simulate.py > "name+".txt",
      "outputs" : [ name+".txt" ],
      "inputs" : "simulate.py",
    } for name in [ "alpha", "beta", "gamma", "delta", "epsilon" ]
    
    

