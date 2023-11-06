# JX Workflow Language Quick Reference

[Makeflow](../makeflow/index.md) | [JX Workflow Tutorial](jx-tutorial.md) | [JX Workflow Reference](jx.md)

    
<table>
<tr>
<th colspan="2"> Workflow </th> <th colspan="1"> Rules </th>
<tr>
<td colspan="2">
```json
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
```
</td>
<td>

```json
{
 # Required elements
 "command" : "./sim calib.dat > out",
 "inputs" : [ "sim", "calib.dat" ],
 "outputs" : [ "out" ],

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
 "allocation" : "first"|"max"|"error"
}
```
</td>
</tr>

<tr>
<th> Values </th> <th> Operators </th> <th> Functions </th>
</tr>

<tr>
<td>
```json
"string"

42
3.14159

true | false

[ 1, 2, 3 ]

{ "temp" : 32.5,
  "name" : "fred" }
```
</td>
<td>

```json
a["b"]
func(x)

* % /
+ -

== != < <= > >=

not
and
or

expr for x in [1,2,3]
```
</td>

<td>

```json
    format( "str: %s int: %d float: %f",
            "hello", 42, 3.14159 )
    join( array, delim )
    range( start, stop, step )
    ceil( value )
    floor( value )
    basename( path )
    dirname( path )
    escape( string )
    len( array )
    fetch( URL/path )
    select( array, boolean )
    project( array, expression )
    schema( object )
    like( object, string )
```

</td>
</tr>

<tr>
<th colspan="3"> Examples </th>
</tr>

<tr>
<td colspan="3">

```json
# A hash at the beginning of a line starts a comment.

# Generate an array of 100 files named "output.1.txt", etc...
[ "output."+x+".txt" for x in range(1,100) ]

# Generate one string containing those 100 file names separated by a space.
join( [ "output."+x+".txt" for x in range(1,100) ], " ")

# Generate five jobs that produce output files alpha.txt, beta.txt, ...
{
  "command" : "simulate.py > "name + ".txt",
  "outputs" : [ name + ".txt" ],
  "inputs"  : "simulate.py",
} for name in [ "alpha", "beta", "gamma", "delta", "epsilon" ]
```
</td>
</tr>
</table>
    
    

