# Tutorial: JX Workflow Language

[Makeflow](makeflow.html) | [JX Tutorial](jx-tutorial.html) | [JX Quick
Reference](jx-quick.html) | [JX Full Reference](jx.html)

This is a gentle introduction to the JX workflow language, which is the
"advanced" language used by the [Makeflow](makeflow.html) workflow engine. JX
is an extension of standard [JSON](http://json.org) expressions, so if you are
familiar with those from another language, you will find it easy to get
started.

To use these examples, you will first need to install the
[Makeflow](makeflow.html) workflow engine and ensure that the `makeflow`
command is in your `PATH`.

## Hello World

Let's begin by creating a simple workflow that executes exactly one task,
which outputs the text "hello world". Enter the following workflow using your
favorite text editor and save it into a file called `example.jx`:

`{ "rules": [ { "command" : "echo hello world > output.txt", "outputs" : [
"output.txt" ], "inputs" : [ ], } ] } `

Now run it locally with the following command:

`makeflow --jx example.jx `

You should see some output like this:

`parsing example.jx... local resources: 4 cores, 15774 MB memory, 2097151 MB
disk max running local jobs: 4 checking example.jx for consistency...
example.jx has 1 rules. starting workflow.... submitting job: echo hello world
> output.txt submitted job 29955 job 29955 completed nothing left to do. `

Now examine the file `output.txt` and you should see that it contains "hello
world". Congratulations, you have run your first workflow!

## Defining Values

JX allows you to programmatically define elements of your workflow, using
expressions to substitute in for parts of jobs. The general structure of a
workflow is this:

`{ "define": { # symbol definitions go here }, "rules": [ # rules go here ] }
`

Building on the previous example, suppose that you want to parameterize the
message constant called `message` To do that, define it in the `define`
section, and then concatentate `message` into the job, like this:

`{ **"define": { "message" : "hello world!" },** "rules": [ { "command" :
"echo " **+message+** " > output.txt", "outputs" : [ "output.txt" ], "inputs"
: [ ], } ] } ` To test out this change, first clean the workflow: `makeflow
--jx example.jx --clean ` And then run it again: `makeflow --jx example.jx `
(For the remainder of the examples in this tutorial, we will assume that you
clean the workflow first, and then run it again.)

## Generating Multiple Jobs

A common use of workflows is to drive a large number of simulation or analysis
codes. Suppose that you have a custom simulation code called `simulate.py`
which takes a command line argument `-n` and produces its output on the
console. To run one instance of that simulator, you could do this:

`{ "rules": [ { "command" : "./simulate.py -n 1 > output.txt", "inputs" : [
"simulate.py" ], "outputs" : [ "output.txt" ], } ] } `

(Note that the simulator code itself is treated as an input file, so that the
code can be copied to the target execution machine as needed.)

If you wanted to run three simulations with slightly different arguments, you
could simply write each one out longhand, giving each one a different command
line argument and sending output to a different file:

`{ "rules": [ { "command" : "./simulate.py -n 1 > output.1.txt", "inputs" : [
"simulate.py" ], "outputs" : [ "output.1.txt" ], }, { "command" :
"./simulate.py -n 2 > output.2.txt", "inputs" : [ "simulate.py" ], "outputs" :
[ "output.2.txt" ], }, { "command" : "./simulate.py -n 3 > output.3.txt",
"inputs" : [ "simulate.py" ], "outputs" : [ "output.3.txt" ], } ] } ` But of
course that would be tiresome for a large number of jobs. Instead, you can
write out the job once and use the `for` operator (sometimes known as a _list
comprehension_ ) to generate multiple instance of the job: `{ "rules": [ {
"command" : "./simulate.py -n " **+N+** " > output." **+N+** ".txt", "inputs"
: [ "simulate.py" ], "outputs" : [ "output." **+N+** ".txt" ], } **for N in [
1, 2, 3 ]** ] } ` Note that the value of ` N` is substituted into both the
commands string and the output list by using the plus sign to indicate string
concatenation. If you prefer a more `printf` style, you can use the `format()`
function to insert values into strings into places indicate by percent codes:
`{ "rules": [ { "command" : **format("./simulate.py -n %d >
output.%d.txt",N,N)** "inputs" : [ "simulate.py" ], "outputs" : [
"output."+N+".txt" ], } for N in [ 1, 2, 3 ] ] } ` If you want to preview how
these list comprehensions expand into individual jobs, use the program `
jx2json` to reduce the JX program into plain JSON: ` jx2json example.jx `
Which should produce output like this: ` {"rules":[{"command":"./simulate.py
-n 1 >
output.1.txt","inputs":["simulate.py"],"outputs":["output.1.txt"]},{"command":"./simulate.py
-n 2 >
output.2.txt","inputs":["simulate.py"],"outputs":["output.2.txt"]},{"command":"./simulate.py
-n 3 > output.3.txt","inputs":["simulate.py"],"outputs":["output.3.txt"]}]} `

## Gather Results

So far, out example workflow will run three simulations independently. But
suppose you want the workflow to have a final step which runs after all the
simulations are complete, to collect the results in a single file called
`output.all.txt`. You could write it out longhand for three files explicitly:
`{ "command" : "cat output.1.txt output.2.txt output.3.txt > output.all",
"inputs" : [ "output.1.txt", "output.2.txt", "output.3.txt" ], "outputs" : [
"output.all.txt" ], } ` Of course, it would be better to generate the list
automatically. The list of output files is easy: just generate them using a
list comprehension. For example, this expression: `[ "output."+N+".txt" for N
in [1,2,3] ] ` Would evaluate to this array:
`["output.1.txt","output.2.txt","output.3.txt"] ` The command line string is a
little trickier, because we want a string containing all of those filenames,
rather than the array. The `join()` function is used to join an array into a
single string. For example, this expression:
`join(["output.1.txt","output.2.txt","output.3.txt"]," ") ` Evaluates to this
string: `"output.1.txt output.2.txt output.3.txt" ` We can put all of those
bits into a single job, like this: `{ "command" : "cat
"+join(["output."+N+".txt" for N in [1,2,3]])+" >output.all.txt", "inputs" : [
"output."+N+".txt" for N in [ 1, 2, 3 ] ], "outputs" : [ "output.all.txt" ], }
` That is correct, but it's rather hard to read. Instead, we can make things
clearer by factoring out the definition of the list and the range to the
`define` section of the workflow. Putting it all together, we have this: `{
"define": { "RANGE" : range(1,3), "FILELIST" : ["output."+N+".txt for N in
RANGE], }, "rules": [ { "command" : "./simulate.py -n "+N+" >
output."+N+".txt", "inputs" : [ "simulate.py" ], "outputs" : [
"output."+N+".txt" ], } for N in RANGE, { "command" : "cat "+join(FILELIST,"
")+" >output.all.txt", "inputs" : [ FILELIST ], "outputs" : [ "output.all.txt"
], } ] } `

## Computational Resources

JX allows you to specify the number of cores, and the memory and disk sizes a
rule requires. To this end, rules are grouped into _categories_. Rules in the
same category are expected to use the same quantity of resources. Following
with our example, we have twonatural categories, rules that perform a
simulation, and a rule that collects the results: `{ "define": { "RANGE" :
range(1,3), "FILELIST" : ["output."+N+".txt for N in RANGE], }, "categories":
{ "simulate":{ "resources": {"cores":4, "memory":512, "disk":1024} },
"collect":{ "resources": {"cores":1, "memory":512, "disk":8192} } }, "rules":
[ { "command" : "./simulate.py -n "+N+" > output."+N+".txt", "inputs" : [
"simulate.py" ], "outputs" : [ "output."+N+".txt" ], "category" : "simulate" }
for N in RANGE, { "command" : "cat "+join(FILELIST," ")+" >output.all.txt",
"inputs" : [ FILELIST ], "outputs" : [ "output.all.txt" ], "category" :
"collect" } ] } ` In the previous example, the category names ` simulate` and
`collect` are arbitrary. Memory and disk are specified in megabytes (MB). Note
that we both defined `categories` and labeled each rule with its `category`.
All rules not explicitely labeled with a category belong to the `default`
category. The resource specifications are used in two ways:

  * A batch job to run a rule is defined using the resource specification. Thus, `makeflow` is able to request the batch system for appropiate resources. 
  * When makeflow is run using resource monitoring (`--monitor=...`), if the resource usage of a rule exceeds the resources declared, it is terminated and marked as failed rule. 
When the resources used by a rule are not known, we recommend to set the
resource specification to the largest resources available (e.g., the largest
size possible for a batch job), and add to the category definition the key-
value `"allocation" : "auto"`. As measurements become available, makeflow
computes efficient resource allocations to maximize throughput. If a rule
fails because the computed allocation is too small, it is retried once using
the maximum resources specified. With this scheme, even when some rules are
retried, overall throughput is increased in most cases.

