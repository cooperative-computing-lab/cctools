# Tutorial: JX Workflow Language

This is a gentle introduction to the [JX workflow language](jx.md), which is the
"advanced" language used by the [Makeflow](../../makeflow) workflow engine. JX
is an extension of standard [JSON](http://json.org) expressions, so if you are
familiar with those from another language, you will find it easy to get
started.

To use these examples, you will first need to install the
[Makeflow](../makeflow) workflow engine and ensure that the `makeflow`
command is in your `PATH`.

## Hello World

Let's begin by creating a simple workflow that executes exactly one task,
which outputs the text "hello world". Enter the following workflow using your
favorite text editor and save it into a file called `hello-world.jx`:

[hello-world.jx](hello-world.jx)

```json
{
    "rules": [
                {
                    "command" : "/bin/echo hello world > output.txt",
                    "outputs" : [ "output.txt" ],
                    "inputs"  : [ ]
                }
            ]
}
```

Now run it locally with the following command:

```sh
$ makeflow --jx hello-world.jx
```

You should see some output like this:

```sh
parsing hello-world.jx...
local resources: 4 cores, 7764 MB memory, 2097151 MB disk
max running local jobs: 4
checking hello-world.jx for consistency...
hello-world.jx has 1 rules.
starting workflow....
submitting job: echo hello world > output.txt
submitted job 27758
job 27758 completed
nothing left to do.
```

Now examine the file `output.txt`:

```sh
cat output.txt
```

and you should see that it contains "hello world". Congratulations, you have run your first workflow!


## Defining Values

JX allows you to programmatically define elements of your workflow, using
expressions to substitute in for parts of jobs. The general structure of a
workflow is this:

```json
{
    "define": {
        # symbol definitions go here
    },
    "rules": [
        # rules go here
    ]
}
```

Building on the previous example, suppose that you want to parameterize the
message constant called `message` To do that, define it in the `define`
section, and then concatentate `message` into the job, like this:

[define-hello.jx](define-hello.jx)
```json
{ 
    "define":{
                "message" : "hello world!"
             },
    "rules": [
                {
                    "command": "/bin/echo " +message+ " > output-from-define.txt",
                    "outputs": [ "output-from-define.txt" ],
                    "inputs":  [ ],
                }
             ]
}
```

```sh
$ makeflow --jx define-hello.jx

parsing define-hello.jx...
local resources: 4 cores, 7764 MB memory, 2097151 MB disk
max running local jobs: 4
checking define-hello.jx for consistency...
define-hello.jx has 1 rules.
starting workflow....
submitting job: /bin/echo hello world! > output-from-define.txt
submitted job 1376
job 1376 completed

$ cat output-from-define.txt
hello world!
```


## Generating Multiple Jobs

A common use of workflows is to drive a large number of simulation or analysis
codes. Suppose that you have a custom simulation code called `simulate.py`
which takes a command line argument `--parameter` and produces its output on the
console. To run one instance of that simulator, you could do this:

[simulate.py](simulate.py)

[simulate-once.jx](simulate-once.jx)
```json
{
    "rules": [
                {
                    "command" : "python ./simulate.py --parameter 1 > output.txt",
                    "inputs"  : [ "simulate.py" ],
                    "outputs" : [ "output.txt" ]
                }
             ]
}
```

(Note that the simulator code itself is treated as an input file, so that the
code can be copied to the target execution machine as needed.)

If you wanted to run three simulations with slightly different arguments, you
could simply write each one out longhand, giving each one a different command
line argument and sending output to a different file:

[simulate-many-long-form.jx](simulate-many-long-form.jx)
```json
{
    "rules": [
                {
                    "command" : "python ./simulate.py --parameter 1 > output.1.txt",
                    "inputs"  : [ "simulate.py" ],
                    "outputs" : [ "output.1.txt" ]
                },
                {
                    "command" : "python ./simulate.py --parameter 2 > output.2.txt",
                    "inputs"  : [ "simulate.py" ],
                    "outputs" : [ "output.2.txt" ]
                },
                {
                    "command" : "python ./simulate.py --parameter 3 > output.3.txt",
                    "inputs"  : [ "simulate.py" ],
                    "outputs" : [ "output.3.txt" ]
                }
             ]
}
```


But of
course that would be tiresome for a large number of jobs. Instead, you can
write out the job once and use the `for` operator (sometimes known as a _list
comprehension_ ) to generate multiple instance of the job:

[simulate-many-concat.jx](simulate-many-concat.jx)
```json
{
    "rules": [
                {
                    "command" : "python ./simulate.py --parameter " + N + " > output." + N + ".txt",
                    "inputs"  : [ "simulate.py" ],
                    "outputs" : [ "output." + N + ".txt" ]
                } for N in [1, 2, 3]
             ]
}
```

Note that the value of `N` is substituted into both the commands
string and the output list by using the plus sign to indicate string
concatenation.  If you prefer a more compact style, you can
use the `template()` function to insert values into strings
into places indicate by curly braces:
 
```
{
    "rules": [
       {
         "command" : template("./simulate.py -n {N} > output.{N}.txt")
          "inputs" : [ "simulate.py" ],
          "outputs" : [ "output."+N+".txt" ],
       } for N in [ 1, 2, 3 ]
```

If you want to preview how
these list comprehensions expand into individual jobs, use the program `jx2json` to reduce the JX program into plain JSON:

```sh
jx2json --pretty simulate-many-concat.jx
```

Which should produce output like this:

```json
{
  "rules":
    [
      {
        "command":"python ./simulate.py --parameter 1 > output.1.txt",
        "inputs":
          [
            "simulate.py"
          ],
        "outputs":
          [
            "output.1.txt"
          ]
      },
      
      {
        "command":"python ./simulate.py --parameter 2 > output.2.txt",
        ...
```


## Gather Results

So far, out example workflow will run three simulations independently. But
suppose you want the workflow to have a final step which runs after all the
simulations are complete, to collect the results in a single file called
`output.all.txt`.

You _could_ write the rule out longhand for three files explicitly:

```json
{
    "command" : "/bin/cat output.1.txt output.2.txt output.3.txt > output.all.txt",
    "inputs"  : [ "output.1.txt", "output.2.txt", "output.3.txt" ],
    "outputs" : [ "output.all.txt" ]
}
```

Of course, it would be better to generate the list
automatically. The list of output files is easy using a list comprehension:

```json
[ "output." + N + ".txt" for N in [1,2,3] ]
```

evaluates to

```json
["output.1.txt","output.2.txt","output.3.txt"]
```

!!! note
    You can corroborate this with: `echo '[ "output." + N + ".txt" for N in [1,2,3] ]' | jx2json`

The command line string takes more thought, because we want a string containing all of those filenames,
rather than the array. The `join()` function is used to join an array into a
single string.

For example, the expression:

```json
join(["output.1.txt","output.2.txt","output.3.txt"], " ")
```

evaluates to:

```json
"output.1.txt output.2.txt output.3.txt"
```

We _could_ put all of those bits into a single rule, like this:

```json
{
    "command" : "/bin/cat " + join([ "output." + N + ".txt" for N in [1,2,3]]) + " > output.all.txt",
    "inputs"  : [ "output." + N + ".txt" ] for N in [ 1, 2, 3 ] ],
    "outputs" : [ "output.all.txt" ]
}
```

That is correct, but it's rather hard to read. Instead, we can make things
clearer by factoring out the definition of the list and the range to the
`define` section of the workflow. Putting it all together, we have this:

[gather.jx](gather.jx)
```json
{
    "define" : {
        "RANGE"    : range(1,4),
        "FILELIST" : [ "output." + N + ".txt" for N in RANGE ],
    },

    "rules" : [
                {
                    "command" : "python ./simulate.py --parameter " + N + " > output."+N+".txt",
                    "inputs"  : [ "simulate.py" ],
                    "outputs" : [ "output." + N + ".txt" ]
                } for N in RANGE,
                {
                    "command" : "/bin/cat " + join(FILELIST," ") + " > output.all.txt",
                    "inputs"  : FILELIST,
                    "outputs" : [ "output.all.txt" ]
                }
              ]
}
```

## Computational Resources

JX allows you to specify the number of cores, and the memory and disk sizes a
rule requires. To this end, rules are grouped into **categories**. Rules in the
same category are expected to use the same quantity of resources. Following
with our example, we have two natural categories, rules that perform a
simulation, and a rule that collects the results:

[gather-with-categories.jx](gather-with-categories.jx)
```json
{
    "define" : {
        "RANGE"    : range(1,4),
        "FILELIST" : [ "output." + N + ".txt" for N in RANGE ],
    },

    "categories" : {
        "simulate" : {
                        "resources" : { "cores" : 4, "memory" : 512, "disk" : 1024 }
        },
        "collect"  : {
                        "resources" : { "cores" : 1, "memory" : 512, "disk" : 8192 }
        }
    },

    "rules" : [
                {
                    "command"  : "python ./simulate.py --parameter " + N + " > output."+N+".txt",
                    "inputs"   : [ "simulate.py" ],
                    "outputs"  : [ "output." + N + ".txt" ],
                    "category" : "simulate"
                } for N in RANGE,
                {
                    "command"  : "/bin/cat " + join(FILELIST," ") + " > output.all.txt",
                    "inputs"   : FILELIST,
                    "outputs"  : [ "output.all.txt" ],
                    "category" : "collect"
                }
              ]
}
```

In the previous example, the category names `simulate` and `collect` are
arbitrary names. Also,both **memory** and **disk** are specified in megabytes
(MB). Note that we both defined `categories` and labeled each rule with its
`category`.  All rules not explicitly labeled with a category belong to the
`default` category.

The resource specifications are used in two ways:

  * To describe the batch jobs used to run a rule. Thus, `makeflow` is able to request the batch system for appropiate resources. 
  * When makeflow is run using resource monitoring (`--monitor=...`), if the resource usage of a rule exceeds the resources declared, it is terminated and marked as failed rule.

When the resources used by a rule are not known, we recommend to set the
resource specification to the largest resources available (e.g., the largest
size possible for a batch job), and add to the category definition the key-
value `"allocation" : "auto"`. As measurements become available, makeflow
computes efficient resource allocations to maximize throughput. If a rule
fails because the computed allocation is too small, it is retried once using
the maximum resources specified. With this scheme, even when some rules are
retried, overall throughput is increased in most cases.


## Nested workflows

JX currently offers support for nesting workflows. When declaring a
nested workflow, the nested workflow itself is treated as any other rule, and
its rules are ran **locally** to the execution site. This means that any
initial inputs and final outputs of the nested workflow should be explictely
specified in the main workflow.

In the following example, note how we need to declare the inputs and outputs to
the nested workflows as if they were a regular rule. Also note how `my_var` is
set from the main workflow, and used inside the nested one:

`FILE: main.jx`
```json
{
    "rules": [
        {
            "command": "echo hello world > my-common-input",
            "outputs": [ "my-common-input" ]
        },
        {
            "workflow": "nested.jx",
            "args": {"my_var": N},
            "inputs": [ "my-common-input" ],
            "outputs": [ "output." + N ]
        } for N in range(5)
    ]
}
```

`FILE: nested.jx`
```json
{
    "rules": [
        {
            "command": format("cp my-common-input output.%d", my_var),
            "outputs": [ "output." + my_var ]
        }
    ]
}
```

