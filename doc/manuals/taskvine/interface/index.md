## Python Programming Models

When writing a manager in Python, you have access to several types of tasks
that wrap around the standard task abstraction:

### Python Tasks

A `PythonTask` is an extension of a standard task.
It is not defined with a command line to execute,
but with a Python function and its arguments, like this:

=== "Python"
    ```python
    def my_sum(x, y):
        return x+y

    # task to execute x = my_sum(1, 2)
    t = vine.PythonTask(my_sum, 1, 2)
    ```

A PythonTask is handled in the same way as a standard task,
except that its output `t.output` is simply the Python return
value of the function.  If the function should throw an exception,
then the output will be the exception object.

You can examine the result of a PythonTask like this:

=== "Python"
    ```
    while not m.empty():
        t = m.wait(5)
        if t:
            x = t.output
            if isinstance(x, Exception):
                print("Exception: {}".format(x))
            else:
                print("Result: {}".format(x))
    ```

A `PythonTask` is derived from `Task` and so all other methods for
controlling scheduling, managing resources, and setting performance options
all apply to `PythonTask` as well.

When running a Python function remotely, it is assumed that the Python interpreter
and libraries available at the worker correspond to the appropriate python environment for the task.
If this is not the case, an environment file can be provided with t.set_environment:

=== "Python"
    ```python
    t = vine.PythonTask(my_sum, 1, 2)
    t.set_environment("my-env.tar.gz")
    ```

The file `my-env.tar.gz` is a
[conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)
environment created with [conda-pack](https://conda.github.io/conda-pack/).  A
minimal environment can be created a follows:

```sh
conda create -y -p my-env python=3.8 cloudpickle conda
conda install -y -p my-env -c conda-forge conda-pack
# conda install -y -p my-env pip and conda install other modules, etc.
conda run -p my-env conda-pack
```

#### Using SciUnit to Discover Python Dependencies

If you're unsure which libraries your python tasks require, you can use
**[SciUnit](https://github.com/depaul-dice/sciunit/wiki)** to detect them
automatically:

**Example: Generating a `requirements.txt` with SciUnit**

To generate the `requirements.txt` file, use SciUnit to capture dependencies:
```sh
sciunit exec python <python_script>
# SciUnit returns an EID (Execution ID)
sciunit export <eid>
# SciUnit creates requirements.txt in the current directory
```

### Serverless Computing

TaskVine offers a serverless computing model which is
especially appropriate for invoking functions that have
a relatively short execution time (10s or less) and have
substantial startup time due to large numbers of libraries
or dependent data.

In this model, you first define and install a `LibraryTask`
that defines a function, and then invoke `FunctionCall` tasks
that invoke the library by name.

Suppose your main program has two functions `my_sum` and `my_mul`.
Invoke `create_library_from_functions` to package up these
function definitions into a library task `libtask`

=== "Python"
    ```python
    def my_sum(x, y):
        return x+y

    def my_mul(x, y):
        return x*y

    libtask = m.create_library_from_functions("my_library", my_sum, my_mul)
    ```

We strongly recommend to specify the modules the function needs inside the function itself. This ensures that the correct modules and their aliases will be available when the functions are executed in isolation at the worker:

You can certainly embed `import` statements within the function and install any necessary packages:

=== "Python"
    ```python
    def divide(dividend, divisor):
        import math
        return dividend / math.sqrt(divisor)

    libtask = m.create_library_from_functions("my_library", divide)
    ```

If the overhead of importing modules per function is noticeable, modules can be optionally imported as a common preamble to the function executions. Common modules can be specified with the `hoisting_modules` argument to `create_library_from_functions`. This reduces the overhead by eliminating redundant imports:


=== "Python"
    ```python
    import numpy
    import math

    hoisting_modules = [numpy, math]
    ```

`hoisting_modules` only accepts modules as arguments (e.g. it can't be used to import functions, or select particular names with `from ... import ...` statements. Such statements should be made inside functions after specifying the modules with `hoisting_modules`.

=== "Python"
    ```python
    def cube(x):
        # whenever using FromImport statments, put them inside of functions
        from random import uniform
        from time import sleep as time_sleep

        random_delay = uniform(0.00001, 0.0001)
        time_sleep(random_delay)

        return math.pow(x, 3)
    ```


After installing the packages and functions, you can optionally specify the number of functions the library can run concurrently by setting the number of function slots.  (If unset, TaskVine will assume the library can run one function per core available.)

=== "Python"
    ```python
    libtask.set_function_slots(4)   # maximum 4 concurrent functions
    ```

Once complete, the library task must be `installed` in the system:

=== "Python"
    ```python
    m.install_library(t)
    ```

This causes the library task to be dispatched and started at
available workers, where it remains running.  Immediately after
installing the library, you may submit `FunctionCall` tasks
that invoke the library and functions by name:

=== "Python"
    ```python
    t = vine.FunctionCall("my_library", "my_mul", 20, 30);
    t.set_cores(1)
    t.set_memory(100)
    t.set_disk(100)
    m.submit(t)
    ```

The function invocation will be dispatched to available workers,
and when it is returned, the result is present as `t.output`:

=== "Python"
    ```python
    t = m.wait(5)
    if t:
        print(t.output)
    ```

Note that both library tasks and function invocations consume
resources at the worker, and the number of running tasks will be
constrained by the available resources in the same way as normal tasks.

### Stateful Serverless Computing
A function typically sets up its states (e.g., load modules/packages, build internal models or states) before executing its computation. With advanced serverless computing in TaskVine, you can set up a shared state between function invocations so the cost of setting up states doesn't have to be paid for every invocation, but instead is paid once and shared many times. TaskVine supports this technique as demonstrated via the below example.

Assume that you program has two functions `my_sum` and `my_mul`, and they both use `base` to set up a common value in their computations.

=== "Python"
    ```python
    def base(x, y=1):
        return x**y

    A = 2
    B = 3

    def my_sum(x, y):
        base_val = base(A, B)
        return base_val + x+y

    def my_mul(x, y):
        base_val = base(A, B)
        return base_val + x*y
    ```

With this setup, `base(A, B)` has to be called repeatedly for every function invocation of `my_sum` and `my_mul`. What you want instead is to have the value of `base(A, B)` created and computed once and stored in a library. `my_sum` and `my_mul` thus only have to load such value, instead of computing the value, from a library's state, as follows.

=== "Python"
    ```python
    from ndcctools.taskvine.utils import load_variable_from_library
    def base(x, y=1):
        return {'base_val': x**y}

    A = 2
    B = 3

    def my_sum(x, y):
        base_val = load_variable_from_library('base_val')
        return base_val + x+y

    def my_mul(x, y):
        base_val = load_variable_from_library('base_val')
        return base_val + x*y

    libtask = m.create_library_from_functions("my_library", my_sum, my_mul, library_context_info=[base, [A], {'y': B})
    m.install(libtask)
    # application continues as usual with submitting FunctionCalls and waiting for results.
    ...
    ```

This technique enables maximum sharing between invocations of functions that share some common states, and between invocations of the same function in a library. This is especially helpful in ML/AI workloads where one has to build an ML/AI model on a remote node to best configure it against the remote node's local resources (e.g., GPU). Thus, instead of loading and creating a model for every invocation:

=== "Python"
    ```python
    def infer(image):
        # load model parameters
        ...
        # build model
        model = tf.ResNet50(...)
        # load model in GPU
        model.to_gpu(1)
        # execute an inference
        return model.infer(image)
    ```

One can do this to have the model created and loaded in a GPU once and separate the model creation from the actual inference:

=== "Python"
    ```python
    from ndcctools.taskvine.utils import load_variable_from_library
    def model_setup():
        # load model parameters
        ...
        # build model
        model = tf.ResNet50(...)
        # load model in GPU
        model.to_gpu(1)
        return {'model': model}

    def infer(image):
        model = load_variable_from_library('model')
        # execute an inference
        return model.infer(image)

    libtask = m.create_library_from_functions('infer_library',
                                              infer,
                                              library_context_info=[model_setup, [], {})
    m.install(libtask)

    # application continues as usual with submitting FunctionCalls and waiting for results.
    ...
    ```



### Futures

TaskVine provides a futures executor model which is a subclass
of Python's concurrent futures executor. A function along with its
arguments are submitted to the executor to be executed. A future is
returned whose value will be resolved at some later point.

To create a future, a `FuturesExecutor` object must first be created. Tasks can
then be submitted through the `submit` function. This will return
a Future object. The result of the task can retrieved by calling `future.result()`

=== "Python"
    ```python
    import ndcctools.taskvine as vine

    def my_sum(x, y):
        return x + y

    m = vine.FuturesExecutor(manager_name='my_manager')

    a = m.submit(my_sum, 3, 4)
    b = m.submit(my_sum, 5, 2)
    c = m.submit(my_sum, a, b)  # note that the futures a and b are
                                # passed as any other argument.

    print(c.result())
    ```

If the tasks need to be configured in some way, for example to specify maximum
resources allowed, the method `future_task` returns a `FuturePythonTask` that
can be tailored as any other task:


=== "Python"
    ```python
    import ndcctools.taskvine as vine

    def my_sum(x, y):
        return x + y

    m = vine.FuturesExecutor(manager_name='my_manager')

    t = m.future_task(my_sum, 3, 4)
    t.set_cores(1)

    f = m.submit(t)

    print(f.result())
    ```

Additionally, the executor can use the VineFactory to submit TaskVine workers.
Specifications for the workers can be provided via the `opts` keyword argument when creating to executor.

=== "Python"
    ```python
    import ndcctools.taskvine as vine

    def my_sum(x, y):
        return x + y

    opts = {"memory": 8000, "disk":8000, "cores":8, "min-workers": 5}
    m = vine.FuturesExecutor(manager_name='my_manager', batch_type="condor", opts=opts)

    t = m.future_task(my_sum, 3, 4)
    t.set_cores(1)

    f = m.submit(t)

    print(f.result())
    ```

Instead of tasks, the futures may also executed using [function calls](#serverless-computing) with the `future_funcall` method:

=== "Python"
    ```python
    import ndcctools.taskvine as vine

    def my_sum(x, y):
        return x + y

    m = vine.FuturesExecutor(manager_name='my_manager')

    libtask = m.create_library_from_functions('test-library', my_sum)
    m.install_library(libtask)

    t = m.future_funcall('test-library', 'my_sum', 7, 4)

    a = m.submit(t)

    print(a.result())
    ```



### Functional Abstractions

The TaskVine **map* abstraction works similar to python map, as it applies a
a function to every element in a list. This function works by taking in a chunk_size,
which is the size of an iterable to send to a worker. The worker than maps the given
function over the iterable and returns it. All the results are then combined from the
workers and returned. The size of the chunk depends on the cost of the function.
If the function is very cheap, then sending a larger chunk_size is better. If the
function is expensive, then smaller is better. If an invalid operation happens,
the error will appear in the results.

```python
def fn(a):
    return a*a

m.map(fn, arry, chunk_size)
```

The TaskVine **pair** function computes all the pairs of 2 sequences, and then uses
them as inputs of a given function. The pairs are generated locally using itertools,
and then based on the given chunk_size, are sent out to a worker as an iterable of pairs.
The given function must accept an iterable, as the pair will be sent to the function as
a tuple. The worker will then return the results, and each result from each worker will be
combined locally. Again, cheaper functions work better with larger chunk_sizes,
more expensive functions work better with smaller ones. Errors will be placed in results.

```python
def fn(pair):
    return pair[0] * pair[1]

m.pair(fn, seq1, seq2, chunk_size)
```

The **treeReduce** function combines an array using a given function by
breaking up the array into chunk_sized chunks, computing the results, and returning
the results to a new array. It then does the same process on the new array until there
only one element left and then returns it. The given function must accept an iterable,
and must be an associative function, or else the same result cannot be guaranteed for
different chunk sizes. Again, cheaper functions work better with larger chunk_sizes,
more expensive functions work better with smaller ones. Errors will be placed in results.
Also, the minimum chunk size is 2, as going 1 element at time would not reduce the array

```python
def fn(seq):
    return max(seq)

m.treeReduce(fn, arry, chunk_size)
```

Below is an example of all three abstractions, and their expected output:

```python
# abstractions.py

import ndcctools.taskvine as vine

def main():
    # Set up queue
    q = vine.Manager(port=9123)

    # map - similar to Python's own map function, but uses a taskvine worker
    # to complete computation. Returns sequence with the results from the given function
    # [result] = q.map(func, sequence)
    # Example: (returns [1, 4, 9, 16])
    results = q.map(lambda x: x*x, [1, 2, 3, 4])
    print(results)

    # pair - similar to map function, but uses the function for every pair between
    # the two sequences. Returns sequence of results of each pair.
    # [result] = q.pair(func, sequence1, sequence2)
    # Example: (returns [1, 2, 3, 4, 2, 4, 6, 8, 3, 6, 9, 12, 4, 8, 12, 16])
    results = q.pair(lambda x, y: x*y, [1, 2, 3, 4], [1, 2, 3, 4])
    print(results)

    # tree_reduce - combines pairs of values using a given function, and then returns
    # to a single final number after reducing the sequence.
    # result = q.tree_reduce(func, sequence)
    # Example (even): (returns 24)
    results = q.tree_reduce(lambda x, y: x*y, [1, 2, 3, 4])
    print(results)

    # Example (odd): (returns 120)
    results = q.tree_reduce(lambda x, y: x*y, [1, 2, 3, 4, 5])
    print(results)


if __name__ == "__main__":
    main()
```

Run:
```
python abstractions.py
```

Expected output:
```
Map: [2, 4, 6, 8]
Pair: [2, 4, 6, 8, 4, 8, 12, 16, 6, 12, 18, 24, 8, 18, 24, 32]
Tree: 8
```

