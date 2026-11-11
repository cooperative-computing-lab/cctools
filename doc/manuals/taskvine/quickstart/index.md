# TaskVine Quickstart

## Installation

Installing via `conda` is the easiest method for most users.
First, [Install Miniforge](https://github.com/conda-forge/miniforge#install) if you don't already have `conda` installed.
Then, open a terminal and install `ndcctools` like this:

```
conda install -c conda-forge ndcctools
```

Using a text editor, create a program called `quickstart.py` like this:

```python
--8<-- "taskvine/quickstart/quickstart.py"
```

The Factory starts a local worker when the `with` block begins and stops it
when the block ends. It obtains the connection information from the manager,
so the program runs both the manager and its worker.

Run the program at the command line like this:

```
python quickstart.py
```

It should display output like this:
```
Listening on port 9123
Submitting tasks...
Waiting for tasks to complete...
```

The factory starts the worker automatically. The manager sends tasks to the
worker for execution. As they complete, you will see output like this:
```
Task 1 completed with result      12     139     824
Task 3 completed with result      99    1199    6672
Task 2 completed with result     536    6314   36667
All tasks done.
```

Congratulations! You have now run a simple manager application that runs tasks on one local worker.
To scale up, configure the Factory to launch more workers on a cluster or cloud
facility, as described in [Running TaskVine Applications](../running/index.md).

## Example Applications

The following examples show more complex applications and various features of TaskVine:

- [BLAST Example](example-blast.md)
- [Gutenberg Example](example-gutenberg.md)
- [Mosaic Example](example-mosaic.md)
- [Gradient Descent Example](example-gradient-descent.md)
- [Watch Files Example](example-watch.md)
- [Functional Example](example-functional.md)
- [CFD Example](example-cfd.md)
- [ObsPy Example](example-obspy.md)

Read on to learn how to build applications from scratch and run large numbers of workers at scale.

