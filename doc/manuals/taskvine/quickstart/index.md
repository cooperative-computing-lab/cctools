## Quickstart

Installing via `conda` is the easiest method for most users.
First, [Install Miniforge](https://github.com/conda-forge/miniforge#install) if you don't already have `conda` installed.
Then, open a terminal and install `ndcctools` like this:

```
conda install -c conda-forge ndcctools
```

Using a text editor, create a manager program called `quickstart.py` like this:

```python
# quickstart.py

import ndcctools.taskvine as vine

# Create a new manager
m = vine.Manager([9123, 9129])
print(f"Listening on port {m.port}")

# Declare a common input file to be shared by multiple tasks.
f = m.declare_url("https://www.gutenberg.org/cache/epub/2600/pg2600.txt", cache="workflow")

# Submit several tasks using that file.
print("Submitting tasks...")
for keyword in ['needle', 'house', 'water']:
    task = vine.Task(f"grep {keyword} warandpeace.txt | wc")
    task.add_input(f, "warandpeace.txt")
    task.set_cores(1)
    m.submit(task)

# As they complete, display the results:
print("Waiting for tasks to complete...")
while not m.empty():
    task = m.wait(5)
    if task:
        print(f"Task {task.id} completed with result {task.output}")

print("All tasks done.")
```

Run the manager program at the command line like this:

```
python quickstart.py
```

It should display output like this:
```
Listening on port 9123
Submitting tasks...
Waiting for tasks to complete...
```

The manager is now waiting for workers to connect and begin
requesting work. (Without any workers, nothing will happen.) You can start one worker on the same machine by opening a new shell and running:

```
vine_worker localhost 9123
```

The manager will send tasks to the worker for execution.  As they complete, you will see output like this:
```
Task 1 completed with result      12     139     824
Task 3 completed with result      99    1199    6672
Task 2 completed with result     536    6314   36667
All tasks done.
```

Congratulations! You have now run a simple manager application that runs tasks on one local worker.
To scale up, simply run more workers on a cluster or cloud facility.

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

