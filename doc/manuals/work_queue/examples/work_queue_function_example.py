# Quick Start Example of Work Queue with Python Functions

# Import the Work Queue library.
import work_queue as wq

# Define a function to invoke remotely.
def my_sum(x, y):
    import math
    return x+y

# Create a new queue, listening on port 9123:
queue = wq.WorkQueue(9123)
print("listening on port {}".format(queue.port))

# Submit several tasks for execution:
print("submitting tasks...")
for value in range(1,100):
    task = wq.PythonTask(my_sum, value, value)
    task.specify_cores(1)
    queue.submit(task)

# As they complete, display the results:
print("waiting for tasks to complete...")
while not queue.empty():
    task = queue.wait(5)
    if task:
        print("task {} completed with result {}".format(task.id,task.output))

print("all done.")
