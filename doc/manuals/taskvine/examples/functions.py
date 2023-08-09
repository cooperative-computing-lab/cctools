# Quick start example of taskvine with python functions

import ndcctools.taskvine as vine

# Define a function to invoke remotely.
def my_sum(x, y):
    import math
    return x+y

# Create a new manager, listening on port 9123:
m = vine.Manager(9123)
print(f"listening on port {m.port}")

# Submit several tasks for execution:
print("submitting tasks...")
for value in range(1,100):
    task = vine.PythonTask(my_sum, value, value)
    task.set_cores(1)
    m.submit(task)

# As they complete, display the results:
print("waiting for tasks to complete...")
while not m.empty():
    task = m.wait(5)
    if task:
        print("task {} completed with result {}".format(task.id,task.output))

print("all done.")
