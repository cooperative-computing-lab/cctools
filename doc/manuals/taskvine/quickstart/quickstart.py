# quickstart.py

import ndcctools.taskvine as vine

# Create a new manager to handle tasks.
manager = vine.Manager(0)
print(f"Listening on port {manager.port}")

# Create a factory to handle workers.
factory = vine.Factory(batch_type="local",manager=manager)
factory.max_workers = 1

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
