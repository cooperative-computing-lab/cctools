# quickstart.py

import ndcctools.taskvine as vine

# Create a new manager to handle tasks.
m = vine.Manager(0)
print(f"Listening on port {m.port}")

# Declare a common input file to be shared by multiple tasks.
book = m.declare_url("https://www.gutenberg.org/cache/epub/2600/pg2600.txt", cache="workflow")

# Start a factory that provides one local worker for this workflow.
with vine.Factory(manager=m, cores=1, min_workers=1, max_workers=1) as factory:
    # Submit several tasks using that file.
    print("Submitting tasks...")
    for keyword in ["needle", "house", "water"]:
        task = vine.Task(f"grep {keyword} warandpeace.txt | wc")
        task.add_input(book, "warandpeace.txt")
        task.set_cores(1)
        m.submit(task)

    # As they complete, display the results.
    print("Waiting for tasks to complete...")
    while not m.empty():
        task = m.wait(5)
        if task:
            print(f"Task {task.id} completed with result {task.output}")

print("All tasks done.")
