# Quick start example of taskvine with python functions
import getpass
import time

import ndcctools.taskvine as vine


# This function is the work each task does. It estimates pi by picking
# random points in a 1x1 square and counting how many land inside a
# quarter circle of radius 1. The fraction inside is about pi/4.
#
# TaskVine sends this function to a worker and runs it there, so any
# module it uses (like random) must be imported inside the function.
def count_inside_circle(seed, samples):
    import random

    # Give each task its own seed so the tasks pick different points.
    rng = random.Random(seed)

    # Each True (point inside the circle) counts as 1 in the sum.
    return sum(
        rng.random() ** 2 + rng.random() ** 2 <= 1
        for _ in range(samples)
    )


def main():
    task_count = 16              # how many tasks to split the work into
    samples_per_task = 500_000   # how many random points each task tests
    total_samples = task_count * samples_per_task

    # Create the manager, which hands out tasks and collects results.
    # port=0 lets the operating system pick any free port, so it never
    # collides with another program. The project name lets workers find
    # this manager by name; including your username keeps it unique.
    # Using "with" shuts the manager down cleanly when the block ends.
    with vine.Manager(
        port=0,
        name=f"taskvine-pi-{getpass.getuser()}",
    ) as manager:
        print(f"Listening on port {manager.port} as {manager.name}")

        # Describe the tasks. A PythonTask is one call of a Python
        # function with the given arguments. Nothing runs yet; these are
        # just descriptions of the work to be done.
        tasks = [
            vine.PythonTask(count_inside_circle, seed, samples_per_task)
            for seed in range(task_count)
        ]

        # Start the clock so we can see how long the distributed run takes.
        start = time.perf_counter()

        # A factory starts worker processes for us. "local" runs them on
        # this machine; on a cluster you would use "condor" or "slurm".
        # Each worker gets one core, and at most four run at once.
        workers = vine.Factory("local", manager=manager)
        workers.min_workers = 1
        workers.max_workers = 4
        workers.cores = 1

        # The workers start when this block begins and are stopped when
        # it ends, so they don't keep running after the program finishes.
        with workers:
            # Hand every task to the manager. The manager sends them to
            # workers as workers become available.
            for task in tasks:
                manager.submit(task)

            total_hits = 0

            # Keep collecting results until no tasks are left.
            while not manager.empty():
                # Wait up to 5 seconds for any task to finish. If none
                # finishes in that time, wait returns None and we try again.
                task = manager.wait(5)
                if task is None:
                    continue

                # Stop with an error if a task failed.
                if not task.successful():
                    raise RuntimeError(
                        f"Task {task.id} failed: {task.result_string}"
                    )

                # For a PythonTask, task.output is the function's return
                # value, which here is the number of points inside the circle.
                total_hits += task.output
                print(f"Task {task.id} counted {task.output:,} points inside")

        # Combine the results: the fraction of points inside the circle
        # is about pi/4, so multiply by 4 to estimate pi.
        elapsed = time.perf_counter() - start
        estimate = 4 * total_hits / total_samples
        print(f"pi is approximately {estimate:.6f} ({total_samples:,} samples)")
        print(f"Completed in {elapsed:.1f} seconds")


# Run main() only when this file is run directly as a program.
if __name__ == "__main__":
    main()
