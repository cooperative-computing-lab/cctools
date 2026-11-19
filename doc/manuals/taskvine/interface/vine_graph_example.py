# vine_graph_example.py
#
# Describe a small analysis as a graph of Python calls and run it with VineGraph.
# Four tasks make samples, four tasks summarize them, and one task combines the summaries.

import random
import statistics

import ndcctools.taskvine as vine
import ndcctools.taskvine.vine_graph as vg


# Runs at the worker: make one sample of numbers.
def make_sample(seed, size):
    generator = random.Random(seed)
    return [generator.gauss(100, 15) for _ in range(size)]


# Runs at the worker: summarize one sample.
def summarize(sample):
    return {"count": len(sample), "mean": statistics.fmean(sample), "largest": max(sample)}


# Runs at the worker: combine the means of all the samples.
def combine(means):
    return statistics.fmean(means)


# Runs at the worker: compare one number with another.
def difference(value, reference):
    return value - reference


if __name__ == "__main__":
    # Describe the tasks. Nothing runs yet: each add() returns a Node that stands for a result.
    graph = vg.Graph()
    summaries = []
    for seed in range(4):
        sample = graph.add(make_sample, seed, size=100_000)
        summaries.append(graph.add(summarize, sample))

    # A task may read just a part of a result. Here, only the mean of each summary is sent.
    overall = graph.add(combine, [summary["mean"] for summary in summaries])

    # Listen on any available port, and start a factory that provides one local worker.
    with vg.Executor(port=0) as executor:
        print(f"Listening on port {executor.port}")
        with vine.Factory(manager_host_port=f"localhost:{executor.port}", cores=4, min_workers=1, max_workers=1):
            # Run the graph, and bring back the results that we ask for.
            results = executor.run(graph, [overall, summaries[0]])
            print(f"overall mean {results[overall]:.2f}")
            print(f"first sample {results[summaries[0]]['count']} values, largest {results[summaries[0]]['largest']:.2f}")

            # Calls can also be submitted one at a time, while the program decides what to do next.
            # Each submit() returns a Future, and a Future given as an argument makes the new task
            # wait for that result. Only the last result is brought back.
            sample = executor.submit(make_sample, 99, size=100_000)
            summary = executor.submit(summarize, sample)
            shift = executor.submit(difference, summary["mean"], results[overall])
            print(f"a new sample differs from the overall mean by {shift.result():+.3f}")
