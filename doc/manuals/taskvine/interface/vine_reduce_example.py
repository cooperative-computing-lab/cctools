# vine_reduce_example.py
#
# Count the words in four books from Project Gutenberg with VineReduce.
# Each author is a dataset, each book is a file, and each line is an entry.

import collections
import glob
import itertools
import os
import re
import urllib.request

import ndcctools.taskvine as vine
from vine_reduce import TaskVineDistributor, VineReduce, serialization

books = {
    "tolstoy": [2600, 1399],  # War and Peace, Anna Karenina
    "austen": [1342, 158],  # Pride and Prejudice, Emma
}


def download(number):
    path = os.path.abspath(f"pg{number}.txt")
    if not os.path.exists(path):
        url = f"https://www.gutenberg.org/cache/epub/{number}/pg{number}.txt"
        urllib.request.urlretrieve(url, path)
    return path


def count_lines(path):
    with open(path, encoding="utf-8") as f:
        return sum(1 for line in f)


# Runs at the worker: reads the lines covered by one chunk.
def read_lines(chunk, dataset_metadata, distributor_metadata=None):
    with open(chunk.url, encoding="utf-8") as f:
        return list(itertools.islice(f, chunk.start, chunk.stop))


# Runs at the worker: the processor, applied to the lines of one chunk.
def count_words(lines):
    words = collections.Counter()
    for line in lines:
        words.update(re.findall(r"[a-z]+", line.lower()))
    return words


if __name__ == "__main__":
    # Each dataset lists its files, and the number of entries in each file.
    datasets = {}
    for author, numbers in books.items():
        files = {}
        for number in numbers:
            path = download(number)
            files[path] = count_lines(path)
        datasets[author] = {"metadata": {}, "files": files}

    # Listen on any available port. The factory finds it by itself.
    distributor = TaskVineDistributor(
        port=0,
        resources_processor={"cores": 1},
        resources_reducer={"cores": 1},
    )

    # Start one local worker with four cores.
    workers = vine.Factory(manager=distributor.manager)
    workers.cores = 4
    workers.min_workers = 1
    workers.max_workers = 1

    vr = VineReduce(
        processors={"words": count_words},
        input=datasets,
        chunk_to_args=read_lines,
        chunksize=5000,
        results_dir="results",
        distributor=distributor,
    )

    with workers, distributor:
        vr.compute()

    for author in datasets:
        (path,) = glob.glob(f"results/{author}/words/*.pkl.zst")
        words = serialization.load(path)
        print(author, {w: words[w] for w in ["needle", "house", "water"]})
