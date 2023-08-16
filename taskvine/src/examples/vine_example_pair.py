# This example program shows the use of the pair()
# abstraction to generate all pairs of several values.

import ndcctools.taskvine as vine
import sys

def make_name(namepair):
    return namepair[0] + " " + namepair[1]

firsts = ["Joe", "Sarah", "Mark", "Lewis", "Jane", "James", "Abby", "Kate",
        "Sean", "William", "Emma", "Miles", "Grace", "Cole", "Robert"]

lasts = ["Smith", "Johnson", "Thomas", "Long", "Jackson", "Knoddington",
        "Riley", "Shirley", "Donaldson", "Madden", "Tyler", "Morales",
        "McKinsey", "Perez", "Redford"]

if __name__ == "__main__":
    m = vine.Manager()
    print("listening on port", m.port)

    print("pairing first and last names...")
    result = m.pair(make_name, firsts, lasts, chunksize=3)

    try:
        print("\n".join(result))
    except:
        # some error in execution...
        pass

# vim: set sts=4 sw=4 ts=4 expandtab ft=python:
