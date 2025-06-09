import ndcctools.taskvine as vine

seed2png = """
from obspy import read
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.gridspec as gridspec
import matplotlib.pylab as plt
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "filename",
    help="Filename for a MiniSEED file in the cwd, from which we will extract the Z component from and process.",
)
args = parser.parse_args()

# Get filename from command line arguments
filename_full = args.filename

# We read into an ObsPy stream
st = read(filename_full)

fig = plt.figure(figsize=(15, 10))

# Go through all the traces in the stream
for x in st:
    # For this simple example we only want the Z component
    if x.stats.channel[-1] == "Z":
        # Dump the stats of the trace
        print(x.stats)
        # Baseline correction (partial)
        x.detrend("linear")
        x.detrend("demean")
        # Now we start the plotting
        plt.plot(x.times("relative"), x.data, color="purple")
        plt.title(f"{filename_full}")
        plt.ylabel("Counts")
        plt.xlabel(f"Time (s) relative to {x.stats.starttime}")
        plt.tight_layout()
        # Save figure to a file
        plt.savefig(f"{filename_full}_Z_comp_processed.png")
    else:
        sys.stderr.write("No Z comp found")
"""

m = vine.Manager([9123, 9129])
print(f"Listening on port {m.port}")

# Filenames, these will be fetches from the ObsPy examples repo
filenames = [
    "IC.MDJ.2013.043.mseed",
    "IC.MDJ.2017.246.mseed",
    "IU_ULN_2015-07-18T02.mseed",
    "LKBD.MSEED",
    "NKC_PLN_ROHR.HHZ.2018.130.mseed",
    "steim2.mseed",
]

# Our script
seed2png = m.declare_buffer(seed2png, cache="workflow")

# Go through all the names and declare our input and output files
seed_files = []
for f in filenames:
    seed_files.append({
        "filename": f,
        "input_file": m.declare_url(f"https://examples.obspy.org/{f}"),
        "output_file": m.declare_file(f"Output/{f}_Z_comp_processed.png"),
    })

# Go through every seed file to generate the tasks
for sf in seed_files:
    task = vine.Task(f"python ./seed2png.py {sf['filename']}")
    task.add_input(seed2png, "seed2png.py")
    task.add_input(sf["input_file"], sf["filename"])
    task.add_output(sf["output_file"],
                    f"{sf['filename']}_Z_comp_processed.png")
    m.submit(task)

print("Waiting for tasks to complete")
while not m.empty():
    task = m.wait(5)
    if task:
        if task.successful():
            print(f"Task {task.id} {task.command}")
        elif task.completed():
            print(
                f"Task {task.id} completed with an execution error, exit code {task.exit_code}"
            )
        else:
            print(f"Task {task.id} failed with status {task.result}")

print("Task complete")
