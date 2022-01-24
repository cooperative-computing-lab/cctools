from DirectoryMonitor import DirectoryMonitor
from BasicEventHandler import BasicEventHandler
from Workflow import Workflow
from WorkflowScheduler import WorkflowScheduler
from utils import parse_filename

import time
import os
import re
import sys
import argparse
import logging


def init(path, scheduler):
        for entry in os.scandir(path):
                if entry.is_file():
                        if re.search("-pre-", entry.name):
                            original_name, _, extension = parse_filename(entry.name)
                            new_path = os.path.join(path, original_name + extension)
                            # TODO: Think about if this is the right thing to do
                            os.rename(os.path.join(path, entry.name), new_path)
                            scheduler.push(new_path)
                        elif not re.search("-post-", entry.name):
                            scheduler.push(os.path.join(path, entry.name))

def parse_options():
    parser = argparse.ArgumentParser(description='Manage makeflows.')
    parser.add_argument('--input', '-i', nargs=1, required=True, type=str, help='inbox directory for input files')
    parser.add_argument('--output', '-o', nargs=1, required=True, type=str, help='outbox directory for output files from makeflows')
    parser.add_argument('--makeflow', '-m', nargs=1, required=True, type=str, help='directory containing code along with makeflow file')
    parser.add_argument('--process-limit', '-p', nargs=1, required=False, type=int, help='maximum concurrent running workflows', default=5)
    parser.add_argument('--error', '-e', nargs=1, required=True, type=str, help='directory to output workflows that resulted in errors')
    return parser.parse_args()

def main():
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
    args = parse_options()
    INPUT = os.path.abspath(args.input[0])
    OUTPUT = os.path.abspath(args.output[0])
    MAKEFILE = os.path.abspath(args.makeflow[0])
    PROC_LIMIT = args.process_limit[0]
    ERROR_FILE = os.path.abspath(args.error[0])

    EXPECTED_INPUT = "input.tar.gz" # the input file for the makeflow

    wf = Workflow(MAKEFILE, EXPECTED_INPUT)
    scheduler = WorkflowScheduler(OUTPUT, wf, ERROR_FILE, PROC_LIMIT)
    dm = DirectoryMonitor(INPUT, scheduler)

    # scan the directory
    init(INPUT, scheduler)

    dm.monitor()

    while True:
        scheduler.schedule()
        dm.monitor()

if __name__=="__main__":
        main()
