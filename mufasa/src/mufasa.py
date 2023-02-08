# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
from DirectoryMonitor import DirectoryMonitor
from Workflow import Workflow
from WorkflowScheduler import WorkflowScheduler
from utils import parse_filename

import time
import os
import re
import sys
import argparse
import logging


def parse_options():
    parser = argparse.ArgumentParser(description='Manage makeflows.')
    parser.add_argument('--input', '-i', nargs=1, required=True, type=str, help='inbox directory for input files')
    parser.add_argument('--output', '-o', nargs=1, required=True, type=str, help='outbox directory for output files from makeflows')
    parser.add_argument('--makeflow', '-m', nargs=1, required=True, type=str, help='directory containing code along with makeflow file')
    parser.add_argument('--error', '-e', nargs=1, required=True, type=str, help='directory to output workflows that resulted in errors')
    return parser.parse_args()

def main():
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)
    args = parse_options()
    INPUT = os.path.abspath(args.input[0])
    OUTPUT = os.path.abspath(args.output[0])
    MAKEFILE = os.path.abspath(args.makeflow[0])
    ERROR_FILE = os.path.abspath(args.error[0])

    EXPECTED_INPUT = "input.tar.gz" # the input file for the makeflow

    # create a workflow by passing the associated makefile and the expected input name
    wf = Workflow(MAKEFILE, EXPECTED_INPUT)

    # create an instance of Mufasa
    scheduler = WorkflowScheduler(OUTPUT, wf, ERROR_FILE)

    # monitor the inbox directory specified at INPUT and push submitted files into the scheduling queue
    dm = DirectoryMonitor(INPUT, scheduler)

    # check if there have been any changes in the directory
    dm.monitor()

    while True:
        # run the scheduling algorithm to start new WMSs
        scheduler.schedule()
        # check for new files
        dm.monitor()

if __name__=="__main__":
    main()
