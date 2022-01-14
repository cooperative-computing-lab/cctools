from DirectoryMonitor import DirectoryMonitor
from BasicEventHandler import BasicEventHandler
from Workflow import Workflow
from WorkflowEventHandler import WorkflowEventHandler
from utils import parse_filename

from queue import Queue
import work_queue as wq
import time
import os
import re
import sys
import argparse


def handle_event(src_path):
        def wq_task(path):
                f = open(path)
                data = f.read()
                f.close()
                return (path, data)
        print("handling:", event_path)
        task = wq.PythonTask(wq_task, event_path)
        task.specify_input_file("BasicEventHandler.py", cache=True)
        task.specify_cores(1)
        self.wq.submit(task)

def init(path, event_handler):
        for entry in os.scandir(path):
                if entry.is_file():
                        if re.search("-pre-", entry.name):
                            original_name, _, extension = parse_filename(entry.name)
                            new_path = os.path.join(path, original_name + extension)
                            print(new_path)
                            print(entry.name)
                            # TODO: Think about if this is the right thing to do
                            os.rename(os.path.join(path, entry.name), new_path)
                            event_handler.handle(new_path)
                        elif not re.search("-post-", entry.name):
                            event_handler.handle(os.path.join(path, entry.name))

def parse_options():
    parser = argparse.ArgumentParser(description='Manage makeflows.')
    parser.add_argument('--input', '-i', nargs=1, required=True, type=str, help='inbox directory for input files')
    parser.add_argument('--output', '-o', nargs=1, required=True, type=str, help='outbox directory for output files from makeflows')
    parser.add_argument('--makeflow', '-m', nargs=1, required=True, type=str, help='directory containing code along with makeflow file')
    return parser.parse_args()

def main():
        args = parse_options()
        INPUT = args.input[0]
        OUTPUT = args.output[0]
        MAKEFILE = args.makeflow[0]
        EXPECTED_INPUT = "input.tar.gz"
        PROCESS_LIST = []

        # specify  event_handler
        w = Workflow(MAKEFILE, EXPECTED_INPUT)
        event_handler = WorkflowEventHandler(OUTPUT, w, PROCESS_LIST)

        # create a directory monitor
        dm = DirectoryMonitor(INPUT, event_handler)

        # scan the directory
        init(INPUT, event_handler)

        dm.monitor()

        while True:
            for proc in PROCESS_LIST:
                proc.join(timeout=0)
                if not proc.is_alive():
                    print("process done!")
                    PROCESS_LIST.remove(proc)
            dm.monitor()

if __name__=="__main__":
        main()
