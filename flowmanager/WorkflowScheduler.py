import os
import Workflow
from utils import file_hash
from queue import Queue

import logging

class WorkflowScheduler:
    def __init__(self, output_dir, workflow, error_dir, proc_limit = 5):
        self.output_dir = output_dir
        self.error_dir = error_dir
        self.workflow = workflow
        self.proc_limit = proc_limit
        self.proc_list = []
        self.queue = Queue()

    def push(self, event_path):
        self.queue.put(event_path)

    def run(self, path):
        input_file = self.__process_file(path)
        print("running:", input_file)
        self.proc_list.append(self.workflow.run(input_file, self.output_dir, self.error_dir))

    def __process_file(self, event_path):
        # construct new filename
        filename = os.path.basename(event_path)
        h = file_hash(event_path)
        new_filename = filename + "-pre-" + h
        dirname = os.path.dirname(event_path)
        new_event_path = os.path.join(os.path.dirname(event_path), new_filename)
        # rename the file
        os.rename(event_path, new_event_path)
        return new_event_path


    def schedule(self):
        while len(self.proc_list) < self.proc_limit and not self.queue.empty():
            self.run(self.queue.get())

        for proc in self.proc_list:
            proc.join(timeout=0)
            if not proc.is_alive():
                self.proc_list.remove(proc)
                print("done:", proc.name, "Success" if not proc.exitcode else "Failure")
                if not self.queue.empty():
                    self.run(self.queue.get())
