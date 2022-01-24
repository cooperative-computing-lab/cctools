import os
import Workflow
from utils import file_hash
from queue import Queue
import multiprocessing

import logging

class WorkflowScheduler:
    def __init__(self, output_dir, workflow, error_dir, proc_limit=5, memory_limit=8000):
        self.output_dir = output_dir
        self.error_dir = error_dir
        self.workflow = workflow
        self.proc_limit = proc_limit
        self.proc_list = []
        self.proc_stats_queue = multiprocessing.Queue()
        self.proc_stat_averages = {"count": 0, "memusage": 0, "cpuusage": 0}
        self.queue = Queue()
        self.mem_limit = memory_limit

    def push(self, event_path):
        self.queue.put(event_path)

    def run(self, path):
        input_file = self.__process_file(path)
        logging.info(f"running: {os.path.basename(input_file)}")
        self.proc_list.append(self.workflow.run(input_file, self.output_dir, self.error_dir, self.proc_stats_queue))

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
        self.proc_limit = self.__compute_process_limit()

        while len(self.proc_list) < self.proc_limit and not self.queue.empty():
            self.run(self.queue.get())

        for proc in self.proc_list:
            proc.join(timeout=0)
            if not proc.is_alive():
                self.proc_list.remove(proc)
                stats = self.proc_stats_queue.get()
                self.__update_averages(stats)
                logging.info(f"Completed process {proc.pid}: {stats}")
                logging.info(f"Current workflow limit: {self.proc_limit}")


    def __update_averages(self, stats):
        self.proc_stat_averages["count"] += 1
        count = self.proc_stat_averages["count"]

        for key, val in self.proc_stat_averages.items():
            if key == "count":
                continue
            self.proc_stat_averages[key] = (val*(count-1) + stats[key]) / count

    def __compute_process_limit(self):
        if not self.proc_stat_averages["count"]:
            return 1

        # compute based on memory usage
        return self.mem_limit // self.proc_stat_averages["memusage"]

