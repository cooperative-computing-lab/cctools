import os
import Workflow
from utils import file_hash
from queue import Queue
import multiprocessing
from copy import copy
import psutil
import time
import logging

class WorkflowScheduler:
    def __init__(self, output_dir, workflow, error_dir, total_limits={"memusage": 8000, "cpuusage": 10, "cluster_cpu": 500, "cluster_mem": 80000, "disk": 50000}, workflow_limits={"cpuusage": 1, "memusage": 1000, "disk": 5000, "cluster_cpu": 10, "cluster_mem": 20000}):
        self.output_dir = output_dir
        self.error_dir = error_dir
        self.workflow = workflow
        self.proc_list = []
        self.proc_stats_queue = multiprocessing.Queue()
        self.proc_stat_averages = {"count": 0, "memusage": 0, "cpuusage": 0, "cluster_cpu": 0, "cluster_mem": 0}
        self.queue = []
        self.total_limits = total_limits
        self.current_resources = { key: 0 for key, val in self.total_limits.items() }
        self.default_wf_limits = workflow_limits

    def push(self, event_path):
        self.queue.append((event_path, copy(self.default_wf_limits), 0))

    def run(self, path, resources):
        logging.info(f"running: {os.path.basename(path)}")
        self.proc_list.append((self.workflow.run(path, self.output_dir, self.error_dir, self.proc_stats_queue, resources), resources, path))
        for res, val in resources.items():
            self.current_resources[res] += val
        logging.info(f"current_resources: {self.current_resources}")

    def check_fit(self, resources):
        for res, val in resources.items():
            # if it won't fit
            if self.total_limits[res] - self.current_resources[res] < val:
                return False
        return True

    def schedule(self):
        # current algorithm is FCFS
        while len(self.queue) and self.check_fit(self.default_wf_limits):
            input_file, resources, current_disk = self.queue[0]

            if not self.check_fit(resources):
                break

            input_file, resources, current_disk = self.queue.pop(0)
            self.run(input_file, resources)

        while not self.proc_stats_queue.empty():
            resources, stats = self.proc_stats_queue.get()
            pid = stats["pid"]
            for prc in self.proc_list:
                if prc[0].pid == pid:
                    proc = prc
            proc[0].join()
            # remove blocked resources
            for res, val in resources.items():
                self.current_resources[res] -= val
            self.proc_list.remove(proc)
            logging.info(f"Completed process {proc[0].pid}: {stats}")

            reason = stats["reason"]
            path = proc[2]
            if reason:
                resources[reason] = int(resources[reason]*2.0)
                self.queue.append((path, resources, resources["disk"]))
                logging.info(f"Process ran out of {reason}")
                logging.info(f"Resubmitting with {resources[reason]} of {reason}")

        self.queue = sorted(self.queue, key=lambda x: x[2], reverse=True)


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
        proc_limit = self.mem_limit // self.proc_stat_averages["memusage"]
        
        # based on cluster emmory
        proc_limit = min(proc_limit, self.cluster_mem // self.proc_stat_averages["cluster_mem"])

        # based on cores
        proc_limit = min(proc_limit, self.cluster_cpu // self.proc_stat_averages["cluster_cpu"])

        return proc_limit

