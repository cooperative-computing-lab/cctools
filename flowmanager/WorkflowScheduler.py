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
    def __init__(self, output_dir, workflow, error_dir, total_limits={"memusage": 8000, "cpuusage": 20, "cluster_cpu": 500, "cluster_mem": 80000, "disk": 75000}, workflow_limits={"cpuusage": 1, "memusage": 250, "disk": 5000, "cluster_cpu": 20, "cluster_mem": 20000}):
        self.output_dir = output_dir
        self.error_dir = error_dir
        self.workflow = workflow
        self.proc_list = []
        self.proc_pipes = []
        self.proc_stat_averages = {"count": 0, "memusage": 0, "cpuusage": 0, "cluster_cpu": 0, "cluster_mem": 0}
        self.queue = []
        self.total_limits = total_limits
        self.current_resources = { key: 0 for key, val in self.total_limits.items() }
        self.current_usage = { key: 0 for key, val in self.total_limits.items() }
        self.default_wf_limits = workflow_limits
        self.resource_types = sorted(self.total_limits.keys())
        self.logfile = open("resources.log", "w+")
        self.logcount = 0
        self.wf_id = 0
        self.logfile.write("time,")
        self.logfile.write("id,")
        self.logfile.write(",".join(self.resource_types))
        allocated_resources = ["allocated_" + rtype for rtype in self.resource_types]
        self.logfile.write("," + ",".join(allocated_resources) + "\n")

    def push(self, event_path):
        self.queue.append((event_path, copy(self.default_wf_limits), 0))

    def run(self, path, resources):
        logging.info(f"running: {os.path.basename(path)}")
        mufasa_conn, workflow_conn = multiprocessing.Pipe()
        self.proc_list.append((self.workflow.run(path, self.output_dir, self.error_dir, workflow_conn, resources), resources, path))
        self.proc_pipes.append((mufasa_conn, self.wf_id))
        self.wf_id += 1
        for res, val in resources.items():
            self.current_resources[res] += val
        logging.info(f"current_resources: {self.current_resources}")

    def check_fit(self, resources):
        for res, val in resources.items():
            # if it won't fit
            if self.total_limits[res] - self.current_resources[res] < val:
                return False
        return True

    def __join_proc(self, pid, resources):
        for prc in self.proc_list:
            if prc[0].pid == pid:
                proc = prc
        proc[0].join()
        # remove blocked resources
        for res, val in resources.items():
            self.current_resources[res] -= val
        self.proc_list.remove(proc)
        return proc

    def __handle_request(self, conn, wf_id):
        request = conn.recv()
        status = request["status"]
        stats = request["workflow_stats"]
        resources = request["allocated_resources"]

        if status == "success" or status == "error":
            proc = self.__join_proc(stats["pid"], resources)
            if status == "success":
                logging.info(f"Completed process {proc[0].pid}: {stats}")
            else:
                logging.info(f"Error with process {proc[0].pid}: {stats}")
            conn.close()
            self.proc_pipes.remove((conn, wf_id))
        elif status == "killed":
            proc = self.__join_proc(stats["pid"], resources)
            reason = stats["reason"]
            path = proc[2]
            resources[reason] = min(int(resources[reason]*1.25), self.total_limits[reason])
            self.queue.append((path, resources, 0))
            logging.info(f"Process killed: {reason}")
            logging.info(f"Resubmitting with {resources[reason]} of {reason}")
            conn.close()
            self.proc_pipes.remove((conn, wf_id))
        elif status == "paused":
            proc = self.__join_proc(stats["pid"], resources)
            reason = stats["reason"]
            path = proc[2]
            resources[reason] = min(int(resources[reason]*1.25), self.total_limits[reason])
            self.current_resources["disk"] += resources["disk"]
            self.queue.append((path, resources, resources["disk"]))
            logging.info(f"Process paused: {reason}")
            logging.info(f"Resubmitting with {resources[reason]} of {reason}")
            conn.close()
            self.proc_pipes.remove((conn, wf_id))
        elif status == "request_pause":
            if self.total_limits["disk"] - self.current_resources["disk"] < self.default_wf_limits["disk"]:
                message = {"response": "kill"}
                conn.send(message)
            else:
                message = {"response": "pause"}
                conn.send(message)
        elif status == "resource_update":
            wf_resources = [str(stats[rtype]) for rtype in self.resource_types]
            for rtype in self.resource_types:
                self.current_usage[rtype] += stats[rtype]
            self.logfile.write(str(self.logcount) + ",")
            self.logfile.write(str(wf_id) + ",")
            self.logfile.write(",".join(wf_resources))
            allocated_resources = [str(resources[rtype]) for rtype in self.resource_types]
            self.logfile.write(",".join(allocated_resources) + "\n")

    def schedule(self):
        self.current_usage = { key: 0 for key, val in self.total_limits.items() }
        # current algorithm is FCFS
        for index, (input_file, resources, current_disk) in list(enumerate(self.queue)):
            self.current_resources["disk"] -= current_disk

            if not self.check_fit(resources):
                self.current_resources["disk"] += current_disk
                continue
                # break

            input_file, resources, current_disk = self.queue.pop(index)
            self.run(input_file, resources)
            break

        # skip the rest if there are no running processes
        if not len(self.proc_pipes):
            return

        self.logcount += 1
        for conn, wf_id in self.proc_pipes:
            # if not conn.poll():
            #     continue 
            self.__handle_request(conn, wf_id)

        self.queue = sorted(self.queue, key=lambda x: x[2], reverse=True)
        self.current_usage["disk"] = self.current_resources["disk"]
        usage_str = [ str(self.current_usage[rtype]) for rtype in self.resource_types ]
        # self.logfile.write(str(self.logcount) + ",")
        # self.logfile.write(",".join(usage_str) + "\n")
        self.logfile.flush()

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

