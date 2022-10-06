# Copyright (C) 2022 The University of Notre Dame
# This software is distributed under the GNU General Public License.
# See the file COPYING for details.
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
    def __init__(self, output_dir, workflow, error_dir, total_limits={"memory": 20000, "cores":1100, "disk": 75000, "jobs": 2000}, workflow_limits={"cores": 110, "memory": 2000, "disk": 5000,  "jobs": 200}):
        self.output_dir = output_dir
        self.error_dir = error_dir
        self.workflow = workflow
        self.proc_list = []
        self.proc_pipes = []
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

    # pushes a file to the internal queue to be scheduled
    def push(self, event_path):
        self.queue.append((event_path, copy(self.default_wf_limits), 0))

    # starts running a makeflow specified at path with 'resources' allocated
    def run(self, path, resources):
        logging.info(f"running: {os.path.basename(path)}")
        mufasa_conn, workflow_conn = multiprocessing.Pipe()
        self.proc_list.append((self.workflow.run(path, self.output_dir, self.error_dir, workflow_conn, resources), resources, path))
        self.proc_pipes.append((mufasa_conn, self.wf_id))
        self.wf_id += 1
        for res, val in resources.items():
            self.current_resources[res] += val
        logging.info(f"current_resources: {self.current_resources}")

    # checks if a workflow with `resources` can fit within the total limits
    def check_fit(self, resources):
        for res, val in resources.items():
            # if it won't fit
            if self.total_limits[res] - self.current_resources[res] < val:
                return False
        return True

    # when a workflow is done running this cleans up the children processes
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

    # handles interprocess communication between Mufasa and the WMSs
    # conn is the pipe between Mufasa and the WMS
    # wf_id is the id number of the associated WMS
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
            self.logfile.write("," + ",".join(allocated_resources) + "\n")

    # loops through the internal queue, checks if processes can be scheduled
    # then loops through all the running processes to collect updates
    # accumulates the esource consumption and outputs to a logfile
    def schedule(self):
        self.current_usage = { key: 0 for key, val in self.total_limits.items() }

        for index, (input_file, resources, current_disk) in list(enumerate(self.queue)):
            self.current_resources["disk"] -= current_disk

            if not self.check_fit(resources):
                self.current_resources["disk"] += current_disk
                continue

            input_file, resources, current_disk = self.queue.pop(index)
            self.run(input_file, resources)
            break

        # skip the rest if there are no running processes
        if not len(self.proc_pipes):
            return

        self.logcount += 1
        for conn, wf_id in self.proc_pipes:
            self.__handle_request(conn, wf_id)

        self.queue = sorted(self.queue, key=lambda x: x[2], reverse=True)
        self.current_usage["disk"] = self.current_resources["disk"]
        usage_str = [ str(self.current_usage[rtype]) for rtype in self.resource_types ]
        self.logfile.write(str(self.logcount) + ",")
        self.logfile.write(str(-1) + ",")
        self.logfile.write(",".join(usage_str) + "\n")
        self.logfile.flush()
