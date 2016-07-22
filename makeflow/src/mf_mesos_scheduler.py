import os
import sys
import uuid
import time
import urllib
import logging
import threading
import mf_mesos_setting as mms

from mesos.interface import Scheduler
from mesos.native import MesosSchedulerDriver
from mesos.interface import mesos_pb2

logging.basicConfig(level=logging.INFO)

FILE_TASK_INFO = "task_info"
FILE_TASK_STATE = "task_state"
MF_DONE_FILE = "makeflow_done"

TASK_CPUS = 0.01
TASK_MEM = 32

EXECUTOR_CPUS = 0.01
EXECUTOR_MEM = 32

# Makeflow mesos scheduler
class MakeflowScheduler(Scheduler):

    def __init__(self, mf_wk_dir):
        self.mf_wk_dir = mf_wk_dir

    # Create a ExecutorInfo instance for mesos task
    def new_mesos_executor(self, mf_task, framework_id):
        executor = mesos_pb2.ExecutorInfo()
        executor.framework_id.value = framework_id
        executor.executor_id.value = str(uuid.uuid4())
        cctools_path = os.getenv('CCTOOLS')
        sh_path = os.path.join(cctools_path, 'bin', 'mf-mesos-executor.in')
        executor.name = "{} makeflow mesos executor".format(mf_task.task_id) 
        executor.source = "python executor"
        executor.command.value = "{} \"{}\" {} {}".format(sh_path, mf_task.cmd, 
                executor.executor_id.value, executor.framework_id.value)

        cpus = executor.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = EXECUTOR_CPUS

        mem = executor.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = EXECUTOR_MEM

        # add $CCTOOLS as the env variables for executor command
        cctools_env = executor.command.environment.variables.add()
        cctools_env.name = "CCTOOLS"
        cctools_env.value = cctools_path

        # add input files list to the executor_info
        for fn in mf_task.inp_fns:
            uri = executor.command.uris.add()
            logging.info("input file is: {}".format(fn.strip(' \t\n\r')))
            uri.value = fn.strip(' \t\n\r')
            uri.executable = False
            uri.extract = False

        return executor
    
    # Create a TaskInfo instance
    def new_mesos_task(self, offer, task_id):
        mesos_task = mesos_pb2.TaskInfo()
        mesos_task.task_id.value = task_id
        mesos_task.slave_id.value = offer.slave_id.value
        mesos_task.name = "task {}".format(str(id))
    
        cpus = mesos_task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = TASK_CPUS
    
        mem = mesos_task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = TASK_MEM
    
        return mesos_task

    def launch_mesos_task(self, driver, offer, task_id): 

        mesos_task = self.new_mesos_task(offer, task_id)
       
        mf_mesos_task_info = mms.tasks_info_dict[task_id] 

        # initialize a ExecutorInfo instance
        executor = self.new_mesos_executor(\
            mf_mesos_task_info, \
            offer.framework_id.value)

        mesos_task.executor.MergeFrom(executor)

        # TODO for version0 one executor only run with one task, in the future
        # we may wanna change the number of tasks running by one executor
        mf_mesos_executor_info = \
                mms.MfMesosExecutorInfo(\
                executor.executor_id, \
                offer.slave_id.value, offer.hostname)

        mf_mesos_executor_info.tasks.append(task_id)

        mms.executors_info_dict[executor.executor_id.value] = \
                mf_mesos_executor_info

        # combine mesos TaskInfo with ExecutorInfo
        mf_mesos_task_info.executor_id = \
                executor.executor_id.value

        mms.tasks_info_dict[task_id] \
                = mf_mesos_task_info 
        
        # create mesos task and launch it with offer 
        logging.info("Launching task {} using offer {}.".format(\
                        task_id, offer.id.value))

        # one task is corresponding to one executor
        tasks = [mesos_task]
        driver.launchTasks(offer.id, tasks)

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: {}".format(framework_id))

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: {}".format(\
                [o.id.value for o in offers]))
       
        idle_task = False 

        for offer in offers:
          
            offer_cpus = 0
            offer_mem = 0

            for resource in offer.resources:
                if resource.name == "cpus":
                    offer_cpus += resource.scalar.value
                if resource.name == "mem":
                    offer_mem += resource.scalar.value

            logging.info("Received resource offer {} with cpus {} and mem: {}\
                    ".format(offer.id.value, offer_cpus, offer_mem))

            remaining_cpus = offer_cpus
            remaining_mem = offer_mem

            with mms.lock:
                for task_info in mms.tasks_info_dict.itervalues():
                    if task_info.action == "submitted" and \
                            remaining_cpus >= TASK_CPUS and \
                            remaining_mem >= TASK_MEM:

                        idle_task = True
                        task_id = task_info.task_id
                        mms.tasks_info_dict[task_id].action = "running"

                        remaining_cpus -= TASK_CPUS
                        remaining_mem -= TASK_MEM

                        self.launch_mesos_task(driver, offer, task_id)
                        break
            
            if not idle_task:
                driver.declineOffer(offer.id)
                
        
    def statusUpdate(self, driver, update):

        if os.path.isfile(FILE_TASK_STATE): 
            oup_fn = open(FILE_TASK_STATE, "a", 0)
        else:
            logging.error("{} is not created in advanced".format(FILE_TASK_STATE))
            exit(1)

        with mms.lock:
            if update.state == mesos_pb2.TASK_ERROR:
                oup_fn.write("{},error".format(update.task_id.value))
                mms.tasks_info_dict[update.task_id.value].action = "error"
                logging.error("{}".format(update.message.value))
            if update.state == mesos_pb2.TASK_FAILED:
                oup_fn.write("{},failed\n".format(update.task_id.value))
                mms.tasks_info_dict[update.task_id.value].action = "failed"
            if update.state == mesos_pb2.TASK_FINISHED:
                oup_fn.write("{},finished\n".format(update.task_id.value))
                mms.tasks_info_dict[update.task_id.value].action = "finished"
        
        oup_fn.close()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        logging.info("Receive message {}".format(message))
        message_list = message.split()

        if message_list[0].strip(' \t\n\r') == "[EXECUTOR_OUTPUT]":
            output_file_dir = message_list[1].strip(' \t\n\r')
            curr_task_id = message_list[3].strip(' \t\n\r')

            with mms.lock:
                output_fns = mms.tasks_info_dict[curr_task_id].oup_fns

                for output_fn in output_fns:
                    output_file_addr = "{}/{}".format(output_file_dir, output_fn)
                    logging.info("The output file address is: {}".format(\
                            output_file_addr))
                    urllib.urlretrieve(output_file_addr, output_fn)
        
        if message_list[0].strip(' \t\n\r') == "[EXECUTOR_STATE]":
            curr_executor_id = message_list[1].strip(' \t\n\r')
            curr_executor_state = message_list[2].strip(' \t\n\r')
             
            with mms.lock:
                mms.executors_info_dict[curr_executor_id].state = \
                        curr_executor_state

                # if a executor is aborted, the corresponding task
                # is aborted
                if curr_executor_state == "aborted":
                    curr_task_id = message_list[3].strip(' \t\n\r')
                    file_task_state = open(FILE_TASK_STATE, "a+")
                    file_task_state.write("{},{}\n".format(curr_task_id,\
                            curr_executor_state))
                    file_task_state.close()


class MakefowMonitor(threading.Thread):
  
    def __init__(self, driver, created_time):
        threading.Thread.__init__(self)
        self.last_mod_time = created_time
        self.driver = driver
        self.first_time = True

    # Check if all tasks done
    def is_all_executor_stopped(self):
        
        with mms.lock:
            for executor_info in mms.executors_info_dict.itervalues():
                if executor_info.state == "registered":
                    return False
    
            return True

    # stop all running executors
    def stop_executors(self):
        task_action_fn = open(FILE_TASK_INFO, "r")
        lines = task_action_fn.readlines()
    
        with mms.lock:
            for line in lines:
                task_info_list = line.split(",")
                task_id = task_info_list[0]
                task_action = task_info_list[4]
                if task_action == "aborting":
                    mf_task = mms.tasks_info_dict[task_id]

                    self.driver.sendFrameworkMessage(self, \
                            mf_task.executor_id, \
                            mms.executors_info_dict[mf_task.executor_id].slave_id, \
                            "[SCHEDULER_REQUEST] abort")
    
        task_action_fn.close()

    def stop_mesos_scheduler(self):

        # If makeflow creat "makeflow_done" file, stop the scheduler
        mf_done_fn_path = os.path.join(mms.mf_wk_dir, MF_DONE_FILE)

        if os.path.isfile(mf_done_fn_path):
            mf_done_fn = open(mf_done_fn_path, "r")
            mf_state = mf_done_fn.readline().strip(' \t\n\r')
            mf_done_fn.close()

            logging.info("Makeflow workflow is {}".format(mf_state))

            if mf_state == "aborted":
                logging.info("Workflow aborted, stopping executors...")
                self.stop_executors()

            fn_run_tks_path = os.path.join(mms.mf_wk_dir, FILE_TASK_INFO)
            fn_finish_tks_path = os.path.join(mms.mf_wk_dir, FILE_TASK_STATE)

            if os.path.isfile(mf_done_fn_path):
                os.remove(mf_done_fn_path)
            if os.path.isfile(fn_run_tks_path):
                os.remove(fn_run_tks_path)
            if os.path.isfile(fn_finish_tks_path):
                os.remove(fn_finish_tks_path)
           
            while(not self.is_all_executor_stopped()):
                pass

            self.driver.stop()  
    
    
    def abort_mesos_task(self, task_id):
        logging.info("Makeflow is trying to abort task {}.".format(task_id))
       
        if mms.tasks_info_dict[task_id].action == "finished" or \
                mms.tasks_info_dict[task_id].action == "failed" or \
                mms.tasks_info_dict[task_id].action == "aborted":
                return

        with mms.lock:
            if mms.tasks_info_dict[task_id].action == "submitted":
                mms.tasks_info_dict[task_id].action = "aborted"
            if mms.tasks_info_dict[task_id].action == "running":
                py_task_id = mesos_pb2.TaskID()
                py_task_id.value = task_id 
                self.driver.killTask(py_task_id)
                mms.tasks_info_dict[task_id].action = "aborted"

        if os.path.isfile(FILE_TASK_STATE): 
            oup_fn = open(FILE_TASK_STATE, "a", 0)
        else:
            logging.error("{} is not created in advanced".format(FILE_TASK_STATE))
            exit(1)
        
        oup_fn.write("{},aborted\n".format(task_id))
        oup_fn.close()

    def run(self):

        while(not os.path.isfile(MF_DONE_FILE)):
            mod_time = os.stat(FILE_TASK_INFO).st_mtime
            
            if (self.last_mod_time != mod_time or self.first_time):

                if self.first_time:
                    self.first_time = False

                logging.info("{} is modified at {}".format(\
                        FILE_TASK_INFO, mod_time))
                self.last_mod_time = mod_time
                task_info_fp = open(FILE_TASK_INFO, "r")
                lines = task_info_fp.readlines()

                for line in lines:
                    task_info_list = line.split(",")
                    task_id = task_info_list[0].strip(" \t\n\r")
                    task_cmd = task_info_list[1].strip(" \t\n\r")
                    task_inp_fns = task_info_list[2].split()
                    task_oup_fns = task_info_list[3].split()
                    task_action = task_info_list[4].strip(" \t\n\r")
                   
                    with mms.lock:
                        # find new tasks
                        if (task_id not in mms.tasks_info_dict):

                            mf_mesos_task_info = mms.MfMesosTaskInfo(\
                                    task_id, task_cmd, task_inp_fns, task_oup_fns, \
                                    task_action)

                            mms.tasks_info_dict[task_id] \
                                    = mf_mesos_task_info

                        # makeflow trying to abort an exist task
                        if (task_id in mms.tasks_info_dict) and\
                                (task_action == "aborting"):

                            self.abort_mesos_task(task_id)                

            else:
                time.sleep(1)
       
        self.stop_mesos_scheduler() 

if __name__ == '__main__':
    # make us a framework
    mms.mf_wk_dir = sys.argv[1]

    # create the "task_state" and "task_info" file
    if not os.path.isfile(FILE_TASK_STATE):
        open(FILE_TASK_STATE, 'w').close()
    if not os.path.isfile(FILE_TASK_INFO):
        open(FILE_TASK_INFO, 'w').close()
    created_time = os.stat(FILE_TASK_INFO).st_mtime

    # initialize a framework instance
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "Makeflow"
    driver = MesosSchedulerDriver(
        MakeflowScheduler(mms.mf_wk_dir),
        framework,
        "127.0.0.1:5050/"  # assumes running on the master
    )
  
    mf_monitor = MakefowMonitor(driver, created_time)
    mf_monitor.start()

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    sys.exit(status)
