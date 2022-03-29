import os
from utils import parse_filename
import shutil
import multiprocessing
import subprocess
import psutil

from WorkflowProfiler import profile
import resource_monitor

class Workflow():
        # note that the workflow should accept a single input gzipped tar file
        # and it should output a single gzipped tar file called output
        # the workflow executable should be 
        def __init__(self, makeflow_file, expected_input_name):
            self.makeflow = makeflow_file
            self.makeflow_dir = os.path.dirname(self.makeflow)
            self.expected_input_name = expected_input_name

        def run(self, input_file, output_directory, error_dir, conn, resources):
            proc = multiprocessing.Process(target=self.__run, args=(input_file, output_directory, error_dir, conn, resources))
            proc.start()
            return proc

        # takes the input file, runs it with the given workflow
        # puts the output file in output_directory
        def __run(self, input_file, output_directory, error_dir, conn, resources):
            # parse the filename
            (inputname, filehash, ext) = parse_filename(os.path.basename(input_file))
            workflow_directory_name = os.path.abspath(f"makeflow-{inputname}-{filehash}")

            # copy the makeflow code and input file
            if not os.path.isdir(workflow_directory_name):
                shutil.copytree(self.makeflow_dir, workflow_directory_name)
                shutil.copyfile(input_file, os.path.join(workflow_directory_name, self.expected_input_name))

            # change to the new makeflow directory
            prev_dir = os.getcwd()
            os.chdir(workflow_directory_name)
            
            # run the makeflow
            prc = subprocess.Popen(["makeflow", os.path.basename(self.makeflow), f"--local-cores={resources['cpuusage']}", f"--local-memory={resources['memusage']}", f"--local-disk={resources['disk']}"], stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

            # monitor memory and cpu usage
            cpu_usage, mem_usage, ccpu, cmem, reason = profile(prc.pid, workflow_directory_name, interval=0.5, resources=resources, conn=conn)

            # wait for it to finish to get return code
            for p in psutil.Process(prc.pid).children(recursive=True):
                p.wait()
            prc.wait()

            stats = {"pid": os.getpid(), "exitcode": prc.returncode, "memusage": mem_usage, "cpuusage": cpu_usage, "cluster_cpu": ccpu, "cluster_mem": cmem, "reason": reason}
            if not reason:
                # create output filename
                output_filename = inputname + "-" +  filehash + ".tar.gz"

                if prc.returncode:
                    # shutil.move("output.tar.gz", os.path.join(error_dir, output_filename))
                    status = "error"
                else:
                    shutil.move("output.tar.gz", os.path.join(output_directory, output_filename))
                    status = "success"

                os.chdir(prev_dir)
                shutil.rmtree(workflow_directory_name)

                input_dir = os.path.dirname(input_file)
                os.rename(input_file, os.path.join(input_dir, inputname + "-post-" + filehash + ext))
            else:
                message = {"status": "request_pause", "allocated_resources": resources, "workflow_stats": stats}
                conn.send(message)

                # wait for response
                resp = conn.recv()

                # if it says to kill, then delete the working directory
                if resp["response"] == "kill":
                    print(f"Killing because of {reason}: {stats['reason']}")
                    os.chdir(prev_dir)
                    shutil.rmtree(workflow_directory_name)
                    status = "killed"
                else:
                    print(f"Pausing because of {reason}: {stats['reason']}")
                    status = "paused"

            message = {"status": status, "allocated_resources": resources, "workflow_stats": stats}
            conn.send(message)
            conn.close()


