import os
from utils import parse_filename
import shutil
import multiprocessing
import subprocess
from memory_profiler import memory_usage

class Workflow():
        # note that the workflow should accept a single input gzipped tar file
        # and it should output a single gzipped tar file called output
        # the workflow executable should be 
        def __init__(self, makeflow_file, expected_input_name):
            self.makeflow = makeflow_file
            self.makeflow_dir = os.path.dirname(self.makeflow)
            self.expected_input_name = expected_input_name

        def run(self, input_file, output_directory, error_dir):
            proc = multiprocessing.Process(target=self.__run, args=(input_file, output_directory, error_dir))
            proc.start()
            return proc

        # takes the input file, runs it with the given workflow
        # puts the output file in output_directory
        def __run(self, input_file, output_directory, error_dir):
            # parse the filename
            (inputname, filehash, ext) = parse_filename(os.path.basename(input_file))
            workflow_directory_name = f"makeflow-{inputname}-{filehash}"

            # copy the makeflow code and input file
            shutil.copytree(self.makeflow_dir, workflow_directory_name)
            shutil.copyfile(input_file, os.path.join(workflow_directory_name, self.expected_input_name))

            # change to the new makeflow directory
            prev_dir = os.getcwd()
            os.chdir(workflow_directory_name)
            
            # run the makeflow
            prc = subprocess.Popen(["makeflow", os.path.basename(self.makeflow)], stdout=subprocess.DEVNULL)

            # monitor memory usage
            mem_usage = memory_usage(proc=prc, interval=0.2, include_children=True, max_usage=True)

            # wait for makeflow to complete
            prc.wait()
            print("Maximum Memory (MB):", mem_usage)
            
            # create output filename
            output_filename = inputname + "-" +  filehash + ".tar.gz"

            if prc.returncode:
                shutil.move("output.tar.gz", os.path.join(error_dir, output_filename))
            else:
                shutil.move("output.tar.gz", os.path.join(output_directory, output_filename))

            os.chdir(prev_dir)
            shutil.rmtree(workflow_directory_name)

            input_dir = os.path.dirname(input_file)
            os.rename(input_file, os.path.join(input_dir, inputname + "-post-" + filehash + ext))

