import Workflow
import os
from utils import file_hash

class WorkflowEventHandler():
    def __init__(self, output_dir, workflow, process_list, process_limit=5):
        self.output_dir = output_dir
        self.process_list = process_list
        self.workflow = workflow

    def handle(self, event_path):
        input_file = self.__process_filename(event_path)
        print("handling", input_file)
        self.process_list.append(self.workflow.run(input_file, self.output_dir))

    def __process_filename(self, event_path):
        # construct new filename
        filename = os.path.basename(event_path)
        h = file_hash(event_path)
        new_filename = filename + "-pre-" + h
        dirname = os.path.dirname(event_path)
        new_event_path = os.path.join(os.path.dirname(event_path), new_filename)
        # rename the file
        os.rename(event_path, new_event_path)
        return new_event_path


