import json
import os
from multiprocessing import Manager, Condition, Process
from config_loader import load_config_from_yaml, validate_and_sort_programs
from program_executor import execute_program


def save_state_times(state_times, output_file):
    print(state_times)

    state_times_dict = dict(state_times)

    for key, value in state_times_dict.items():
        state_times_dict[key] = dict(value)

    with open(output_file, 'w') as f:
        json.dump(state_times_dict, f, indent=2)


class ServiceManager:
    def __init__(self, config_path):
        self.config = load_config_from_yaml(config_path)
        self.services = self.config['services']
        self.sorted_services = validate_and_sort_programs(self.config)
        self.working_dir = os.path.dirname(os.path.abspath(config_path))
        self.output = self.config['output']

    def start_services(self, start_time):
        manager = Manager()
        state_dict = manager.dict()
        state_times = manager.dict()

        cond = Condition()

        processes = []

        for service in self.sorted_services:
            config = self.services[service]

            command = os.path.join(self.working_dir, config['command'])
            log_file = os.path.join(self.working_dir, config['log_file'])
            error_file = os.path.join(self.working_dir, config['error_file'])

            dependencies = config.get('depends_on', {})
            state_keywords = config['state']['log']

            state_dict[service] = "initialized"
            state_times[service] = {}

            p_exec = Process(target=execute_program, args=(
                command, log_file, error_file, self.working_dir, state_dict, service, dependencies, state_keywords,
                cond, state_times, start_time))

            p_exec.start()
            processes.append(p_exec)

        for p in processes:
            p.join()

        save_state_times(state_times, os.path.join(self.working_dir, self.output['state_times']))
