import json
import os
import signal
import threading
import time
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
        self.stop_signal_path = os.path.join(self.working_dir, self.config.get('stop_signal', ''))
        self.max_run_time = self.config.get('max_run_time', None)
        self.stop_event = threading.Event()
        self.processes = {}
        self.pgid_dict = Manager().dict()  # Shared dictionary for PGIDs

    def start_services(self, start_time):
        manager = Manager()
        state_dict = manager.dict()
        state_times = manager.dict()

        cond = Condition()

        for service in self.sorted_services:
            service_config = self.services[service]

            state_dict[service] = "initial"
            state_times[service] = {}

            p_exec = Process(target=execute_program, args=(
                service_config, self.working_dir, state_dict, service, cond, state_times, start_time, self.pgid_dict))

            p_exec.start()
            self.processes[service] = p_exec

        stop_thread = threading.Thread(target=self.check_stop_conditions, args=(start_time,))
        stop_thread.start()

        for p in self.processes.values():
            p.join()

        stop_thread.join()

        # todo: additional cleanup
        if os.path.exists(self.stop_signal_path):
            os.remove(self.stop_signal_path)

        save_state_times(state_times, os.path.join(self.working_dir, self.output['state_times']))

    def check_stop_conditions(self, start_time):
        while not self.stop_event.is_set():
            if self.check_stop_signal() or self.check_max_run_time(start_time):
                self.stop_all_services()
                self.stop_event.set()
            else:
                self.stop_event.wait(timeout=1)  # Wait with timeout to recheck conditions

    def stop_service(self, service_name):
        if service_name in self.processes:
            process = self.processes[service_name]
            if process.is_alive():
                pgid = self.pgid_dict.get(service_name)
                if pgid:
                    os.killpg(pgid, signal.SIGTERM)  # Terminate the process group

                process.terminate()
                process.join()
            print(f"Service {service_name} has been stopped.")
        else:
            print(f"Service {service_name} not found.")

    def stop_all_services(self):
        for service_name, process in self.processes.items():
            if process.is_alive():
                pgid = self.pgid_dict.get(service_name)
                if pgid:
                    try:
                        os.killpg(pgid, signal.SIGTERM)  # Terminate the process group
                    except ProcessLookupError:
                        print(f"Process group {pgid} for service {service_name} not found.")
                process.terminate()
                process.join()
            print(f"Service {service_name} has been stopped.")

        print("All services have been stopped.")

    def check_stop_signal(self):
        if os.path.exists(self.stop_signal_path):
            print("Received stop signal")
            return True

    def check_max_run_time(self, start_time):
        if self.max_run_time:
            current_time = time.time()
            if (current_time - start_time) > self.max_run_time:
                print("Maximum runtime exceeded. Stopping all services.")
                return True
        return False
