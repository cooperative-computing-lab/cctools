from multiprocessing import Process
import json
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import time
import yaml

# --- config_loader.py ---


def load_and_preprocess_config(filepath):
    """Loads and preprocesses configuration from a YAML file."""
    with open(filepath, 'r') as file:
        config = yaml.safe_load(file)

    preprocess_config(config, filepath)

    print(f"DEBUG: Loaded and preprocessed config from {filepath}")
    return config


def preprocess_config(config, config_path):
    """Automatically fills in missing stdout_path and stderr_path paths."""
    services = config.get('services', {})
    stdout_dir = config.get('output', {}).get('stdout_dir', '')
    working_dir = os.path.dirname(os.path.abspath(config_path))

    for service_name, details in services.items():
        # Auto-fill log and error files if not specified
        if 'stdout_path' not in details:
            details['stdout_path'] = f"{service_name}_stdout.log"
        if 'stderr_path' not in details:
            details['stderr_path'] = f"{service_name}_stderr.log"

        if stdout_dir:
            details['stdout_path'] = os.path.join(stdout_dir, details['stdout_path'])
            details['stderr_path'] = os.path.join(stdout_dir, details['stderr_path'])
        else:
            details['stdout_path'] = os.path.join(working_dir, details['stdout_path'])
            details['stderr_path'] = os.path.join(working_dir, details['stderr_path'])

        state_file_path = details.get('state', {}).get('file', {}).get('path', "")

        if state_file_path:
            details['state']['file']['path'] = os.path.join(working_dir, state_file_path)


def validate_and_sort_programs(config):
    print("DEBUG: Validating and sorting programs")
    required_keys = ['services']

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required key: {key}")

    services = config['services']

    for service, details in services.items():
        if 'command' not in details:
            raise ValueError(f"Program {service} is missing the 'command' key")
        if 'stdout_path' not in details:
            raise ValueError(f"Program {service} is missing the 'stdout_path' key")

    sorted_services = topological_sort(services)
    print(f"DEBUG: Sorted services: {sorted_services}")
    return sorted_services


def topological_sort(programs):
    print("DEBUG: Performing topological sort")

    graph = {program: details.get('dependency', {}).get('items', {}) for program, details in programs.items()}

    visited = set()
    visiting = set()
    stack = []

    def dfs(node):
        if node in visiting:
            raise ValueError(f"Cyclic dependency on {node}")

        visiting.add(node)

        if node not in visited:
            visited.add(node)
            for neighbor in graph[node]:
                dfs(neighbor)
            stack.append(node)

        visiting.remove(node)

    for program in graph:
        dfs(program)
    print(f"DEBUG: Topological sort result: {stack}")
    return stack


# --- log_monitor.py ---


def monitor_log_file(log_path, state_dict, service_name, state_keywords, cond, state_times, start_time, stop_event):
    print(f"DEBUG: Starting to monitor file '{log_path}' for {service_name}")

    if not state_keywords:
        print(f"DEBUG: No state keywords for {service_name}, exiting monitor")
        return

    while not os.path.exists(log_path):
        if stop_event.is_set():
            print(f"DEBUG: Stop event set, exiting monitor for {service_name}")
            return
        time.sleep(0.1)

    last_state = list(state_keywords.keys())[-1]

    with open(log_path, 'r') as file:
        while not stop_event.is_set():
            line = file.readline()
            if not line:
                time.sleep(0.01)
                continue

            current_time = time.time() - start_time

            reached_last_state = False

            for state in state_keywords:
                if state_keywords[state] in line:
                    with cond:
                        state_dict[service_name] = state
                        local_state_times = state_times[service_name]
                        local_state_times[state] = current_time
                        state_times[service_name] = local_state_times
                        cond.notify_all()

                        print(f"DEBUG: {service_name} reached state '{state}' at {current_time}")

                        if state == last_state:
                            reached_last_state = True
                            break

            if reached_last_state:
                break

    print(f"DEBUG: Finished monitoring file '{log_path}' for {service_name}")


# --- program_executor.py ---


def execute_program(config, working_dir, state_dict, service_name, cond, state_times, start_time, pgid_dict,
                    stop_event):
    def signal_handler(signum, frame):
        print(f"Received signal {signum} in {service_name}")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    command = config['command']
    stdout_path = config['stdout_path']
    stderr_path = config['stderr_path']

    dependencies = config.get('dependency', {}).get('items', {})
    dependency_mode = config.get('dependency', {}).get('mode', 'all')
    stdout_states = config.get('state', {}).get('log', {})
    file_path_to_monitor = config.get('state', {}).get('file', {}).get('path', '')
    file_states = config.get('state', {}).get('file', {}).get('states', {})

    service_type = config.get('type', 'action')

    with cond:
        state_dict[service_name] = "initialized"
        update_state_time(service_name, "initialized", start_time, state_times)
        cond.notify_all()

    try:
        with cond:
            if dependency_mode == 'all':
                for dep_service, required_state in dependencies.items():
                    while required_state not in state_times.get(dep_service, {}):
                        cond.wait()

            elif dependency_mode == 'any':
                satisfied = False
                while not satisfied and not stop_event.is_set():
                    for dep_service, required_state in dependencies.items():
                        if required_state in state_times.get(dep_service, {}):
                            satisfied = True
                            break
                    if not satisfied:
                        cond.wait()

        print(f"DEBUG: Starting execution of '{service_type}' {service_name}")

        with cond:
            state_dict[service_name] = "started"
            update_state_time(service_name, "started", start_time, state_times)
            cond.notify_all()

        # Start the main log monitoring thread
        log_thread = threading.Thread(target=monitor_log_file,
                                      args=(stdout_path, state_dict, service_name, stdout_states, cond, state_times,
                                            start_time, stop_event))
        log_thread.start()

        # Optional: Start additional file monitoring thread if a file path is specified
        file_monitor_thread = None
        if file_path_to_monitor:
            file_monitor_thread = threading.Thread(target=monitor_log_file,
                                                   args=(
                                                       file_path_to_monitor, state_dict, service_name, file_states,
                                                       cond,
                                                       state_times, start_time, stop_event))
            file_monitor_thread.start()

        # Execute the process
        with open(stdout_path, 'w') as out, open(stderr_path, 'w') as err:
            process = subprocess.Popen(command, shell=True, cwd=working_dir, stdout=out, stderr=err,
                                       preexec_fn=os.setsid)
            pgid_dict[service_name] = os.getpgid(process.pid)

        while process.poll() is None:
            time.sleep(0.1)

        return_code = process.returncode

        print(f"Returned with code {return_code}")

        with cond:
            if stop_event.is_set() and return_code == -signal.SIGTERM:
                state_dict[service_name] = "stopped"
                update_state_time(service_name, "stopped", start_time, state_times)
                cond.notify_all()

        if service_type == 'service' and not stop_event.is_set():
            print(f"DEBUG: Stopping execution of '{service_type}' {service_name}")

            # If a service stops before receiving a stop event, mark it as failed
            with cond:
                state_dict[service_name] = "failure"
                update_state_time(service_name, "failure", start_time, state_times)
                cond.notify_all()
            print(f"ERROR: Service {service_name} stopped unexpectedly, marked as failure.")

        elif service_type == 'action':
            action_state = "action_success" if return_code == 0 else "action_failure"

            with cond:
                state_dict[service_name] = action_state
                update_state_time(service_name, action_state, start_time, state_times)
                cond.notify_all()

        with cond:
            state_dict[service_name] = "final"
            update_state_time(service_name, "final", start_time, state_times)
            cond.notify_all()

        if log_thread.is_alive():
            log_thread.join()

        if file_monitor_thread and file_monitor_thread.is_alive():
            file_monitor_thread.join()

    except Exception as e:
        print(f"Exception in executing {service_name}: {e}")

    print(f"DEBUG: Finished execution of {service_name}")

def update_state_time(service_name, state, start_time, state_times):
    local_state_times = state_times[service_name]
    local_state_times[state] = time.time() - start_time
    state_times[service_name] = local_state_times


# --- service_manager.py ---


def save_state_times(state_times, output_file):
    print(state_times)

    state_times_dict = dict(state_times)

    for key, value in state_times_dict.items():
        state_times_dict[key] = dict(value)

    with open(output_file, 'w') as f:
        json.dump(state_times_dict, f, indent=2)


class ServiceManager:
    def __init__(self, config_path):
        print("DEBUG: Initializing ServiceManager")
        self.config = load_and_preprocess_config(config_path)
        self.services = self.config['services']
        self.sorted_services = validate_and_sort_programs(self.config)
        self.working_dir = os.path.dirname(os.path.abspath(config_path))
        self.output = self.config['output']
        self.stop_signal_path = os.path.join(self.working_dir, self.config.get('stop_signal', ''))
        self.max_run_time = self.config.get('max_run_time', None)
        self.stop_event = multiprocessing.Event()
        self.pgid_dict = multiprocessing.Manager().dict()
        self.state_dict = multiprocessing.Manager().dict()
        self.state_times = multiprocessing.Manager().dict()
        self.cond = multiprocessing.Condition()
        self.processes = {}
        print("DEBUG: ServiceManager initialized")

    def setup_signal_handlers(self):
        print("DEBUG: Setting up signal handlers")
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, signum, frame):
        print(f"DEBUG: Received signal {signum} in pid {os.getpid()}, stopping all services...")
        self.stop_event.set()

    def start_services(self, start_time):
        print("DEBUG: Starting services")
        self.setup_signal_handlers()

        for service in self.sorted_services:
            service_config = self.services[service]

            self.state_dict[service] = ""
            self.state_times[service] = {}

            p_exec = Process(target=execute_program, args=(
                service_config, self.working_dir, self.state_dict, service, self.cond, self.state_times, start_time,
                self.pgid_dict, self.stop_event))

            p_exec.start()
            self.processes[service] = p_exec

        print("DEBUG: All services initialized")

        stop_thread = threading.Thread(target=self.check_stop_conditions, args=(start_time,))
        stop_thread.start()

        for p in self.processes.values():
            p.join()

        stop_thread.join()

        if os.path.isfile(self.stop_signal_path) and os.path.exists(self.stop_signal_path):
            os.remove(self.stop_signal_path)

        save_state_times(self.state_times, os.path.join(self.working_dir, self.output['state_times']))

    def check_stop_conditions(self, start_time):
        print("DEBUG: Checking stop conditions")
        while not self.stop_event.is_set():
            if self.check_stop_signal_file() or self.check_max_run_time(start_time):
                self.stop_event.set()
            else:
                self.stop_event.wait(timeout=1)

        self.stop_all_services()

        print("DEBUG: Finished checking stop conditions")

    def stop_all_services(self):
        print("DEBUG: Stopping all services")
        for service_name, process in self.processes.items():
            pgid = self.pgid_dict.get(service_name)
            if pgid:
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    print(f"Process group {pgid} for service {service_name} not found.")
            # process.terminate()

        for process in self.processes.values():
            process.join()

        print("DEBUG: All services have been stopped")

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

    def check_stop_signal_file(self):
        if os.path.exists(self.stop_signal_path) and os.path.isfile(self.stop_signal_path):
            print("Received stop signal")
            return True

    def check_max_run_time(self, start_time):
        if self.max_run_time:
            current_time = time.time()
            if (current_time - start_time) > self.max_run_time:
                print("Maximum runtime exceeded. Stopping all services.")
                return True
        return False


# --- main.py ---


def main():
    config_path = sys.argv[1]

    start_time = time.time()

    print("DEBUG: Starting main")
    service_manager = ServiceManager(config_path)
    service_manager.start_services(start_time)
    print("DEBUG: Exiting main")


# --- Main Execution Block ---
if __name__ == '__main__':
    main()