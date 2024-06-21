import os
import signal
import subprocess
import threading
import time

from log_monitor import monitor_log_file


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
    current_time = time.time() - start_time

    local_state_times = state_times[service_name]
    local_state_times[state] = current_time
    state_times[service_name] = local_state_times

    print(f"DEBUG: Service '{service_name}' reached the state '{state}' at time {current_time:.3f}")