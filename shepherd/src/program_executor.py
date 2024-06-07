import os
import subprocess
import threading
import time

from log_monitor import monitor_log_file


def execute_program(config, working_dir, state_dict, service_name, cond, state_times, start_time, pgid_dict,
                    stop_event):
    command = config['command']
    stdout_path = config['stdout_path']
    stderr_path = config['stderr_path']

    dependencies = config.get('dependency', {}).get('items', {})
    dependency_mode = config.get('dependency', {}).get('mode', 'all')
    stdout_states = config.get('state', {}).get('log', {})
    file_path_to_monitor = config.get('state', {}).get('file', {}).get('path', '')
    file_states = config.get('state', {}).get('file', {}).get('states', {})

    service_type = config.get('type', 'action')  # Default to 'action' if type is not specified

    try:
        with cond:
            if dependency_mode == 'all':
                for dep_service, required_state in dependencies.items():
                    while state_dict.get(dep_service) != required_state:
                        cond.wait()

            elif dependency_mode == 'any':
                satisfied = False
                while not satisfied and not stop_event.is_set():
                    for dep_service, required_state in dependencies.items():
                        if state_dict.get(dep_service) == required_state:
                            satisfied = True
                            break
                    if not satisfied:
                        cond.wait()

        print(f"DEBUG: Starting execution of '{service_type}' {service_name}")

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
            process.wait()

        # #todo: disucss this. this was done to makeup for log monitor delay
        time.sleep(0.01)

        if service_type == 'service' and not stop_event.is_set():
            print(f"DEBUG: Stopping execution of '{service_type}' {service_name}")

            # If a service stops before receiving a stop event, mark it as failed
            with cond:
                state_dict[service_name] = "failure"
                local_state_times = state_times[service_name]
                local_state_times['failure'] = time.time() - start_time
                state_times[service_name] = local_state_times
                cond.notify_all()
            print(f"ERROR: Service {service_name} stopped unexpectedly, marked as failure.")

        with cond:
            state_dict[service_name] = "final"
            local_state_times = state_times[service_name]
            local_state_times['final'] = time.time() - start_time
            state_times[service_name] = local_state_times
            cond.notify_all()

        if log_thread.is_alive():
            log_thread.join()

        if file_monitor_thread and file_monitor_thread.is_alive():
            file_monitor_thread.join()

    except Exception as e:
        print(f"Exception in executing {service_name}: {e}")

    print(f"DEBUG: Finished execution of {service_name}")
