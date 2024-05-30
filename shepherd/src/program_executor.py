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

        print(f"DEBUG: Starting execution of {service_name}")

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
