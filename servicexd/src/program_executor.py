import os
import subprocess
import threading
import time

from log_monitor import monitor_log_file


def execute_program(config, working_dir, state_dict, service_name, cond, state_times, start_time, pgid_dict,
                    stop_event):
    command = config['command']

    stdout_path = os.path.join(working_dir, config['stdout_path'])
    stderr_path = os.path.join(working_dir, config['stderr_path'])

    dependencies = config.get('dependency', {}).get('items', {})
    dependency_mode = config.get('dependency', {}).get('mode', 'all')

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

    state_keywords = config.get('state', {}).get('log', {})

    log_thread = threading.Thread(target=monitor_log_file,
                                  args=(
                                      stdout_path, state_dict, service_name, state_keywords, cond, state_times,
                                      start_time, stop_event))
    log_thread.start()

    local_state_times = {'start': time.time() - start_time}
    state_times[service_name] = local_state_times

    with open(stdout_path, 'w') as out, open(stderr_path, 'w') as err:
        process = subprocess.Popen(command, shell=True, cwd=working_dir, stdout=out, stderr=err, preexec_fn=os.setsid)

        pgid_dict[service_name] = os.getpgid(process.pid)
        process.wait()

    with cond:
        state_dict[service_name] = "final"
        local_state_times = state_times[service_name]
        local_state_times['final'] = time.time() - start_time
        state_times[service_name] = local_state_times

        print(f"DEBUG: {service_name} reached state 'final' at {local_state_times['final']}")

        cond.notify_all()

    log_thread.join()
    print(f"DEBUG: Finished execution of {service_name}")
