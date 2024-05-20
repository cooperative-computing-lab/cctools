import subprocess
import threading
import time

from log_monitor import monitor_log_file


def execute_program(command, log_file, error_file, working_dir, state_dict, service_name, dependencies, state_keywords,
                    cond, state_times, start_time):
    log_thread = threading.Thread(target=monitor_log_file,
                                  args=(
                                      log_file, state_dict, service_name, state_keywords, cond, state_times,
                                      start_time))
    log_thread.start()

    with cond:
        for dependency, state in dependencies.items():
            while state_dict.get(dependency) != state:
                cond.wait()

    # todo: get states from config
    local_state_times = {
        'start': time.time() - start_time,
        'ready': None,
        'completed': None
    }
    state_times[service_name] = local_state_times

    with open(log_file, 'w') as log, open(error_file, 'w') as err:
        process = subprocess.Popen(command, shell=True, cwd=working_dir, stdout=log, stderr=err)
        process.wait()

    with cond:
        state_dict[service_name] = "completed"
        local_state_times = state_times[service_name]
        local_state_times['completed'] = time.time() - start_time
        state_times[service_name] = local_state_times
        cond.notify_all()

    log_thread.join()
