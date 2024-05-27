import os
import subprocess
import threading
import time

from log_monitor import monitor_log_file


def execute_program(config, working_dir, state_dict, service_name, cond, state_times, start_time, pgid_dict,
                    stop_event):
    command = config['command']

    log_file = os.path.join(working_dir, config['log_file'])
    error_file = os.path.join(working_dir, config['error_file'])

    dependencies = config.get('depends_on', {})
    state_keywords = config.get('state', {}).get('log', {})  # todo: refactor -> state can come from other places too

    with cond:
        for dependency, state in dependencies.items():
            while state_dict.get(dependency) != state:
                cond.wait()

    print(f"DEBUG: Starting execution of {service_name}")

    log_thread = threading.Thread(target=monitor_log_file,
                                  args=(
                                      log_file, state_dict, service_name, state_keywords, cond, state_times,
                                      start_time, stop_event))
    log_thread.start()

    local_state_times = {'start': time.time() - start_time}
    state_times[service_name] = local_state_times

    with open(log_file, 'w') as log, open(error_file, 'w') as err:
        process = subprocess.Popen(command, shell=True, cwd=working_dir, stdout=log, stderr=err, preexec_fn=os.setsid)

        pgid_dict[service_name] = os.getpgid(process.pid)
        process.wait()

    with cond:
        state_dict[service_name] = "final"
        local_state_times = state_times[service_name]
        local_state_times['final'] = time.time() - start_time
        state_times[service_name] = local_state_times
        cond.notify_all()

    log_thread.join()
    print(f"DEBUG: Finished execution of {service_name}")
