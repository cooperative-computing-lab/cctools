import time
import os

def monitor_log_file(log_file, state_dict, service_name, state_keywords, cond, state_times, start_time):
    while not os.path.exists(log_file):
        time.sleep(0.1)  # Wait until the log file is created

    with open(log_file, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                time.sleep(0.1)
                continue

            current_time = time.time() - start_time

            #todo: state can be any arbitary word

            if 'ready' in state_keywords and state_keywords['ready'] in line:
                with cond:
                    state_dict[service_name] = "ready"
                    local_state_times = state_times[service_name]
                    local_state_times['ready'] = current_time
                    state_times[service_name] = local_state_times
                    cond.notify_all()

            elif state_keywords['complete'] in line:
                with cond:
                    state_dict[service_name] = "completed"
                    local_state_times = state_times[service_name]
                    local_state_times['completed'] = current_time
                    state_times[service_name] = local_state_times
                    cond.notify_all()
                    break
