import time
import os


def monitor_log_file(log_path, state_dict, service_name, state_keywords, cond, state_times, start_time):
    if not state_keywords:
        return

    while not os.path.exists(log_path):
        time.sleep(0.1)

    # todo: get from user?
    final_state = list(state_keywords.keys())[-1]

    with open(log_path, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                time.sleep(0.1)
                continue

            current_time = time.time() - start_time

            reached_final_state = False

            for state in state_keywords:
                if state_keywords[state] in line:
                    with cond:
                        state_dict[service_name] = state
                        local_state_times = state_times[service_name]
                        local_state_times[state] = current_time
                        state_times[service_name] = local_state_times
                        cond.notify_all()

                        if state == final_state:
                            reached_final_state = True
                            break

            if reached_final_state:
                print(f'Reached final state {final_state} for {service_name}')
                break
