import os
import time
import logging


def monitor_log_file(log_path, state_dict, service_name, state_keywords, cond, state_times, start_time, stop_event):
    logging.debug(f"Starting to monitor file '{log_path}' for {service_name}")

    if not state_keywords:
        logging.debug(f"No state keywords for {service_name}, exiting monitor")
        return

    while not os.path.exists(log_path):
        if stop_event.is_set():
            logging.debug(f"Stop event set, exiting monitor for {service_name}")
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

                        logging.debug(f"{service_name} reached state '{state}' at {current_time}")

                        if state == last_state:
                            reached_last_state = True
                            break

            if reached_last_state:
                break

    logging.debug(f"Finished monitoring file '{log_path}' for {service_name}")
