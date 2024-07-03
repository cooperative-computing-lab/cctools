#!/usr/bin/env python3
import multiprocessing
import sys
import time
import logging
import logging.config
import logging.handlers

from shepherd.service_manager import ServiceManager
from shepherd.logging_setup import setup_logging, listener_process

__version__ = "0.1.0"

def main():
    config_path = sys.argv[1]

    if len(sys.argv) > 2:
        log_file = sys.argv[2]
    else:
        log_file = f"shepherd.{time.time()}.log"

    start_time = time.time()

    logging_queue = multiprocessing.Queue()
    listener = multiprocessing.Process(target=listener_process, args=(logging_queue, log_file))
    listener.start()
    setup_logging(logging_queue)

    logging.debug("Starting main")
    service_manager = ServiceManager(config_path, logging_queue)
    service_manager.start_services(start_time)
    logging.debug("Exiting main")

    logging_queue.put(None)  # Send None to the listener to stop it
    listener.join()


if __name__ == '__main__':
    main()
