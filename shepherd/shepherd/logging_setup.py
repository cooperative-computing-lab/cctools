import logging
import logging.config
import logging.handlers


def setup_logging(queue):
    root = logging.getLogger()
    handler = logging.handlers.QueueHandler(queue)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


def configure_listener(log_file=None):
    root = logging.getLogger()
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


def listener_process(queue, log_file=None):
    configure_listener(log_file)
    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception:
            import sys, traceback
            print('Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
