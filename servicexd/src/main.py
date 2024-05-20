import sys
import time

from service_manager import ServiceManager


def main():
    config_path = sys.argv[1]

    start_time = time.time()

    service_manager = ServiceManager(config_path)
    service_manager.start_services(start_time)


if __name__ == '__main__':
    main()
