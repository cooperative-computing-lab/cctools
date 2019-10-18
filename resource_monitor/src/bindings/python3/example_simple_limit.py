import resource_monitor

import sys
import time

@resource_monitor.monitored(limits = {'wall_time': 1e6})  # wall_time in microseconds
def my_function(n):
    sys.stdout.write("waiting for {} seconds...".format(n))
    time.sleep(n)
    sys.stdout.write("done.\n")

    return n

try:
    (output, resources) = my_function(0.5)
except Exception as e:
    sys.stdout.write("\nGot exception <{}>, but did not expect any error.\n".format(e))
    sys.exit(1)


try:
    (output, resources) = my_function(2)
except resource_monitor.ResourceExhaustion as e:
    sys.stdout.write("\nGot expected exception <{}>.\n".format(e))
except Exception as e:
    sys.stdout.write("\nGot exception <{}>, but did not expect such error.\n".format(e))
    sys.exit(1)

sys.exit(0)

