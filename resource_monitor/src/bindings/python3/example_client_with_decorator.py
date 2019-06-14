# to run example:
# in some terminal: python3 example_udp_server.py
# in another terminal: python3 example_client_with_decorator.py

import resource_monitor

# monitor callback function example
# a callback function will be called everytime resources are measured.
# arguments are:
# - resources: if finished = False, the instantaneous value of the resources used.
#              if finished = True,  the maximum value of the resources used.
# - finished: True/False whether the monitored function has been completed
# - resources: True/False whether the monitored function exhausted resources
def send_udp_message(resources, finished, resource_exhaustion):
    """ Send a UDP message with the results of a measurement. Server implemented in callback_server.py """
    import socket
    import pickle

    print('callback: finished? {}, exhaustion? {}, wall_time: {} s, memory: {} MB'.format(finished, resource_exhaustion, resources['wall_time']/1e6, resources['memory']))

    msg = {'finished': finished, 'resource_exhaustion': resource_exhaustion, 'resources': resources}

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(pickle.dumps(msg), ('localhost', 9800))


def my_function(wait_for, buffer_size):
    """
    A function that creates a memory buffer of size buffer_size MB, and
    that waits at least wait_for seconds.  Returns the number of seconds
    explicitely waited for. (I.e., without the creation of the memory buffer
    overhead.)
    """
    import time
    start = time.time()
    buf   = bytearray(int(buffer_size * 1024 * 1024))
    delta = wait_for/1e6 - (time.time() - start)   # x/1e6 because sleep expects seconds
    if delta < 0:
        delta = 0
    time.sleep(delta)
    return delta

@resource_monitor.monitored(callback = send_udp_message, interval = 0.1 * 1e6)
def my_function_monitored(wait_for, buffer_size):
    """
    Like my_function, but because of the decorator,
    returns: (my_function(...), dict with max resources used)
    There is a measurement every 0.1 seconds (specified in us)
    """
    return my_function(wait_for, buffer_size)

# alternatively, we could have defined my_function_monitored as:
# my_function_monitored = resource_monitor.make_monitored(my_function, callback = send_udp_message, interval = 0.5/1e6)

@resource_monitor.monitored(callback = send_udp_message, limits = {'memory': 100, 'wall_time': 10e6})
def my_function_with_limits(wait_for, buffer_size):
    """
    Like my_function_monitored, but because of the decorator,
    throw an exception if memory usage goes above 100MB or wall time 10e6 us (10s)
    Since interval is not specified, it defaults to 1s
    """
    return my_function(wait_for, buffer_size)

# alternatively, we could have defined my_function_monitored as:
# my_function_with_limits = resource_monitor.make_monitored(my_function, callback = send_udp_message, limits = {'memory': ...})


print('\ncalling original function...')
result_original = my_function(wait_for = 1e6, buffer_size = 1024)
print('original function result: {}'.format(result_original))

print('\ncalling monitored function...')
(result_monitored, resources_used) = my_function_monitored(wait_for = 1e6, buffer_size = 1024)
print('monitored function result: {}'.format(result_monitored))
print('monitored function resources used: {}'.format(resources_used))

print('\ncalling function with limits...')
try:
    (result_exh, resources_exh) = my_function_with_limits(wait_for = 10e6, buffer_size = 1024)
except resource_monitor.ResourceExhaustion as e:
    print(e)
    print('resources broken: {}'.format(e.resources['limits_exceeded']))

