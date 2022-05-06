#! /usr/bin/env python

import socket
import json
import multiprocessing
import os

from function_handler import handler, get_name

'''
The function signature should always be the same, but what is actually
contained in the event will be different. You decide what is in the event,
and the function body (that you define) will work accordingly. For now,
this is just a dummy prototype
'''
def function_handler(event, response):
	result = int(event["a"]) + int(event["b"])
	response["Result"] = result
	response["StatusCode"] = 200

def send_configuration(config):
	config_string = json.dumps(config)
	print(len(config_string) + 1, "\n", config_string, flush=True)

def main():
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		# modify the port argument to be 0 to listen on an arbitrary port
		s.bind(('localhost', 0))
	except Exception as e:
		s.close()
		print(e)
		exit(1)

	# information to print to stdout for worker
	config = {
		"name": get_name(),
		"port": s.getsockname()[1],
	}

	send_configuration(config)

	while True:
		s.listen()
		conn, addr = s.accept()
		print('Connection from {}'.format(addr))
		while True:
			# peek at message to find newline to get the size
			event_size = None
			line = conn.recv(100, socket.MSG_PEEK)
			eol = line.find(b'\n')
			if eol >= 0:
				size = eol+1
				# actually read the size of the event
				event_size = int(conn.recv(size).decode('utf-8').split()[1])

			if event_size:
				# receive the event itself
				event = conn.recv(event_size)

				event = json.loads(event)
				print('event: {}'.format(event))

				# create a forked process for function handler
				manager = multiprocessing.Manager()
				response = manager.dict()
				p = multiprocessing.Process(target=handler, args=(event, response))
				p.start()
				p.join()

				response = json.dumps(response.copy())
				response_size = len(response)

				size_msg = "data {}\n".format(response_size)

				# send the size of response
				conn.sendall(size_msg.encode('utf-8'))

				# send response
				conn.sendall(response.encode('utf-8'))

				break

	return 0

if __name__ == "__main__":
	main()
