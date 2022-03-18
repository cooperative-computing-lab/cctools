import socket
import json

'''
The function signature should always be the same, but what is actually
contained in the event will be different. You decide what is in the event,
and the function body (that you define) will work accordingly. For now,
this is just a dummy prototype
'''
def function_handler(event):
	
    return [ x + 1 for x in event["p"] ]
       
def main():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	
	try:
		s.bind(('localhost', 45107))
	except Exception as e:
		s.close()
		print(e)
		exit(1)

	while True:
		print('listening on port: {}\n'.format(s.getsockname()[1]))
		
		# receive message from worker
		event, addr = s.recvfrom(1024)
		event = json.loads(event)
		print('received message: {} from {}'.format(event, addr))

		'''
		Once we have received the function input, we need to call that function somehow
		- How do we actually get the function over here?
		'''

		result = function_handler(event)

		response = {
			"Result": result,
			"StatusCode": "200",
		}
		# respond to worker
		s.sendto(json.dumps(response).encode('utf-8'), addr)

	return 0

if __name__ == "__main__":
	main()
