def handler(event, response):
	result = int(event["a"]) + int(event["b"])
	response["Result"] = result
	response["StatusCode"] = 200

def get_name():
	return "my_func"
