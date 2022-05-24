'''
A work queue worker coprocess should at a minimum define a get_name() function.
'''
def name():
    return "my_coprocess_example"

'''
The function signatures should always be the same, but what is actually
contained in the event will be different. You decide what is in the event, and
the function body (that you define) will work accordingly.
'''
def my_sum(event, response):
	result = int(event["a"]) + int(event["b"])
	response["Result"] = result
	response["StatusCode"] = 200

def my_multiplication(event, response):
	result = int(event["a"]) * int(event["b"])
	response["Result"] = result
	response["StatusCode"] = 200


if __name__ == "__main__":
    # Run workers as:
    # work_queue_worker --coprocess ./network_function.py localhost 9123
    # For tests:
    # python coprocess_example.py

    import json

    # Ensure we are testing the most recent source code
    from importlib.machinery import SourceFileLoader
    import site

    site.addsitedir("bindings/python3")
    wq = SourceFileLoader("wq", "bindings/python3/work_queue.py").load_module()
    wq.set_debug_flag("all")

    event = json.dumps({"a": 2, "b": 3})

    q = wq.WorkQueue(9123)

    t = wq.Task("my_sum");
    t.specify_coprocess("my_coprocess_example")
    t.specify_buffer(event, "infile")
    q.submit(t)

    t = wq.Task("my_multiplication");
    t.specify_coprocess("my_coprocess_example")
    t.specify_buffer(event, "infile")
    q.submit(t)

    while not q.empty():
        t = q.wait(5)
        if t:
            print(f"task {t.id} done. status: {t.result} exit code: {t.return_status}.\noutput:\n{t.output}")


