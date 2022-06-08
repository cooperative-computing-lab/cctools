'''
A work queue worker coprocess should at a minimum define a get_name() function.
'''
import json

def name():
    return "my_coprocess_example"

def remote_execute(func):
    def remote_wrapper(event, q=None):
        if q:
            event = json.loads(event)
            del event["exec_method"]
        try:
            response = {
                "Result": func(**event),
                "StatusCode": 200
            }
        except Exception as e:
            response = { 
                "Result": str(e),
                "StatusCode": 500 
            }
        if not q:
            return response
        q.put(response)
    return remote_wrapper


'''
The function signatures should always be the same, but what is actually
contained in the event will be different. You decide what is in the event, and
the function body (that you define) will work accordingly.
'''
@remote_execute
def my_sum(a, b):
	return a + b

@remote_execute
def my_multiplication(a, b):
	return a * b

if __name__ == "__main__":
    # Run workers as:
    # work_queue_worker --coprocess ./network_function.py localhost 9123
    # For tests:
    # python coprocess_example.py

    # Ensure we are testing the most recent source code
    from importlib.machinery import SourceFileLoader
    import site

    site.addsitedir("bindings/python3")
    wq = SourceFileLoader("wq", "bindings/python3/work_queue.py").load_module()
    wq.set_debug_flag("all")

    q = wq.WorkQueue(9123)

    t = wq.RemoteTask("my_sum", "my_coprocess_example", a=2, b=3, exec_method="fork")
    q.submit(t)

    t = wq.RemoteTask("my_multiplication", "my_coprocess_example", a=2, b=3, exec_method="thread")
    q.submit(t)

    while not q.empty():
        t = q.wait(5)
        if t:
            print(f"task {t.id} done. status: {t.result} exit code: {t.return_status}.\noutput:\n{t.output}")


