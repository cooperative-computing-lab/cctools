from ndcctools.poncho.library_network_code import library_network_code
import inspect
rsc = inspect.getsource(library_network_code)
nc = "\n".join([line[4:] for line in rsc.split("\n")[1:]])
print(nc)
