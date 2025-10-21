from ndcctools.taskvine.vinedag import capi


class StrategicOrchestrationGraph:
    def __init__(self, c_taskvine):
        self._c_sog = capi.sog_create(c_taskvine)

    def tune(self, name, value):
        capi.sog_tune(self._c_sog, name, value)

    def add_node(self, key, is_target_key):
        capi.sog_add_node(self._c_sog, key, is_target_key)

    def add_dependency(self, parent_key, child_key):
        capi.sog_add_dependency(self._c_sog, parent_key, child_key)

    def compute_topology_metrics(self):
        capi.sog_compute_topology_metrics(self._c_sog)

    def get_node_outfile_remote_name(self, key):
        return capi.sog_get_node_outfile_remote_name(self._c_sog, key)

    def get_proxy_library_name(self):
        return capi.sog_get_proxy_library_name(self._c_sog)

    def set_proxy_function(self, proxy_function):
        capi.sog_set_proxy_function_name(self._c_sog, proxy_function.__name__)

    def execute(self):
        capi.sog_execute(self._c_sog)

    def delete(self):
        capi.sog_delete(self._c_sog)
