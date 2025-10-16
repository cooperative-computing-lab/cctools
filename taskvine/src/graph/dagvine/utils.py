import inspect

def extract_manager_kwargs(kwargs, base_class):
    params = set(inspect.signature(base_class.__init__).parameters)
    super_kwargs = {k: v for k, v in kwargs.items() if k in params}
    leftover_kwargs = {k: v for k, v in kwargs.items() if k not in params}
    return super_kwargs, leftover_kwargs

def apply_tuning(manager, tune_dict):
    for k, v in tune_dict.items():
        try:
            vine_param = k.replace("_", "-")
            manager.tune(vine_param, v)
            print(f"Tuned {vine_param} to {v}")
        except Exception as e:
            print(f"Failed to tune {k}={v}: {e}")
            raise
