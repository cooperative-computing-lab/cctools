from dataclasses import dataclass, field
from ndcctools.taskvine.utils import get_c_constant


@dataclass
class ManagerTuningParams:
    """These parameters are used to tune the manager at the C level 
    and should comply with the TaskVine manager API."""
    worker_source_max_transfers: int = 100
    max_retrievals: int = -1
    prefer_dispatch: int = 1
    transient_error_interval: int = 1
    attempt_schedule_depth: int = 10000
    temp_replica_count: int = 1
    enforce_worker_eviction_interval: int = -1
    balance_worker_disk_load: int = 0

    def update(self, params: dict):
        """Update configuration from a dict.
        - Converts '-' to '_' in all keys.
        - Creates new attributes if they don't exist.
        """
        if not isinstance(params, dict):
            raise TypeError(f"update() expects a dict, got {type(params).__name__}")

        for k, v in params.items():
            normalized_key = k.replace("-", "_")
            setattr(self, normalized_key, v)
        return self

    def to_dict(self):
        """Convert all current attributes (including dynamically added ones)
        to a dict, replacing '_' with '-'.
        """
        return {k.replace("_", "-"): v for k, v in self.__dict__.items()}


@dataclass
class VineConstantParams:
    """
    All attributes are accessed in lower case for the convenience of the users.
    If there is a need to use these values in the C code, convert them to uppercase and call the get_c_constant_of method to get the C constant.
    """

    schedule: str = "worst"
    task_priority_mode: str = "largest-input-first"

    valid_normalized_values = {
        "schedule": {"files", "time", "rand", "worst", "disk"},
        "task_priority_mode": {"random", "depth-first", "breadth-first", "fifo", "lifo", "largest-input-first", "largest-storage-footprint-first"},
    }

    def normalize(self, obj):
        """Normalize a string by converting '-' to '_' and uppercase the string."""
        return obj.replace("-", "_").lower()

    def update(self, params: dict):
        """Update configuration from a dict.
        - Converts '-' to '_' in all keys and values, and uppercase the values.
        - Creates new attributes if they don't exist.
        """
        if not isinstance(params, dict):
            raise TypeError(f"update() expects a dict, got {type(params).__name__}")

        for k, v in params.items():
            normalized_key = self.normalize(k)
            normalized_value = self.normalize(v)
            assert normalized_key in self.valid_normalized_values, f"Invalid key: {normalized_key}"
            assert normalized_value in self.valid_normalized_values[normalized_key], f"Invalid value: {normalized_value} for key: {normalized_key}"
            setattr(self, normalized_key, normalized_value)
        return self

    def get_c_constant_of(self, key):
        """Get the C constant of a key."""
        normalized_key = self.normalize(key)
        if normalized_key not in self.valid_normalized_values:
            raise ValueError(f"Invalid key: {normalized_key}")
        return get_c_constant(f"{normalized_key.upper()}_{getattr(self, normalized_key).upper()}")


@dataclass
class RegularParams:
    """Regular parameters that will be used directly by the graph executor."""
    libcores: int = 16
    failure_injection_step_percent: int = -1
    prune_depth: int = 1
    staging_dir: str = "./staging"
    shared_file_system_dir: str = "./shared_file_system"
    extra_task_output_size_mb: list[str, float, float] = field(default_factory=lambda: ["uniform", 0, 0])
    extra_task_sleep_time: list[str, float, float] = field(default_factory=lambda: ["uniform", 0, 0])
    outfile_type: dict[str, float] = field(default_factory=lambda: {
        "temp": 1.0,
        "shared-file-system": 0.0,
    })

    def update(self, params: dict):
        """Update configuration from a dict.
        - Convert '-' in keys to '_', values are as is.
        """
        if not isinstance(params, dict):
            raise TypeError(f"update() expects a dict, got {type(params).__name__}")

        for k, v in params.items():
            normalized_key = k.replace("-", "_")
            if normalized_key not in self.__dict__.keys():
                raise ValueError(f"Invalid param key: {normalized_key}")

            setattr(self, normalized_key, v)
        return self
