import yaml


def load_and_preprocess_config(filepath):
    """Loads and preprocesses configuration from a YAML file."""
    with open(filepath, 'r') as file:
        config = yaml.safe_load(file)
    preprocess_config(config)  # Preprocess to auto-fill log and error file paths
    print(f"DEBUG: Loaded and preprocessed config from {filepath}")
    return config


def preprocess_config(config):
    """Automatically fills in missing log_file and error_file paths."""
    services = config.get('services', {})
    for service_name, details in services.items():
        # Auto-fill log and error files if not specified
        if 'log_file' not in details:
            details['log_file'] = f"{service_name}_stdout.log"
        if 'error_file' not in details:
            details['error_file'] = f"{service_name}_stderr.log"


def validate_and_sort_programs(config):
    print("DEBUG: Validating and sorting programs")
    required_keys = ['services']

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required key: {key}")

    services = config['services']

    for service, details in services.items():
        if 'command' not in details:
            raise ValueError(f"Program {service} is missing the 'command' key")
        if 'log_file' not in details:
            raise ValueError(f"Program {service} is missing the 'log_file' key")

    sorted_services = topological_sort(services)
    print(f"DEBUG: Sorted services: {sorted_services}")
    return sorted_services


def topological_sort(programs):
    print("DEBUG: Performing topological sort")

    graph = {program: details.get('dependency', {}).get('items', {}) for program, details in programs.items()}

    visited = set()
    visiting = set()
    stack = []

    def dfs(node):
        if node in visiting:
            raise ValueError(f"Cyclic dependency on {node}")

        visiting.add(node)

        if node not in visited:
            visited.add(node)
            for neighbor in graph[node]:
                dfs(neighbor)
            stack.append(node)

        visiting.remove(node)

    for program in graph:
        dfs(program)
    print(f"DEBUG: Topological sort result: {stack}")
    return stack

# Example usage:
# config = load_config_from_yaml('config.yml')
# sorted_programs = validate_and_sort_programs(config)
