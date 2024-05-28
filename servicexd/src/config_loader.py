import os

import yaml


def load_and_preprocess_config(filepath):
    """Loads and preprocesses configuration from a YAML file."""
    with open(filepath, 'r') as file:
        config = yaml.safe_load(file)
    preprocess_config(config)  # Preprocess to auto-fill log and error file paths
    print(f"DEBUG: Loaded and preprocessed config from {filepath}")
    return config


def preprocess_config(config):
    """Automatically fills in missing stdout_path and stderr_path paths."""
    services = config.get('services', {})
    log_dir = config.get('output', {}).get('log_dir', '')

    for service_name, details in services.items():
        # Auto-fill log and error files if not specified
        if 'stdout_path' not in details:
            details['stdout_path'] = f"{service_name}_stdout.log"
        if 'stderr_path' not in details:
            details['stderr_path'] = f"{service_name}_stderr.log"

        if log_dir:
            details['stdout_path'] = os.path.join(log_dir, details['stdout_path'])
            details['stderr_path'] = os.path.join(log_dir, details['stderr_path'])


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
        if 'stdout_path' not in details:
            raise ValueError(f"Program {service} is missing the 'stdout_path' key")

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
