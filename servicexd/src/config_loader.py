import sys

import yaml


def load_config_from_yaml(path):
    with open(path, 'r') as f:
        config = yaml.safe_load(f)
        print(f"DEBUG: Loaded config from {path}")
        return config


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
    graph = {program: details.get('depends_on', {}) for program, details in programs.items()}

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
