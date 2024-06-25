#!/usr/bin/env python3
import os

from config_loader import load_and_preprocess_config

import argparse
import json
import sys
from graphviz import Digraph, Source


def load_json_from_file(filename):
    if filename is None or not os.path.exists(filename):
        return None

    with open(filename) as f:
        return json.load(f)


def render_dot(dot_source, output_filename='workflow_visualization', output_format='png'):
    # Todo: try optional unflatten method
    src = Source(dot_source)
    src.format = output_format
    src.render(output_filename)


def generate_state_times_graph(state_transition, output_prefix, output_format='png'):
    pass


def generate_state_transition_graph(config, state_transition, output_prefix, output_format='png'):
    output_filename = output_prefix + '_workflow_transition'

    dot = Digraph(comment='Workflow Visualization')

    colors = {
        'service': 'lightblue',
        'action': 'lightblue',
    }

    # Add subgraphs for each service with their state transitions
    for service, details in config['services'].items():
        service_type = details.get('type', 'action')  # Default to 'action' if type is not specified
        node_color = colors.get(service_type, 'lightgrey')  # Default color if no specific type is found

        with dot.subgraph(name=f'cluster_{service}') as sub:
            sub.attr(style='filled', color='lightgrey')
            sub.node_attr.update(style='filled', color=node_color)
            states = state_transition.get(service, {})

            # Add nodes for each state
            for state, time in states.items():
                sub.node(f'{service}_{state}', f'{state}\n{time:.2f}s')

            # Add edges between states
            state_list = list(states.keys())
            for i in range(len(state_list) - 1):
                sub.edge(f'{service}_{state_list[i]}', f'{service}_{state_list[i + 1]}')

            sub.attr(label=service)

    # Add edges based on dependencies
    for service, details in config['services'].items():
        dependencies = details.get('dependency', {}).get('items', {})
        for dep, state in dependencies.items():
            dot.edge(f'{dep}_{state}', f'{service}_started', label=f'{dep} {state}')

    return render_dot(dot.source, output_filename, output_format)


def generate_workflow_graph(config, output_prefix, output_format='png'):
    output_filename = output_prefix + '_workflow'

    dot = Digraph(comment='Workflow Visualization')

    colors = {
        'service': 'lightblue',
        'action': 'lightgreen',
    }

    # Add nodes with color based on service type
    for service, details in config['services'].items():
        service_type = details.get('type', 'action')  # Default to 'service' if type is not specified
        node_color = colors.get(service_type, 'lightgrey')  # Default color if no specific type is found
        dot.node(service, service, shape='box', style='filled', color=node_color)

        dependencies = details.get('dependency', {}).get('items', {})
        for dep, state in dependencies.items():
            edge_color = colors.get(state, '')
            dot.edge(dep, service, label=state, color=edge_color, fontcolor=edge_color)

    return render_dot(dot.source, output_filename, output_format)


def main():
    parser = argparse.ArgumentParser(description='Generate visualizations for Shepherd workflow manger.')
    parser.add_argument('--config', '-c', type=str, help='Path to the program config YAML file')
    parser.add_argument('--state_transition', '-s', type=str, help='Path to the state transition JSON file')
    parser.add_argument('--output_prefix', '-p', type=str, default='shepherd',
                        help='Output filename prefix for the visualization')
    parser.add_argument('--output_format', '-f', type=str, default='png',
                        help='Output format for the visualization (e.g., svg, png)')

    args = parser.parse_args()

    config = load_and_preprocess_config(args.config)
    state_transition = load_json_from_file(args.state_transition)
    output_prefix = args.output_prefix
    output_format = args.output_format

    if config is not None:
        generate_workflow_graph(config, output_prefix, output_format)
    if state_transition is not None:
        generate_state_times_graph(state_transition, output_prefix, output_format)
    if config is not None and state_transition is not None:
        generate_state_transition_graph(config, state_transition, output_prefix, output_format)


if __name__ == '__main__':
    main()
