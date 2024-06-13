import sys
import json
from graphviz import Digraph, Source


def generate_dot(config, state_transitions):
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
            states = state_transitions.get(service, {})

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
            dot.edge(f'{dep}_{state}', f'{service}_initialized', label=f'{dep} {state}')

    return dot.source


def render_dot(dot_source, output_filename='workflow_visualization'):
    src = Source(dot_source)
    src.format = 'png'
    src.render(output_filename, view=True)


if __name__ == '__main__':
    config = {
        'services': {
            'program1': {
                'command': './program1.sh',
                'state': {
                    'log': {
                        'ready': 'program is ready',
                        'complete': 'program is completed'
                    }
                }
            },
            'program2': {
                'command': './program2.sh',
                'state': {
                    'log': {
                        'ready': 'program is ready',
                        'complete': 'program is completed'
                    }
                },
                'dependency': {
                    'mode': 'all',
                    'items': {
                        'program1': 'ready',
                        'program3': 'complete'
                    }
                }
            },
            'program3': {
                'command': './program3.sh',
                'state': {
                    'log': {
                        'ready': 'program is ready',
                        'complete': 'program is completed'
                    }
                }
            }
        },
        'output': {
            'state_times': 'state_transition_times.json'
        },
        'max_run_time': 120
    }

    state_transitions = {
        "program1": {
            "initialized": 0.2520601749420166,
            "started": 0.2529749870300293,
            "ready": 5.447847843170166,
            "complete": 36.73489499092102,
            "success": 36.781131982803345,
            "final": 36.781386852264404
        },
        "program3": {
            "initialized": 0.252730131149292,
            "started": 0.25317811965942383,
            "ready": 5.451045989990234,
            "complete": 36.72722291946411,
            "success": 36.80730319023132,
            "final": 36.80773401260376
        },
        "program2": {
            "initialized": 0.25133585929870605,
            "started": 36.72884702682495,
            "ready": 42.47040295600891,
            "complete": 73.59736275672913,
            "success": 73.61198306083679,
            "final": 73.612459897995
        }
    }

    dot = generate_dot(config, state_transitions)
    print(dot)
    render_dot(dot)
