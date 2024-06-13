import json
import sys

from graphviz import Digraph, Source


def generate_dot(config):
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

    return dot.source


def render_dot(dot_source, output_filename='workflow_visualization'):
    # Todo: try optional unflatten method
    src = Source(dot_source)
    src.format = 'svg'
    src.render(output_filename, view=True)


config = {
    'services': {
        'reserve_port': {

        },
        'chmod_port_config': {
            'dependency': {'items': {'reserve_port': 'final'}}
        },
        'copy_ports_config': {
            'dependency': {'items': {'chmod_port_config': 'final'}}
        },
        'gazebo_server': {
            'dependency': {'items': {'chmod_port_config': 'final'}}
        },
        'px4_instance_0': {
            'dependency': {'items': {'gazebo_server': 'ready'}}
        },
        'spawn_model_0': {
            'dependency': {'items': {'px4_instance_0': 'waiting_for_simulator'}}
        },
        'pose_sender': {
            'dependency': {'items': {'px4_instance_0': 'ready'}}
        }
    }
}

if __name__ == '__main__':
    from config_loader import load_and_preprocess_config

    if len(sys.argv) > 1:
        config = load_and_preprocess_config(sys.argv[1])
    else:
        config = load_and_preprocess_config('sade-config.yml')

    json.dump(config, sys.stdout, indent=2)


    dot = generate_dot(config)
    print(dot)
    render_dot(dot)
