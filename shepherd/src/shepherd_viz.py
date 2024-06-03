from graphviz import Digraph, Source


def generate_dot(config):
    dot = Digraph(comment='Workflow Visualization')

    # Add nodes and edges
    for service, details in config['services'].items():
        dot.node(service, service, shape='box', style='filled', color='lightblue')
        dependencies = details.get('dependency', {}).get('items', {})
        for dep, state in dependencies.items():
            dot.edge(dep, service, label=state, color='grey')

    return dot.source


def render_dot(dot_source, output_filename='workflow_visualization'):
    src = Source(dot_source)
    src.format = 'png'
    src.render(output_filename)  # 'view=True' opens the image automatically


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

dot = generate_dot(config)
print(dot)
render_dot(dot)