import graphviz


def render_dot_file(dot_file_path, output_file_path, output_format='png'):
    """
    Renders a DOT file and saves it as an image.

    :param dot_file_path: Path to the input DOT file.
    :param output_file_path: Path to the output image file.
    :param output_format: Format of the output image (default is 'png').
    """
    # Read the content of the DOT file
    with open(dot_file_path, 'r') as file:
        dot_content = file.read()

    # Create a Graphviz source object
    dot_graph = graphviz.Source(dot_content)

    # Render the DOT file and save it as an image
    dot_graph.render(filename=output_file_path, format=output_format, cleanup=True)


if __name__ == "__main__":
    # Example usage
    input_dot_file = "shepherd-state-machine.dot"  # Replace with your DOT file path
    output_image_file = "shepherd-state-machine"  # Output file path without extension

    render_dot_file(input_dot_file, output_image_file, output_format='svg')
