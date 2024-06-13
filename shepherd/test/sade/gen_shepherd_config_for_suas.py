import sys
from jinja2 import Environment, FileSystemLoader

def generate_config(template_file, output_file, num_px4_instances):
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_file)

    config = template.render(num_px4_instances=num_px4_instances)

    with open(output_file, 'w') as f:
        f.write(config)

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print("Usage: python generate_shepherd_config.py <template_file> <output_file> <num_px4_instances>")
        sys.exit(1)

    template_file = sys.argv[1]
    output_file = sys.argv[2]
    num_px4_instances = int(sys.argv[3])

    generate_config(template_file, output_file, num_px4_instances)

