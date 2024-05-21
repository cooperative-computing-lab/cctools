import os

def get_all_python_files(src_dir):
    python_files = []
    for root, _, files in os.walk(src_dir):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files

def concatenate_files(python_files, output_file):
    with open(output_file, 'w') as outfile:
        for file in python_files:
            with open(file, 'r') as infile:
                outfile.write(f'# Start of {file}\n')
                outfile.write(infile.read())
                outfile.write(f'# End of {file}\n\n')

if __name__ == "__main__":
    src_dir = 'src'
    output_file = "servicexd.py"
    python_files = get_all_python_files(src_dir)
    concatenate_files(python_files, output_file)
    print(f"All Python files have been concatenated into {output_file}")
