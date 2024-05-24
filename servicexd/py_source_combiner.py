import os
import re
import sys


def extract_imports(file_content, source_dir):
    """Extract import statements from the file content."""
    imports = []
    other_lines = []
    import_pattern = re.compile(r'^\s*(import|from)\s+')
    module_names = [f[:-3] for f in os.listdir(source_dir) if f.endswith('.py')]

    for line in file_content.splitlines():
        if import_pattern.match(line):
            # Check if it's an internal import
            is_internal = any(f"import {name}" in line or f"from {name}" in line for name in module_names)
            if not is_internal:
                imports.append(line)
        else:
            other_lines.append(line)

    return imports, other_lines


def combine_files(source_dir, output_file):
    all_imports = set()
    combined_code = []
    main_blocks = []

    for root, _, files in os.walk(source_dir):
        for file in sorted(files):
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                with open(filepath) as infile:
                    file_content = infile.read()
                    imports, code = extract_imports(file_content, source_dir)
                    all_imports.update(imports)

                    code_blocks = ""
                    inside_main = False

                    for line in code:
                        if line.strip().startswith("if __name__"):
                            inside_main = True
                        if inside_main:
                            main_blocks.append(line)
                        else:
                            code_blocks += line + "\n"

                    combined_code.append(f"# --- {file} ---\n")
                    combined_code.append(code_blocks.strip())
                    combined_code.append("\n")

    with open(output_file, 'w') as outfile:
        for imp in sorted(all_imports):
            outfile.write(f"{imp}\n")
        outfile.write("\n")
        outfile.write("\n".join(combined_code))
        if main_blocks:
            outfile.write("\n")
            outfile.write("# --- Main Execution Block ---\n")
            outfile.write("\n".join(main_blocks))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python py_source_combiner.py <source_directory> <output_file>")
        sys.exit(1)

    source_dir = sys.argv[1]
    output_file = sys.argv[2]

    combine_files(source_dir, output_file)
    print(f"Combined file created: {output_file}")
