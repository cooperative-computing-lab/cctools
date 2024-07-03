import os
import re
import sys

# This is temporary file and should be removed

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


def combine_files(source_dir, output_file, file_list=None, compact=False):
    all_imports = set()
    combined_code = []
    main_blocks = []

    if file_list is None or len(file_list) == 0:
        file_list = os.listdir(source_dir)
        file_list.sort()

    for file in file_list:
        if file.endswith('.py'):
            filepath = os.path.join(source_dir, file)
            with open(filepath) as infile:
                file_content = infile.read()
                imports, code = extract_imports(file_content, source_dir)
                all_imports.update(imports)

                code_blocks = ""
                inside_main = False

                for line in code:
                    if compact and line.strip() == '':
                        continue

                    if line.strip().startswith("if __name__"):
                        inside_main = True
                    if inside_main:
                        main_blocks.append(line)
                    else:
                        code_blocks += line + "\n"

                combined_code.append(f"# --- {file} ---")

                if not compact:
                    combined_code.append("\n")

                combined_code.append(code_blocks.strip())

                if not compact:
                    combined_code.append("\n")

    with open(output_file, 'w') as outfile:
        for imp in sorted(all_imports):
            outfile.write(f"{imp}\n")

        if not compact:
            outfile.write("\n")

        outfile.write("\n".join(combined_code))

        if main_blocks:
            outfile.write("\n")
            outfile.write("# --- Main Execution Block ---\n")
            outfile.write("\n".join(main_blocks))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python py_source_combiner.py <source_directory> <output_file> [--compact] [<file1> <file2>]")
        sys.exit(1)

    source_dir = sys.argv[1]
    output_file = sys.argv[2]

    compact = '--compact' in sys.argv
    file_list = [arg for arg in sys.argv[3:] if arg != '--compact' and not arg.startswith('--')]

    combine_files(source_dir, output_file, file_list, compact)

    print(f"Combined file created: {output_file}")
