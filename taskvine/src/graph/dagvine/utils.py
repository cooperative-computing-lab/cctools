import cloudpickle
import os


def context_loader_func(graph_pkl):
    graph = cloudpickle.loads(graph_pkl)
    return {"graph": graph}


def delete_all_files(root_dir):
    """Remove files under the run-info template directory."""
    if not os.path.exists(root_dir):
        return
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                os.remove(file_path)
            except FileNotFoundError:
                print(f"Failed to delete file {file_path}")


def color_text(text, color_code):
    """Return text wrapped in an ANSI color code."""
    return f"\033[{color_code}m{text}\033[0m"
