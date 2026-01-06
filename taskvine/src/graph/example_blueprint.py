"""Example blueprint graph using ReturnRef and file edges."""

try:
    from .high_level import BlueprintGraph
except ImportError:  # Allow running as a standalone script
    import os
    import sys

    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    if CURRENT_DIR not in sys.path:
        sys.path.insert(0, CURRENT_DIR)
    from high_level import BlueprintGraph  # type: ignore


def func_1(a, b, c):
    with open("file1.txt", "w") as f:
        f.write("Hello, world!")

    with open("file2.txt", "w") as f:
        f.write("Goodbye, world!")

    return a + b + c


def func_2(number):
    with open("file1.txt", "r") as f:
        contents1 = f.read()

    with open("file2.txt", "r") as f:
        contents2 = f.read()

    return f"{contents1} {contents2} {number}"


def build_blueprint_graph():
    g = BlueprintGraph()

    f1 = g.define_file("file1.txt", kind="intermediate")
    f2 = g.define_file("file2.txt", kind="intermediate")

    t1 = g.task(func_1, 1, 2, 3, name="task_a").produces(f1, f2)
    t2 = g.task(func_2, t1.ret(), name="task_b").consumes(f1, f2)

    return g

def describe_graph(graph):
    description = graph.describe()
    print("Files:", {name: meta for name, meta in description["files"].items()})
    for name, info in description["tasks"].items():
        print(f"Node {name}:")
        args = ", ".join(_format_arg(a) for a in info.get("args", []))
        kwargs = ", ".join(f"{k}={_format_arg(v)}" for k, v in info.get("kwargs", {}).items())
        produces = ", ".join(_format_arg(f) for f in info.get("produces", []))
        consumes = ", ".join(_format_arg(f) for f in info.get("consumes", []))
        print(f"  args     : {args or '-'}")
        print(f"  kwargs   : {kwargs or '-'}")
        print(f"  produces : {produces or '-'}")
        print(f"  consumes : {consumes or '-'}")
    print("Return links:", description["return_links"])
    print("File links  :", description["file_links"])


def _format_arg(value):
    if isinstance(value, dict) and value.get("type") == "return_ref":
        return f"ReturnRef({value['node']})"
    if isinstance(value, list):
        return "[" + ", ".join(_format_arg(v) for v in value) + "]"
    if isinstance(value, set):
        return "{" + ", ".join(_format_arg(v) for v in sorted(value, key=str)) + "}"
    if isinstance(value, dict):
        inner = ", ".join(f"{k}: {_format_arg(v)}" for k, v in value.items())
        return "{" + inner + "}"
    return str(value)


if __name__ == "__main__":
    graph = build_blueprint_graph()
    describe_graph(graph)


