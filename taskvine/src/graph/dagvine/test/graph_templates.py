import math

from ndcctools.taskvine.dagvine.workflow import TaskOutputRef, Workflow

MERGE_FANIN = 32  # cap add() fan-in for wide merges


def add(*args):
    return sum(args)


def _merge(bg, keys, prefix="M"):
    if len(keys) == 1:
        return keys[0]
    cur = list(keys)
    level = idx = 0
    while len(cur) > 1:
        nxt = []
        for i in range(0, len(cur), MERGE_FANIN):
            grp = cur[i : i + MERGE_FANIN]
            if len(grp) == 1:
                nxt.append(grp[0])
            else:
                k = f"{prefix}{level}_{idx}"
                idx += 1
                bg.add_task(k, add, *[TaskOutputRef(x) for x in grp])
                nxt.append(k)
        cur = nxt
        level += 1
    return cur[0]


def make_simple_graph():
    bg = Workflow()
    bg.add_task("t0", add, 1, 5)
    bg.add_task("output", add, TaskOutputRef("t0"))
    return bg


def make_chain_graph(chain_len=1, branches=1):
    chain_len = max(1, int(chain_len))
    branches = max(1, int(branches))
    bg = Workflow()
    finals = []
    for b in range(branches):
        head = f"C{b}_0"
        bg.add_task(head, add, 1)
        prev = head
        for i in range(1, chain_len):
            node = f"C{b}_{i}"
            bg.add_task(node, add, TaskOutputRef(prev))
            prev = node
        finals.append(prev)
    root = finals[0] if len(finals) == 1 else _merge(bg, finals, "CH_")
    bg.add_task("output", add, TaskOutputRef(root))
    return bg


def make_chain_rich(n=1):
    n = max(1, int(n))
    bg = Workflow()
    if n == 1:
        bg.add_task("output", add, 1)
        return bg
    branch_count = min(MERGE_FANIN, max(1, (n - 1) // 8))
    base, extra = divmod(n - 1, branch_count)
    finals = []
    for b in range(branch_count):
        size = base + (1 if b < extra else 0)
        k = f"B{b}_0"
        bg.add_task(k, add, 1)
        prev = k
        for i in range(1, size):
            nk = f"B{b}_{i}"
            bg.add_task(nk, add, TaskOutputRef(prev))
            prev = nk
        finals.append(prev)
    bg.add_task("output", add, TaskOutputRef(_merge(bg, finals, "CR_")))
    return bg


def make_individuals(n=1):
    n = max(1, int(n))
    bg = Workflow()
    keys = [f"I{i}" for i in range(n)]
    for k in keys:
        bg.add_task(k, add, 1)
    bg.add_task("output", add, TaskOutputRef(_merge(bg, keys, "IND_")))
    return bg


def make_trivial(n=1):
    bg = Workflow()
    for i in range(max(1, int(n))):
        bg.add_task(f"T{i}", add, 1)
    return bg


def make_binary_tree(n=1):
    n = max(1, int(n))
    bg = Workflow()
    last = (n - 2) // 2
    for i in range(last + 1, n):
        bg.add_task(f"BT{i}", add, 1)
    for i in range(last, -1, -1):
        deps = [TaskOutputRef(f"BT{2 * i + 1}")]
        if 2 * i + 2 < n:
            deps.append(TaskOutputRef(f"BT{2 * i + 2}"))
        bg.add_task(f"BT{i}", add, *deps)
    return bg


def make_binary_forest(n=None, *, branches=5, level=8):
    if n is not None:
        n = max(1, int(n))
        branches = max(1, min(n, MERGE_FANIN))
        level = max(1, math.ceil(math.log2(max(1, n // branches) + 1)))
    else:
        branches, level = max(1, branches), max(1, level)

    bg = Workflow()
    finals = []
    for b in range(branches):
        leaves = [f"F{b}_L{i}" for i in range(2 ** (level - 1))]
        for leaf in leaves:
            bg.add_task(leaf, add, 1)
        cur = leaves
        for d in range(level - 1):
            nxt = []
            for i in range(0, len(cur), 2):
                p = f"F{b}_N{d}_{i // 2}"
                bg.add_task(p, add, TaskOutputRef(cur[i]), TaskOutputRef(cur[i + 1]))
                nxt.append(p)
            cur = nxt
        finals.append(cur[0])
    bg.add_task("output", add, TaskOutputRef(_merge(bg, finals, "BF_")))
    return bg


def build(name, n=None):
    if name == "simple":
        return make_simple_graph()
    if name == "chain":
        return make_chain_graph(max(1, n or 8))
    if name == "chain-branches":
        return make_chain_graph(max(1, n or 8), branches=4)
    if name == "chain-rich":
        return make_chain_rich(max(1, n or 1000))
    if name == "binary-forest":
        return make_binary_forest(n)
    if name == "individuals":
        return make_individuals(max(1, n or 1000))
    if name == "trivial":
        return make_trivial(max(1, n or 1000))
    if name == "binary-tree":
        return make_binary_tree(max(1, n or 1000))
    raise ValueError(name)
