import hashlib


def _legacy_subgraph_key(*parts):
    """Stable short key for expanded legacy Dask subgraph tasks."""
    return hashlib.sha256("".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:20]


def _rewrite_subgraph_expr(dsk_key, inner_dsk, expr, blockwise_args):
    try:
        if expr in inner_dsk:
            return _legacy_subgraph_key(dsk_key, expr)
    except Exception:
        pass

    if hasattr(expr, "item") and callable(expr.item):
        try:
            item = expr.item()
            if item in inner_dsk:
                return _legacy_subgraph_key(dsk_key, item)
        except Exception:
            pass

    if isinstance(expr, list):
        return [_rewrite_subgraph_expr(dsk_key, inner_dsk, item, blockwise_args) for item in expr]
    if isinstance(expr, tuple):
        return tuple(_rewrite_subgraph_expr(dsk_key, inner_dsk, item, blockwise_args) for item in expr)
    if isinstance(expr, str) and expr.startswith("__dask_blockwise__"):
        return blockwise_args[int(expr.split("__")[-1])]
    return expr


def expand_legacy_subgraph_dsk(task_dict, dask_module):
    """Inline legacy ``SubgraphCallable`` layers into a flat dsk."""
    if not task_dict or dask_module is None:
        return task_dict

    try:
        from dask.optimization import SubgraphCallable
    except ImportError:
        return task_dict

    expanded = {}
    for key, sexpr in task_dict.items():
        if not isinstance(sexpr, (tuple, list)) or not sexpr:
            expanded[key] = sexpr
            continue

        head = sexpr[0]
        tail = sexpr[1:]
        if isinstance(head, SubgraphCallable):
            expanded[key] = _legacy_subgraph_key(key, head.outkey)
            for sub_key, sub_sexpr in head.dsk.items():
                rewritten_key = _legacy_subgraph_key(key, sub_key)
                expanded[rewritten_key] = _rewrite_subgraph_expr(key, head.dsk, sub_sexpr, tail)
        elif callable(head):
            expanded[key] = sexpr
        else:
            raise TypeError(
                f"Legacy dsk task {key!r} has non-callable head {type(head).__name__}; "
                "expected SubgraphCallable or a callable."
            )

    return expanded
