import importlib
import importlib.util
import pathlib
import sys
import unittest
from dataclasses import dataclass, field

from ndcctools.taskvine.dagvine.blueprint_graph.adaptor import Adaptor as _Adaptor, TaskOutputRef as _TaskOutputRef

_MODULE_NAME = "ndcctools.taskvine.dagvine.blueprint_graph.adaptor"
_LOCAL_ADAPTOR = pathlib.Path(__file__).resolve().parent / "adaptor.py"

if _LOCAL_ADAPTOR.exists():
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _LOCAL_ADAPTOR)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    Adaptor = module.Adaptor
    TaskOutputRef = module.TaskOutputRef
    adaptor_impl = module
else:
    Adaptor = _Adaptor
    TaskOutputRef = _TaskOutputRef
    adaptor_impl = importlib.import_module(Adaptor.__module__)


class AdaptorSexprTests(unittest.TestCase):
    def test_empty_graph_returns_empty_dict(self):
        self.assertEqual(Adaptor({}).task_dict, {})

    def test_wraps_references_in_args_and_kwargs(self):
        def seed():
            return 1

        def consume(value, bonus=0):
            return value + bonus

        graph = {
            "seed": (seed,),
            "consumer": (consume, "seed", {"bonus": "seed"}),
        }

        adaptor = Adaptor(graph)
        adapted = adaptor.task_dict

        seed_func, seed_args, seed_kwargs = adapted["seed"]
        self.assertIs(seed_func, seed)
        self.assertEqual(seed_args, ())
        self.assertEqual(seed_kwargs, {})

        func, args, kwargs = adapted["consumer"]
        self.assertIs(func, consume)
        self.assertEqual(len(args), 1)
        self.assertIsInstance(args[0], TaskOutputRef)
        self.assertEqual(args[0].task_key, "seed")
        self.assertEqual(args[0].path, ())

        self.assertIsInstance(kwargs["bonus"], TaskOutputRef)
        self.assertEqual(kwargs["bonus"].task_key, "seed")
        self.assertEqual(kwargs["bonus"].path, ())

    def test_handles_nested_collections(self):
        def aggregate(structure, *, options=None):
            return structure, options

        graph = {
            "alpha": (lambda: {"value": 1},),
            "beta": (lambda: 2,),
            "collector": (
                aggregate,
                ["alpha", ("beta", "alpha")],
                {
                    "mapping": {
                        "left": "alpha",
                        "right": ["beta", {"deep": "alpha"}],
                    },
                    "flags": {"alpha", "unchanged"},
                },
            ),
        }

        adaptor = Adaptor(graph)
        adapted = adaptor.task_dict

        func, args, kwargs = adapted["collector"]
        self.assertIs(func, aggregate)

        self.assertEqual(len(args), 1)
        structure = args[0]
        self.assertIsInstance(structure, list)
        self.assertIsInstance(structure[0], TaskOutputRef)
        self.assertEqual(structure[0].task_key, "alpha")

        tuple_fragment = structure[1]
        self.assertIsInstance(tuple_fragment, tuple)
        self.assertIsInstance(tuple_fragment[0], TaskOutputRef)
        self.assertEqual(tuple_fragment[0].task_key, "beta")
        self.assertIsInstance(tuple_fragment[1], TaskOutputRef)
        self.assertEqual(tuple_fragment[1].task_key, "alpha")

        mapping = kwargs["mapping"]
        self.assertIsInstance(mapping["left"], TaskOutputRef)
        self.assertEqual(mapping["left"].task_key, "alpha")

        right_list = mapping["right"]
        self.assertIsInstance(right_list[0], TaskOutputRef)
        self.assertEqual(right_list[0].task_key, "beta")
        self.assertIsInstance(right_list[1]["deep"], TaskOutputRef)
        self.assertEqual(right_list[1]["deep"].task_key, "alpha")

        flags = kwargs["flags"]
        self.assertIsInstance(flags, set)
        ref_keys = {item.task_key for item in flags if isinstance(item, TaskOutputRef)}
        self.assertEqual(ref_keys, {"alpha"})
        self.assertIn("unchanged", flags)

    def test_literal_strings_remain_literals(self):
        def attach_unit(value, *, unit):
            return value, unit

        graph = {
            "value": (lambda: 42,),
            "with_unit": (
                attach_unit,
                "value",
                {"unit": "kg"},
            ),
        }

        adaptor = Adaptor(graph)
        adapted = adaptor.task_dict
        func, args, kwargs = adapted["with_unit"]
        self.assertIs(func, attach_unit)
        self.assertEqual(len(args), 1)
        self.assertIsInstance(args[0], TaskOutputRef)
        self.assertEqual(kwargs["unit"], "kg")

    def test_existing_task_output_ref_is_preserved(self):
        original_ref = TaskOutputRef("seed")

        graph = {
            "seed": (lambda: 5,),
            "forward": (lambda x: x, original_ref),
        }

        adapted = Adaptor(graph).task_dict
        func, args, kwargs = adapted["forward"]
        self.assertIs(func, graph["forward"][0])
        self.assertEqual(kwargs, {})
        self.assertIs(args[0], original_ref)

    def test_sets_and_frozensets_are_rewritten(self):
        graph = {
            "seed": (lambda: 1,),
            "consumer": (
                lambda payload, *, meta=None: (payload, meta),
                (
                    {
                        "set_refs": {"seed", "literal"},
                        "froze_refs": frozenset({"seed"}),
                    },
                ),
                {
                    "meta": {
                        "labels": {"seed", "plain"},
                        "deep": frozenset({"seed"}),
                    }
                },
            ),
        }

        adapted = Adaptor(graph).task_dict
        func, args, kwargs = adapted["consumer"]
        self.assertEqual(len(args), 1)

        self.assertIsInstance(args[0], tuple)
        payload = args[0][0]
        set_refs = payload["set_refs"]
        self.assertIsInstance(set_refs, set)
        self.assertIn("literal", set_refs)
        refs = [item for item in set_refs if isinstance(item, TaskOutputRef)]
        self.assertEqual(len(refs), 1)
        self.assertEqual(refs[0].task_key, "seed")

        froze_refs = payload["froze_refs"]
        self.assertIsInstance(froze_refs, frozenset)
        sole_ref = next(iter(froze_refs))
        self.assertIsInstance(sole_ref, TaskOutputRef)
        self.assertEqual(sole_ref.task_key, "seed")

        labels = kwargs["meta"]["labels"]
        self.assertIsInstance(labels, set)
        label_refs = [item for item in labels if isinstance(item, TaskOutputRef)]
        self.assertEqual(len(label_refs), 1)
        self.assertEqual(label_refs[0].task_key, "seed")
        self.assertIn("plain", labels)

        deep_froze = kwargs["meta"]["deep"]
        self.assertIsInstance(deep_froze, frozenset)
        deep_ref = next(iter(deep_froze))
        self.assertIsInstance(deep_ref, TaskOutputRef)
        self.assertEqual(deep_ref.task_key, "seed")

    def test_callable_tuple_preserves_callable(self):
        def source():
            return 2

        def apply(func_tuple):
            fn, value = func_tuple
            return fn(value)

        increment = lambda x: x + 1  # noqa: E731

        graph = {
            "value": (source,),
            "result": (
                apply,
                (increment, "value"),
            ),
        }

        adapted = Adaptor(graph).task_dict
        func, args, kwargs = adapted["result"]
        self.assertIs(func, apply)
        nested = args[0]
        self.assertIsInstance(nested, tuple)
        self.assertIs(nested[0], increment)
        self.assertIsInstance(nested[1], TaskOutputRef)
        self.assertEqual(nested[1].task_key, "value")

    def test_invalid_task_definition_raises(self):
        graph = {"broken": []}
        with self.assertRaises(TypeError):
            Adaptor(graph)

    def test_large_graph_scaling(self):
        size = 500

        graph = {"root": (lambda: 1,)}
        for i in range(1, size + 1):
            key = f"node_{i}"
            prev_key = "root" if i == 1 else f"node_{i - 1}"
            graph[key] = (
                lambda x, inc=1: x + inc,
                prev_key,
                {"inc": i},
            )

        graph["fanout"] = (
            lambda *vals: sum(vals),
            tuple(graph.keys()),
        )

        adapted = Adaptor(graph).task_dict

        self.assertEqual(len(adapted), len(graph))

        fanout_func, fanout_args, fanout_kwargs = adapted["fanout"]
        self.assertEqual(fanout_kwargs, {})
        self.assertEqual(len(fanout_args), 1)
        arg_tuple = fanout_args[0]
        self.assertEqual(len(arg_tuple), len(graph) - 1)
        refs = [item for item in arg_tuple if isinstance(item, TaskOutputRef)]
        self.assertEqual(len(refs), len(graph) - 1)
        ref_keys = {ref.task_key for ref in refs}
        expected_keys = set(graph.keys()) - {"fanout"}
        self.assertEqual(ref_keys, expected_keys)


class _FakeGraphNode:
    __slots__ = ()


@dataclass
class _FakeTaskRef(_FakeGraphNode):
    key: str
    path: tuple = field(default_factory=tuple)


@dataclass
class _FakeAlias(_FakeGraphNode):
    target: str
    path: tuple = field(default_factory=tuple)
    dependencies: frozenset = field(default_factory=frozenset)

    def __post_init__(self):
        if not self.dependencies:
            self.dependencies = frozenset({self.target})


@dataclass
class _FakeLiteral(_FakeGraphNode):
    value: object


@dataclass
class _FakeDataNode(_FakeGraphNode):
    value: object


@dataclass
class _FakeNestedContainer(_FakeGraphNode):
    value: object


@dataclass
class _FakeTask(_FakeGraphNode):
    key: str
    function: object
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    dependencies: frozenset = field(default_factory=frozenset)

    def __post_init__(self):
        if not self.dependencies:
            deps = set()
            for arg in self.args:
                if isinstance(arg, _FakeTaskRef):
                    deps.add(arg.key)
            for value in self.kwargs.values():
                if isinstance(value, _FakeTaskRef):
                    deps.add(value.key)
            self.dependencies = frozenset(deps)


class _FakeDtsModule:
    GraphNode = _FakeGraphNode
    Task = _FakeTask
    TaskRef = _FakeTaskRef
    Alias = _FakeAlias
    Literal = _FakeLiteral
    DataNode = _FakeDataNode
    NestedContainer = _FakeNestedContainer

    @staticmethod
    def convert_legacy_graph(task_dict):
        return task_dict


class AdaptorTaskSpecTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_dts = adaptor_impl.dts
        adaptor_impl.dts = _FakeDtsModule()

    @classmethod
    def tearDownClass(cls):
        adaptor_impl.dts = cls._original_dts

    def test_adapts_taskspec_graph(self):
        def add_bonus(value, bonus):
            return value + bonus

        def combine(upstream, payload=None):
            return upstream, payload

        graph = {
            "raw": _FakeDataNode(value=7),
            "literal_wrapper": _FakeTask(
                key="literal_wrapper",
                function=add_bonus,
                args=(_FakeTaskRef("raw"),),
                kwargs={"bonus": _FakeLiteral(3)},
            ),
            "alias": _FakeAlias(target="literal_wrapper", path=("result",)),
            "aggregate": _FakeTask(
                key="aggregate",
                function=combine,
                args=(_FakeTaskRef("alias", path=("payload",)),),
                kwargs={
                    "payload": _FakeNestedContainer(
                        value=[
                            _FakeTaskRef("raw"),
                            {"inner": _FakeTaskRef("alias", path=("extra",))},
                        ]
                    )
                },
            ),
        }

        adaptor = Adaptor(graph)
        adapted = adaptor.task_dict

        raw_func, raw_args, raw_kwargs = adapted["raw"]
        self.assertEqual(raw_args, (7,))
        self.assertEqual(raw_kwargs, {})
        self.assertEqual(raw_func(raw_args[0]), raw_args[0])

        wrapper_func, wrapper_args, wrapper_kwargs = adapted["literal_wrapper"]
        self.assertIs(wrapper_func, add_bonus)
        self.assertEqual(len(wrapper_args), 1)
        self.assertIsInstance(wrapper_args[0], TaskOutputRef)
        self.assertEqual(wrapper_args[0].task_key, "raw")
        self.assertEqual(wrapper_kwargs["bonus"], 3)

        self.assertIn("alias", adapted)
        alias_func, alias_args, alias_kwargs = adapted["alias"]
        self.assertIs(alias_func, adaptor_impl._identity)
        self.assertEqual(alias_kwargs, {})
        self.assertEqual(len(alias_args), 1)
        alias_input = alias_args[0]
        self.assertIsInstance(alias_input, TaskOutputRef)
        self.assertEqual(alias_input.task_key, "literal_wrapper")
        self.assertEqual(alias_input.path, ("result",))

        agg_func, agg_args, agg_kwargs = adapted["aggregate"]
        self.assertIs(agg_func, combine)
        self.assertEqual(len(agg_args), 1)
        primary_input = agg_args[0]
        self.assertIsInstance(primary_input, TaskOutputRef)
        self.assertEqual(primary_input.task_key, "alias")
        self.assertEqual(primary_input.path, ("payload",))

        payload = agg_kwargs["payload"]
        self.assertIsInstance(payload, list)
        self.assertIsInstance(payload[0], TaskOutputRef)
        self.assertEqual(payload[0].task_key, "raw")
        nested_inner = payload[1]["inner"]
        self.assertIsInstance(nested_inner, TaskOutputRef)
        self.assertEqual(nested_inner.task_key, "alias")
        self.assertEqual(nested_inner.path, ("extra",))

    def test_taskspec_data_node_literal_passthrough(self):
        graph = {"literal": _FakeDataNode(value=11)}
        adapted = Adaptor(graph).task_dict
        func, args, kwargs = adapted["literal"]
        self.assertEqual(args, (11,))
        self.assertEqual(kwargs, {})
        self.assertEqual(func(args[0]), 11)

    def test_taskspec_alias_with_missing_target_raises(self):
        alias = _FakeAlias(target="ghost", dependencies=frozenset())
        alias.dependencies = frozenset()
        graph = {"alias": alias}
        with self.assertRaises(ValueError):
            Adaptor(graph)

    def test_taskspec_nested_container_fallback_to_data(self):
        container = _FakeNestedContainer(value=None)
        container.data = [_FakeTaskRef("raw")]
        graph = {
            "raw": _FakeDataNode(value=5),
            "use_container": _FakeTask(
                key="use_container",
                function=lambda payload: payload,
                args=(container,),
            ),
        }

        adapted = Adaptor(graph).task_dict
        func, args, kwargs = adapted["use_container"]
        self.assertEqual(kwargs, {})
        (payload,) = args
        self.assertIsInstance(payload, list)
        self.assertIsInstance(payload[0], TaskOutputRef)
        self.assertEqual(payload[0].task_key, "raw")

    def test_taskspec_task_missing_function_raises(self):
        graph = {
            "broken": _FakeTask(
                key="broken",
                function=None,
                args=(),
            )
        }
        with self.assertRaises(TypeError):
            Adaptor(graph)

    def test_taskspec_task_nested_inside_args_is_lifted(self):
        # Inline Tasks inside args/kwargs should be lifted unless they are a top-level key
        # reference or a pure value constructor. Here `inner` is an inline task with
        # an unknown (lambda) op, so it must be lifted to a new node.
        inner = _FakeTask(
            key=None,
            function=lambda: 1,
            args=(),
        )
        graph = {
            "outer": _FakeTask(
                key="outer",
                function=lambda x: x,
                args=(inner,),
            )
        }
        adapted = Adaptor(graph).task_dict
        outer_func, outer_args, outer_kwargs = adapted["outer"]
        self.assertEqual(outer_kwargs, {})
        self.assertEqual(len(outer_args), 1)
        self.assertIsInstance(outer_args[0], TaskOutputRef)
        lifted_key = outer_args[0].task_key
        self.assertIn(lifted_key, adapted)
        self.assertNotEqual(lifted_key, "outer")

    def test_taskspec_identity_cast_is_structurally_reduced(self):
        # Ensure we never execute dask private identity-cast during adaptation.
        def fake_identity_cast(x, *_, **__):
            raise RuntimeError("must not be executed")

        fake_identity_cast.__name__ = "_identity_cast"
        fake_identity_cast.__module__ = "dask._fake"

        graph = {
            "raw": _FakeDataNode(value=5),
            "outer": _FakeTask(
                key="outer",
                function=lambda x: x,
                args=(
                    _FakeTask(
                        key=None,
                        function=fake_identity_cast,
                        args=(_FakeTaskRef("raw"),),
                    ),
                ),
            ),
        }

        adapted = Adaptor(graph).task_dict
        _, outer_args, outer_kwargs = adapted["outer"]
        self.assertEqual(outer_kwargs, {})
        self.assertEqual(len(outer_args), 1)
        self.assertIsInstance(outer_args[0], TaskOutputRef)
        self.assertEqual(outer_args[0].task_key, "raw")
        self.assertFalse(any(str(k).startswith("__lift__") for k in adapted.keys()))


if __name__ == "__main__":
    unittest.main()
