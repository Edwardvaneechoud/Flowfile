"""Seeded-random fixed-point property test for the parser<->generator pair.

For a random DesignerState ``s`` over the full component/kwarg space:
  * ``parse(generate(s)).designer_state == s``   (round-trip identity)
  * ``generate(parse(generate(s))) == generate(s)``  (byte-stable fixed point)

Random values are drawn from the self-normalizing subset (canonical forms the
parser reproduces exactly) so equality is exact, not approximate. Plain
``random`` with a fixed seed — no hypothesis dependency.
"""

import random

import pytest

from flowfile_core.flowfile.node_designer.codegen import generate_source
from flowfile_core.flowfile.node_designer.parsing import parse_source
from flowfile_core.flowfile.node_designer.state import (
    ColumnActionInputState,
    ColumnSelectorState,
    DesignerState,
    EnvironmentState,
    ExampleInput,
    NumericInputState,
    SectionState,
    SecretSelectorState,
    SelectOption,
    SelectState,
    SliderInputState,
    TextInputState,
    ToggleSwitchState,
    VisibleWhenState,
)

# Leaf DataType values that normalize to themselves (see enumeration in the SDK).
_LEAF_TYPES = [
    "Boolean",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Float32",
    "Float64",
    "Decimal",
    "Categorical",
    "Datetime",
    "Time",
    "Duration",
    "Binary",
    "List",
    "Struct",
    # Canonical group tokens are preserved verbatim (not expanded), so they must
    # round-trip through parse/codegen too.
    "Numeric",
    "String",
    "Date",
    "Complex",
]

_WORDS = ["alpha", "beta", "gamma", "delta", "col", "value", "x", "y", "score", "name"]


def _ident(rng: random.Random, taken: set[str], prefix: str = "f") -> str:
    while True:
        name = f"{prefix}_{rng.choice(_WORDS)}_{rng.randint(0, 9999)}"
        if name not in taken:
            taken.add(name)
            return name


def _text(rng: random.Random) -> str:
    # include quote/newline/unicode occasionally to exercise escaping
    base = " ".join(rng.sample(_WORDS, rng.randint(1, 3)))
    flourish = rng.choice(["", ' "quoted"', "\nsecond line", " ünïcodé", " tab\there", ""])
    return base + flourish


def _maybe(rng: random.Random, value, prob: float = 0.5):
    return value if rng.random() < prob else None


def _data_types(rng: random.Random):
    if rng.random() < 0.4:
        return "ALL"
    n = rng.randint(1, 4)
    return sorted(set(rng.sample(_LEAF_TYPES, n)))


def _options(rng: random.Random) -> list[SelectOption]:
    opts = []
    seen = set()
    for _ in range(rng.randint(1, 4)):
        value = f"opt_{rng.randint(0, 999)}"
        if value in seen:
            continue
        seen.add(value)
        label = _text(rng) if rng.random() < 0.5 else None
        opts.append(SelectOption(value=value, label=label))
    return opts


def _component(rng: random.Random, taken: set[str]):
    kind = rng.choice(
        [
            "TextInput",
            "NumericInput",
            "SliderInput",
            "ToggleSwitch",
            "SingleSelect",
            "MultiSelect",
            "ColumnSelector",
            "SecretSelector",
            "ColumnActionInput",
        ]
    )
    name = _ident(rng, taken, "c")
    label = _maybe(rng, _text(rng))

    if kind == "TextInput":
        return TextInputState(
            name=name, label=label, default=_maybe(rng, _text(rng)), placeholder=_maybe(rng, _text(rng))
        )
    if kind == "NumericInput":
        return NumericInputState(
            name=name,
            label=label,
            default=_maybe(rng, float(rng.randint(-100, 100))),
            min_value=_maybe(rng, float(rng.randint(-100, 0))),
            max_value=_maybe(rng, float(rng.randint(1, 100))),
        )
    if kind == "SliderInput":
        return SliderInputState(
            name=name,
            label=label,
            default=_maybe(rng, float(rng.randint(0, 100))),
            min_value=float(rng.choice([0, 1, 5])),
            max_value=float(rng.choice([10, 50, 100])),
            step=float(rng.choice([1, 2, 5])),
        )
    if kind == "ToggleSwitch":
        return ToggleSwitchState(
            name=name, label=label, default=rng.random() < 0.5, description=_maybe(rng, _text(rng))
        )
    if kind in ("SingleSelect", "MultiSelect"):
        source = rng.choice(["static", "static", "incoming_columns", "available_artifacts"])
        if source == "static":
            opts = _options(rng)
            default = None
            if kind == "MultiSelect" and rng.random() < 0.5:
                default = [o.value for o in rng.sample(opts, rng.randint(0, len(opts)))]
            elif kind == "SingleSelect" and opts and rng.random() < 0.5:
                default = rng.choice(opts).value
            return SelectState(
                name=name, label=label, component_type=kind, options_source="static", options=opts, default=default
            )
        return SelectState(name=name, label=label, component_type=kind, options_source=source, options=[])
    if kind == "ColumnSelector":
        return ColumnSelectorState(
            name=name,
            label=label,
            required=rng.random() < 0.5,
            multiple=rng.random() < 0.5,
            data_types=_data_types(rng),
        )
    if kind == "SecretSelector":
        return SecretSelectorState(
            name=name,
            label=label,
            required=rng.random() < 0.5,
            description=_maybe(rng, _text(rng)),
            name_prefix=_maybe(rng, f"prefix_{rng.randint(0, 99)}"),
        )
    # ColumnActionInput
    actions = [
        SelectOption(value=f"act_{i}", label=_text(rng) if rng.random() < 0.5 else None)
        for i in range(rng.randint(0, 3))
    ]
    return ColumnActionInputState(
        name=name,
        label=label,
        actions=actions,
        output_name_template=rng.choice(["{column}_{action}", "{column}__{action}", "out_{column}"]),
        show_group_by=rng.random() < 0.5,
        show_order_by=rng.random() < 0.5,
        data_types=_data_types(rng),
    )


def _visible_when(rng: random.Random) -> VisibleWhenState | None:
    if rng.random() < 0.5:
        return None
    return VisibleWhenState(
        field=f"{rng.choice(_WORDS)}.{rng.choice(_WORDS)}",
        equals=rng.random() < 0.5,
    )


def _section(rng: random.Random, taken: set[str]) -> SectionState:
    return SectionState(
        name=_ident(rng, taken, "s"),
        title=_maybe(rng, _text(rng)),
        description=_maybe(rng, _text(rng)),
        hidden=rng.random() < 0.3,
        visible_when=_visible_when(rng),
        layout=rng.choice(["vertical", "horizontal"]),
        components=[_component(rng, taken) for _ in range(rng.randint(0, 4))],
    )


def _example_inputs(rng: random.Random):
    if rng.random() < 0.5:
        return None
    n_ports = rng.randint(1, 2)
    examples = []
    for _ in range(n_ports):
        cols = {}
        for _ in range(rng.randint(1, 3)):
            col = f"col_{rng.randint(0, 99)}"
            length = rng.randint(0, 4)
            kind = rng.choice(["int", "str", "bool", "float"])
            if kind == "int":
                cols[col] = [rng.randint(-50, 50) for _ in range(length)]
            elif kind == "str":
                cols[col] = [_text(rng) for _ in range(length)]
            elif kind == "bool":
                cols[col] = [rng.random() < 0.5 for _ in range(length)]
            else:
                cols[col] = [float(rng.randint(-50, 50)) for _ in range(length)]
        examples.append(ExampleInput(data=cols))
    return examples


def _random_state(seed: int) -> DesignerState:
    rng = random.Random(seed)
    taken: set[str] = set()
    n_outputs = rng.randint(1, 3)
    output_names = ["main"] if n_outputs == 1 else [f"out_{i}" for i in range(n_outputs)]
    env_kind = rng.choice(["local", "local", "kernel"])
    environment = EnvironmentState(
        kind=env_kind,
        dependencies=[f"pkg-{i}=={i}.0" for i in range(rng.randint(0, 2))] if env_kind == "kernel" else [],
        default_kernel_id=_maybe(rng, f"kernel-{rng.randint(0, 9)}", 0.3) if env_kind == "kernel" else None,
    )
    example_settings = None
    if rng.random() < 0.3:
        example_settings = {f"sec_{rng.randint(0, 9)}": {f"comp_{rng.randint(0, 9)}": rng.randint(0, 100)}}

    class_name = f"Node{rng.choice(_WORDS).title()}{rng.randint(0, 9999)}"
    sections = [_section(rng, taken) for _ in range(rng.randint(0, 3))]
    # With no settings class emitted, the parser falls back to <class_name>Settings;
    # match that so the generated settings_class_name round-trips.
    if sections:
        settings_class_name = f"Settings{rng.choice(_WORDS).title()}{rng.randint(0, 9999)}"
    else:
        settings_class_name = f"{class_name}Settings"

    return DesignerState(
        class_name=class_name,
        settings_class_name=settings_class_name,
        node_name=_text(rng) or "Node",
        node_category=rng.choice(["Custom", "Text Processing", "Numerics", "My Nodes"]),
        node_group=rng.choice(["custom", "transform"]),
        node_icon=rng.choice(["user-defined-icon.png", "ruler.svg"]),
        title=rng.choice(["", _text(rng)]),
        intro=rng.choice(["", _text(rng)]),
        author=rng.choice(["", _text(rng)]),
        version=rng.choice(["", f"{rng.randint(0, 3)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}"]),
        tags=[f"tag-{i}" for i in range(rng.randint(1, 3))] if rng.random() < 0.4 else [],
        number_of_inputs=rng.randint(0, 3),
        number_of_outputs=n_outputs,
        output_names=output_names,
        environment=environment,
        sections=sections,
        process_code="def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n    return inputs[0]",
        example_inputs=_example_inputs(rng),
        example_settings=example_settings,
    )


@pytest.mark.parametrize("seed", range(200))
def test_fixed_point_random(seed: int):
    state = _random_state(seed)
    generated = generate_source(state)

    # generated code is valid python and loads back as designer mode
    import ast

    ast.parse(generated)

    reparsed = parse_source(generated)
    assert (
        reparsed.mode == "designer"
    ), f"seed={seed} parsed as {reparsed.mode}: {[(i.code, i.message) for i in reparsed.issues if i.severity == 'error']}"
    assert reparsed.designer_state == state, f"seed={seed} round-trip identity failed"

    # byte-stable fixed point
    assert generate_source(reparsed.designer_state) == generated, f"seed={seed} not byte-stable"
