"""DesignerState wire contracts for the visual node designer.

Shared shape between the AST parser, the canonical code generator, and the
frontend editing model (hand-maintained TS mirror:
flowfile_frontend/src/renderer/app/pages/nodeDesigner/designerState.ts).
"""

import keyword
from typing import Annotated, Any, Literal

from pydantic import AfterValidator, BaseModel, Field, model_validator

DESIGNER_STATE_VERSION = 1


def _validate_identifier(value: str) -> str:
    if not value.isidentifier() or keyword.iskeyword(value):
        raise ValueError(f"{value!r} is not a valid Python identifier")
    return value


PyIdentifier = Annotated[str, AfterValidator(_validate_identifier)]


class SelectOption(BaseModel):
    value: str
    label: str | None = None  # None ⇒ label == value


DataTypesSpec = Literal["ALL"] | list[str]


class _ComponentBase(BaseModel):
    name: PyIdentifier
    label: str | None = None


class TextInputState(_ComponentBase):
    component_type: Literal["TextInput"] = "TextInput"
    default: str | None = None
    placeholder: str | None = None


class NumericInputState(_ComponentBase):
    component_type: Literal["NumericInput"] = "NumericInput"
    default: float | None = None
    min_value: float | None = None
    max_value: float | None = None


class SliderInputState(_ComponentBase):
    component_type: Literal["SliderInput"] = "SliderInput"
    default: float | None = None
    min_value: float = 0
    max_value: float = 100
    step: float = 1


class ToggleSwitchState(_ComponentBase):
    component_type: Literal["ToggleSwitch"] = "ToggleSwitch"
    default: bool = False
    description: str | None = None


class SelectState(_ComponentBase):
    component_type: Literal["SingleSelect", "MultiSelect"]
    options_source: Literal["static", "incoming_columns", "available_artifacts"] = "static"
    options: list[SelectOption] = []  # only when static
    default: Any | None = None  # list for MultiSelect
    artifact_scope: Literal["upstream", "global", "all"] = "upstream"  # only when available_artifacts
    artifact_type_filter: list[str] = []  # only when available_artifacts

    @model_validator(mode="after")
    def _clear_artifact_fields_off_source(self) -> "SelectState":
        # Codegen only emits these for the available_artifacts source; reset them
        # otherwise so parse can't lose them and the fixed point stays exact.
        # On-source, canonicalize the filter (sorted-unique) so a frontend-authored
        # state converges to the same byte-identical fixed point the parser produces.
        if self.options_source != "available_artifacts":
            self.artifact_scope = "upstream"
            self.artifact_type_filter = []
        else:
            self.artifact_type_filter = sorted(set(self.artifact_type_filter))
        return self


class ColumnSelectorState(_ComponentBase):
    component_type: Literal["ColumnSelector"] = "ColumnSelector"
    required: bool = False
    multiple: bool = False
    data_types: DataTypesSpec = "ALL"


class SecretSelectorState(_ComponentBase):
    component_type: Literal["SecretSelector"] = "SecretSelector"
    required: bool = False
    description: str | None = None
    name_prefix: str | None = None


class ColumnActionInputState(_ComponentBase):
    component_type: Literal["ColumnActionInput"] = "ColumnActionInput"
    actions: list[SelectOption] = []
    output_name_template: str = "{column}_{action}"
    show_group_by: bool = False
    show_order_by: bool = False
    data_types: DataTypesSpec = "ALL"


ComponentState = Annotated[
    TextInputState
    | NumericInputState
    | SliderInputState
    | ToggleSwitchState
    | SelectState
    | ColumnSelectorState
    | SecretSelectorState
    | ColumnActionInputState,
    Field(discriminator="component_type"),
]


class VisibleWhenState(BaseModel):
    field: str  # dotted "<section>.<toggle>" reference
    equals: bool = True


class SectionState(BaseModel):
    name: PyIdentifier
    title: str | None = None
    description: str | None = None
    hidden: bool = False
    visible_when: VisibleWhenState | None = None
    layout: Literal["vertical", "horizontal"] = "vertical"
    components: list[ComponentState] = []  # ordered


class EnvironmentState(BaseModel):
    kind: Literal["local", "kernel"] = "local"
    dependencies: list[str] = []  # pip requirement strings, kernel only
    default_kernel_id: str | None = None  # legacy kernel_id carry-over, deprecated


class ExampleInput(BaseModel):
    data: dict[str, list[Any]]  # column -> JSON-scalar values


class ArtifactDecl(BaseModel):
    name: str
    type: str | None = None


class DesignerState(BaseModel):
    schema_version: Literal[1] = 1
    class_name: PyIdentifier
    settings_class_name: PyIdentifier
    node_name: str
    node_category: str = "Custom"
    node_group: str = "custom"
    node_icon: str = "user-defined-icon.png"
    title: str = ""
    intro: str = ""
    author: str = ""
    version: str = ""
    tags: list[str] = []
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    output_names: list[str] = ["main"]
    environment: EnvironmentState = EnvironmentState()
    sections: list[SectionState] = []
    process_code: str = ""  # full `def process(...)` source, dedented, verbatim
    predict_schema_code: str = ""  # full `def predict_output_schema(...)` source; "" = no hook
    example_inputs: list[ExampleInput] | None = None
    example_settings: dict[str, dict[str, Any]] | None = None
    extra_imports: list[str] = []  # verbatim statements, order preserved
    module_extra: list[str] = []  # verbatim module-level blocks, order preserved
    class_extra: list[str] = []  # verbatim class members, order preserved
    publishes: list[ArtifactDecl] = []  # artifacts the node declares it publishes


class ParseIssue(BaseModel):
    code: str  # ParseIssueCode value (parsing.py)
    message: str
    line: int | None = None
    severity: Literal["error", "warning"] = "error"


class ParseResult(BaseModel):
    mode: Literal["designer", "code_only"]
    designer_state: DesignerState | None = None  # None iff code_only
    issues: list[ParseIssue] = []  # errors explain code_only; warnings ride along in designer mode


class NodeManifest(BaseModel):
    """Exec-free listing metadata lifted from a node file; every field best-effort."""

    class_name: str | None = None
    node_name: str | None = None
    node_category: str = "Custom"
    node_group: str | None = "custom"
    node_icon: str = "user-defined-icon.png"
    title: str = ""
    intro: str = ""
    author: str = ""
    version: str = ""
    tags: list[str] = []
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    output_names: list[str] = ["main"]
    environment: EnvironmentState = EnvironmentState()
    node_type: Literal["input", "output", "process"] = "process"
    transform_type: Literal["narrow", "wide", "other"] = "wide"
    publishes: list[ArtifactDecl] = []
