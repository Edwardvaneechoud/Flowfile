from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class GeoRole(BaseModel):
    # Placeholder for geo role specifics
    role_type: str  # Example attribute


class Expression(BaseModel):
    # Placeholder for expression specifics
    expression: str  # Example attribute


AnalyticTypeLit = Literal["measure", "dimension"]


class IField(BaseModel):
    fid: str
    name: str
    basename: str | None = None
    semanticType: str
    analyticType: AnalyticTypeLit
    cmp: str | None = None
    geoRole: GeoRole | None = None
    computed: bool | None = None
    expression: str | None = None
    timeUnit: str | None = None
    path: list[str] | None = None
    offset: int | None = None
    aggName: str | None = None
    aggregated: bool | None = None

    @model_validator(mode="after")
    def set_default_aggname(self):
        if self.aggName is None and self.analyticType == "measure":
            self.aggName = "sum"
        return self

    def model_dump_dict(self):
        d = self.model_dump(exclude_none=True)
        d["offset"] = None
        return d


class ViewField(IField):
    sort: str | None = None


class FilterField(ViewField):
    rule: Any
    enableAgg: bool | None = False


class DraggableFieldState(BaseModel):
    dimensions: list[ViewField]
    measures: list[ViewField]
    rows: list[ViewField]
    columns: list[ViewField]
    color: list[ViewField]
    opacity: list[ViewField]
    size: list[ViewField]
    shape: list[ViewField]
    theta: list[ViewField]
    radius: list[ViewField]
    longitude: list[ViewField]
    latitude: list[ViewField]
    geoId: list[ViewField]
    details: list[ViewField]
    filters: list[FilterField]
    text: list[ViewField]


class ConfigScale(BaseModel):
    rangeMax: int | None
    rangeMin: int | None
    domainMin: int | None
    domainMax: int | None


class MutField(BaseModel):
    fid: str
    key: str | None = None
    name: str | None = None
    basename: str | None = None
    disable: bool | None = False
    semanticType: str
    analyticType: AnalyticTypeLit
    path: list[str] | None = None
    offset: int | None = None


class DataModel(BaseModel):
    data: list[dict[str, Any]]
    fields: list[MutField]


class IVisualConfigNew(BaseModel):
    defaultAggregated: bool
    geoms: list[str]
    coordSystem: str | None
    limit: int = None
    folds: list[str] | None = []
    timezoneDisplayOffset: int | None = None


class Chart(BaseModel):
    visId: str
    name: str | None
    encodings: DraggableFieldState
    config: IVisualConfigNew


class GraphicWalkerInput(BaseModel):
    dataModel: DataModel = Field(default_factory=lambda: DataModel(data=[], fields=[]))
    is_initial: bool = True
    specList: list[Any] | None = None
