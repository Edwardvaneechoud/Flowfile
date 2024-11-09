from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class GeoRole(BaseModel):
    # Placeholder for geo role specifics
    role_type: str  # Example attribute


class Expression(BaseModel):
    # Placeholder for expression specifics
    expression: str  # Example attribute


class IField(BaseModel):
    fid: str
    name: str
    aggName: Optional[str]
    semanticType: str
    analyticType: str
    cmp: Optional[str]
    geoRole: Optional[GeoRole] = None
    computed: Optional[bool] = False
    expression: Optional[Expression] = ''
    timeUnit: Optional[str]  # Depending on DATE_TIME_DRILL_LEVELS definition
    basename: Optional[str]
    path: Optional[List[str]]
    offset: Optional[int]
    aggergated: Optional[bool] = False


class ViewField(IField):
    sort: Optional[str] = None


class FilterField(ViewField):
    rule: Any
    enableAgg: Optional[bool] = False


class DraggableFieldState(BaseModel):
    dimensions: List[ViewField]
    measures: List[ViewField]
    rows: List[ViewField]
    columns: List[ViewField]
    color: List[ViewField]
    opacity: List[ViewField]
    size: List[ViewField]
    shape: List[ViewField]
    theta: List[ViewField]
    radius: List[ViewField]
    longitude: List[ViewField]
    latitude: List[ViewField]
    geoId: List[ViewField]
    details: List[ViewField]
    filters: List[FilterField]
    text: List[ViewField]


class ConfigScale(BaseModel):
    rangeMax: Optional[int]
    rangeMin: Optional[int]
    domainMin: Optional[int]
    domainMax: Optional[int]


class MutField(BaseModel):
    fid: str
    key: Optional[str] = None
    name: Optional[str] = None
    basename: Optional[str] = None
    disable: Optional[bool] = False
    semanticType: str
    analyticType: str
    path: Optional[List[str]] = None
    offset: Optional[int] = None


class DataModel(BaseModel):
    data: List[Dict[str, Any]]
    fields: List[MutField]


class IVisualConfigNew (BaseModel):
    defaultAggregated: bool
    geoms: List[str]
    coordSystem: Optional[str]
    limit: int = None
    folds: Optional[List[str]] = []
    timezoneDisplayOffset: Optional[int] = None


class Chart(BaseModel):
    visId: str
    name: Optional[str]
    encodings: DraggableFieldState
    config: IVisualConfigNew


class GraphicWalkerInput (BaseModel):
    dataModel: DataModel = Field(default_factory=lambda: DataModel(data=[], fields=[]))
    is_initial: bool = True
    specList: Optional[List[Any]] = None

