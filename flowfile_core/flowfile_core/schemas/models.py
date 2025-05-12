from pydantic import BaseModel
from typing import List, Any, Callable
from datetime import datetime
from dataclasses import dataclass, field
import os
import mimetypes
from copy import deepcopy
from flowfile_core.configs.settings import FILE_LOCATION


class DirItem:
    name: str
    full_path: str
    path: str
    type: str
    stats: os.stat = None
    creation_date: datetime = None
    access_date: datetime = None
    modification_date: datetime = None
    source_path: str = None

    def __init__(self, name: str, path: str,
                 stats: os.stat = None,
                 creation_date: datetime = None,
                 modification_date: datetime = None,
                 access_date: datetime = None, *args, **kwargs):
        self.full_path = os.path.relpath(path)
        if name == FILE_LOCATION:
            self.name = self.full_path
            self.source_path = self.full_path
        else:
            self.name = name
            self.source_path = os.sep.join(os.path.split(self.full_path)[:-1])
        self.path = self.full_path
        self.stats = os.stat(self.full_path) if stats is None else stats
        self.creation_date = datetime.fromtimestamp(self.stats.st_ctime) if creation_date is None else creation_date
        self.modification_date = datetime.fromtimestamp(self.stats.st_mtime) if modification_date is None else modification_date
        self.access_date = datetime.fromtimestamp(self.stats.st_atime) if access_date is None else access_date


@dataclass
class DirFile(DirItem):
    ext: str
    file_size: int
    size: int
    mimetype: str = None

    def __init__(self, name: str, path: str):
        ext = os.path.splitext(name)[-1]
        if ext == '':
            ext = 'unk'
        self.ext = ext
        self.type = 'file'
        self.mimetype = mimetypes.guess_type(path)[0]
        super().__init__(name, path)
        self.file_size = self.stats.st_size
        self.size = self.file_size_in_kb


    @property
    def file_size_in_kb(self) -> int:
        return int(self.file_size/1024)

    def json_repr(self):
        self_dict = deepcopy(self.__dict__)
        self_dict.pop('stats')
        return self_dict

@dataclass
class DirLocation(DirItem):
    all_items: List[str] = field(default_factory=list)
    number_of_items: int = -1
    files: List[DirFile] = field(default_factory=list)
    directories: List["DirLocation"] = field(default_factory=list)
    __size: int = -1

    def __init__(self, name: str, path: str = None):
        path = '.' + os.path.sep + name if path is None else path
        self.all_items = os.listdir(path)
        self.files = []
        self.directories = []
        self.type = 'dir'
        super().__init__(name, path)
        self.create_structure()
        self.number_of_items = len(self.all_items)

    def as_dict(self):
        return {**self.__dict__,**{'size':self.size}}

    def get_dirs(self):
        return self.directories

    @property
    def size(self) -> int:
        if self.__size<0:
            size = 0
            for file in self.files:
                size += file.size
            for d in self.directories:
                size += d.size
            self.__size = size
        return self.__size

    def create_structure(self):
        for item_name in self.all_items:

            ref = os.path.join(self.full_path, item_name)
            if os.path.isdir(ref):
                self.directories.append(self.__class__(item_name,ref))
            elif os.path.isfile(ref):
                self.files.append(DirFile(item_name,ref))

    def json_repr(self):
        self_dict = deepcopy(self.__dict__)
        self_dict.pop('stats')
        files: List[DirFile] = self_dict.pop('files')
        directories: List[DirLocation] = self_dict.pop('directories')
        self_dict.pop('all_items')
        self_dict['files'] = [f.json_repr() for f in files]
        self_dict['directories'] = [d.json_repr() for d in directories]
        return self_dict

class Country(BaseModel):
    id: int
    name: str
    abbreviation: str


class Address(BaseModel):
    street: str = None
    house_number: int = None
    house_number_addition: str = None
    city: str = None
    zipcode: str = None
    country: Country = None


class Team(Address):
    id: int
    name: str


class User(Address):
    user_name: str
    hashed_password: str
    user_id: int
    team: Team


class ExampleValues(BaseModel):
    pass


class SourceStats(BaseModel):
    id: int
    created_by: User
    created_date: datetime = datetime.now()
    updated_by: User
    updated_date: datetime = datetime.now()


class Source(SourceStats):
    name: str
    team: Team


class Table(SourceStats):
    name: str
    table_type: str = None
    table_fields: List['TableField'] = None


class TableField(BaseModel):
    id: int
    source: Source
    name: str
    python_type: type


class FieldMapper(SourceStats):
    mapping_type: str = None  # direct, formula, default value
    input_fields: List[TableField] = None
    output_fields: List[TableField]
    source_table: Table
    target_table: Table
    formula: Callable = None
    default_value: Any = None


class TableMapper(SourceStats):
    source_table: Table
    target_table: Table

