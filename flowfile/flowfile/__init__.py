"""
FlowFile: A framework combining visual ETL with a Polars-like API.

This package ties together the FlowFile ecosystem components:
- flowfile_core: Core ETL functionality
- flowfile_frame: Polars-like DataFrame API
- flowfile_worker: Computation engine
"""

from shared._version import get_version

__version__ = get_version()

import functools
import logging
import os
import sys

os.environ["FLOWFILE_WORKER_PORT"] = "63578"
os.environ["FLOWFILE_SINGLE_FILE_MODE"] = "1"

from flowfile.api import open_graph_in_editor as _open_graph_in_editor
from flowfile.web import start_server as start_web_ui
from flowfile_frame import _fl_namespace
from flowfile_frame._fl_namespace import *  # noqa: F403
from flowfile_frame._fl_namespace import node_designer
from flowfile_frame.notebook import refuse


@functools.wraps(_open_graph_in_editor)
def open_graph_in_editor(*args, **kwargs):
    refuse("ff.open_graph_in_editor")
    return _open_graph_in_editor(*args, **kwargs)


# Bind node_designer as a real submodule so `from flowfile.node_designer import ...`
# resolves (it is otherwise only an attribute); the os.path idiom.
sys.modules[f"{__name__}.node_designer"] = node_designer

__all__ = ["open_graph_in_editor", "start_web_ui"]
__all__ += _fl_namespace.__all__
logging.getLogger("PipelineHandler").setLevel(logging.WARNING)
