"""Child-process execution of user custom nodes.

Runs in a spawned subprocess: loads the node from shipped source text (never a
path — immune to concurrent save/delete), decrypts secrets locally from the
``$ffsec$`` ciphertext shipped by core, executes ``process()`` on LazyFrames,
and writes every named output as Arrow IPC. The child is the memory
blast-radius boundary; core only ever sees file paths and row counts.
"""

import contextlib
import io
import json
import logging
import time
import traceback
from multiprocessing import Array, Queue, Value

import polars as pl
from pydantic import SecretStr

from flowfile_worker.flow_logger import FlowfileLogHandler, get_worker_logger
from flowfile_worker.secrets import decrypt_secret
from flowfile_worker.utils import collect_lazy_frame

PREVIEW_ROW_LIMIT_DEFAULT = 100


class WorkerSecretResolver:
    """Resolves secret names from the ciphertext map shipped by core.

    The ``$ffsec$1$<owner_id>$`` prefix embeds the owner, so decryption needs no
    core round-trip and group-shared secrets work unchanged.
    """

    def __init__(self, secrets: dict[str, str]):
        self._secrets = secrets

    def resolve(self, secret_name: str) -> SecretStr:
        from shared.node_designer.secrets import SecretResolutionError

        encrypted = self._secrets.get(secret_name)
        if encrypted is None:
            raise SecretResolutionError(f"Secret '{secret_name}' was not provided for this execution")
        return decrypt_secret(encrypted)


class _BufferingLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(self.format(record))


class _StreamToRecords(io.TextIOBase):
    """Tee ``print()`` / stderr writes into the dry-run log buffer, line by line,
    so they interleave with captured logging records in call order."""

    def __init__(self, records: list[str]):
        self._records = records
        self._partial = ""

    def write(self, s: str) -> int:
        self._partial += s
        while "\n" in self._partial:
            line, self._partial = self._partial.split("\n", 1)
            self._records.append(line)
        return len(s)

    def flush(self) -> None:
        if self._partial:
            self._records.append(self._partial)
            self._partial = ""


def _normalize_outputs(result, output_names: list[str]) -> dict[str, pl.LazyFrame]:
    if isinstance(result, dict):
        unknown = sorted(set(result) - set(output_names))
        if unknown:
            raise ValueError(f"process() returned unknown output names {unknown}; declared: {output_names}")
        missing = [n for n in output_names if n not in result]
        if missing:
            raise ValueError(f"process() did not return declared outputs {missing}")
        frames = result
    elif isinstance(result, pl.LazyFrame | pl.DataFrame):
        if len(output_names) > 1:
            raise ValueError(
                f"Node declares {len(output_names)} outputs {output_names} but process() returned a single frame; "
                "return a dict mapping output names to frames"
            )
        frames = {output_names[0]: result}
    elif result is None:
        raise ValueError("process() returned None; return a DataFrame or LazyFrame")
    else:
        raise TypeError(f"process() must return a DataFrame, LazyFrame or dict of them, got {type(result).__name__}")

    return {name: frame.lazy() if isinstance(frame, pl.DataFrame) else frame for name, frame in frames.items()}


def _output_path(file_path: str, name: str, primary_name: str) -> str:
    if name == primary_name:
        return file_path
    return file_path.removesuffix(".arrow") + f"__{name}.arrow"


def _preview_payload(df: pl.DataFrame, row_limit: int) -> dict:
    limited = df.head(row_limit)
    return {
        "columns": [{"name": c, "data_type": str(t)} for c, t in zip(df.columns, df.dtypes, strict=False)],
        "rows": limited.rows(),
        "truncated": df.height > row_limit,
    }


def execute_custom_node_task(
    node_source: str,
    class_name: str | None,
    settings_values: dict,
    secrets: dict,
    inputs: list[bytes],
    output_names: list[str],
    dry_run: bool,
    row_limit: int | None,
    user_id: int | None,
    progress: Value,
    error_message: Array,
    queue: Queue,
    file_path: str,
    flowfile_flow_id: int,
    flowfile_node_id: int | str,
):
    started = time.monotonic()
    buffer_handler: _BufferingLogHandler | None = None
    root_capture_handler: logging.Handler | None = None
    root_logger = logging.getLogger()
    prev_root_level = root_logger.level
    if dry_run:
        # Capture the author's own root-logger output (bare logging.debug/info(...))
        # plus the framework lines into one ordered buffer surfaced in the Test panel.
        # This child is a dedicated spawned process, so mutating the root logger is safe.
        buffer_handler = _BufferingLogHandler()
        buffer_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        root_capture_handler = buffer_handler
        root_logger.setLevel(logging.DEBUG)
        flowfile_logger = logging.getLogger(f"custom_node_dry_run_{flowfile_node_id}")
    else:
        flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)

        root_capture_handler = FlowfileLogHandler(flowfile_flow_id, flowfile_node_id)
        root_capture_handler.setLevel(logging.INFO)
        root_capture_handler.setFormatter(logging.Formatter("%(message)s"))
        root_logger.setLevel(logging.INFO)
    if root_capture_handler is not None:
        root_logger.addHandler(root_capture_handler)

    try:
        from shared.node_designer.loading import find_custom_node_class, install_import_aliases, load_node_module

        install_import_aliases()
        module = load_node_module(source=node_source)
        # The designer never emits `import logging`, so bind it in the node's module
        # namespace — otherwise a bare `logging.debug(...)` in process() raises NameError.
        module.__dict__.setdefault("logging", logging)
        node_cls = find_custom_node_class(module, class_name)
        node = node_cls()

        if settings_values and node.settings_schema:
            node.settings_schema.populate_values(settings_values)
        node.set_execution_context(user_id if user_id is not None else -1, resolver=WorkerSecretResolver(secrets))

        lazy_inputs = [pl.LazyFrame.deserialize(io.BytesIO(raw)) for raw in inputs]
        flowfile_logger.info(f"Executing custom node '{node.node_name}' with {len(lazy_inputs)} input(s)")

        payload: dict = {}
        preview: dict = {}
        with contextlib.ExitStack() as stack:
            if dry_run:
                stream = _StreamToRecords(buffer_handler.records)
                stack.enter_context(contextlib.redirect_stdout(stream))
                stack.enter_context(contextlib.redirect_stderr(stream))
            outputs = _normalize_outputs(node.process(*lazy_inputs), output_names or ["main"])

            payload = {"outputs": {}, "accessed_secret_count": len(node.get_accessed_secrets())}
            primary_name = (output_names or ["main"])[0]
            for name, lf in outputs.items():
                path = _output_path(file_path, name, primary_name)
                df = collect_lazy_frame(lf)
                df.write_ipc(path)
                payload["outputs"][name] = {"path": path, "row_count": df.height}
                if dry_run:
                    preview[name] = _preview_payload(df, row_limit or PREVIEW_ROW_LIMIT_DEFAULT)
                del df
            if dry_run:
                stream.flush()

        flowfile_logger.info(f"Custom node '{node.node_name}' completed")
        if dry_run:
            payload["preview"] = preview
            payload["logs"] = buffer_handler.records if buffer_handler else []
            payload["duration_ms"] = (time.monotonic() - started) * 1000
        queue.put(json.dumps(payload))
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        if dry_run:
            # Full traceback rides back in the error message for the Test panel;
            # the 1024-byte Array truncates, so keep the exception first.
            error_msg = f"{e}\n{traceback.format_exc()}".encode()[:1020]
        else:
            flowfile_logger.error(f"Error executing custom node: {e}")
            flowfile_logger.error(traceback.format_exc())
            error_msg = str(e).encode()[:1020]
        with error_message.get_lock():
            error_message[: len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1
    finally:
        if root_capture_handler is not None:
            root_logger.removeHandler(root_capture_handler)
            root_logger.setLevel(prev_root_level)
