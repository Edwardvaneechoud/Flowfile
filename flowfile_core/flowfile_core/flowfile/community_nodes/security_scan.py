"""AST security scanner for community custom nodes.

Honest limits: this scanner parses node source with :mod:`ast` and never
executes, imports, or ``compile()``s it. It blocks the cheap remote-code-execution
idioms (``eval``/``exec``, ``__import__``/importlib indirection, decode-then-exec
dataflow, ``getattr``-into-builtins, ``ctypes``/FFI, shell subprocess, reverse-shell
shapes, runtime pip, opaque blobs, env enumeration) and surfaces capabilities
(network, filesystem, subprocess, env reads, serialization, secrets) so the
in-app consent dialog can disclose them. It is deliberately conservative and
easy to evade: reflection, attribute-stored payloads crossing functions, and
novel encodings can slip past static analysis. The real control is the reviewed
PR gate on the community repo; this scan is a fast pre-filter and a capability
discloser, not a sandbox.

Rule tiers and rationale (stable ids; never renumber):
  FF-SEC-000 deny  unparseable source — a node that can't parse can't be trusted.
  FF-SEC-001 deny  eval/exec/compile — a data node never needs to run arbitrary code.
  FF-SEC-002 deny  __import__/importlib indirection — dynamic import hides the real target.
  FF-SEC-003 deny  decode-then-exec dataflow — the canonical obfuscated-RCE shape.
  FF-SEC-004 deny  getattr/globals()/vars()/__builtins__ into builtins — reflection to eval/system.
  FF-SEC-005 deny  ctypes/cffi/_ctypes — native FFI escapes every Python-level guard.
  FF-SEC-006 deny  os.system/popen/exec*/spawn* — direct shell/command execution.
  FF-SEC-007 deny  subprocess with shell=True or non-literal args — command injection surface.
  FF-SEC-008 deny  pty/forkpty/socket+subprocess — the reverse-shell shape.
  FF-SEC-009 deny  sys._getframe/inspect.stack/currentframe — frame walking to reach caller state.
  FF-SEC-010 deny  runtime pip / pip import — a node must not mutate the environment.
  FF-SEC-011 deny  opaque high-entropy blob — hidden payloads a reviewer can't read.
  FF-SEC-012 deny  os.environ enumeration — bulk secret/credential exfiltration shape.
  FF-SEC-101 flag  network imports — capability: network.
  FF-SEC-102 flag  subprocess with literal args — capability: subprocess.
  FF-SEC-103 flag  filesystem writes/deletes — capability: fs_write.
  FF-SEC-104 flag  targeted env reads — capability: env_read.
  FF-SEC-105 flag  filesystem reads — capability: fs_read.
  FF-SEC-106 flag  dynamic attribute mutation / FunctionType — capability: dynamic_code.
  FF-SEC-107 flag  serialization loads / decode not reaching exec — capability: serialization.
  FF-SEC-108 flag  SecretSelector/AvailableSecrets/.secret_value — capability: secrets.
  FF-SEC-201 lint  env read without declaring a secrets capability — informational.
"""

import ast
import math
import re
from collections import Counter
from typing import Literal

from pydantic import BaseModel

SCANNER_VERSION: str = "1"

CAPABILITIES: tuple = (
    "network",
    "fs_read",
    "fs_write",
    "subprocess",
    "env_read",
    "dynamic_code",
    "native_ffi",
    "secrets",
    "serialization",
)

_NETWORK_MODULES = frozenset(
    {
        "socket",
        "requests",
        "httpx",
        "urllib",
        "urllib3",
        "http",
        "ftplib",
        "smtplib",
        "aiohttp",
        "websockets",
        "paramiko",
        "boto3",
    }
)
_FFI_MODULES = frozenset({"ctypes", "cffi", "_ctypes"})
_EXEC_BUILTINS = frozenset({"eval", "exec", "compile"})
_SUBPROCESS_FUNCS = frozenset({"run", "call", "check_call", "check_output", "Popen", "getoutput", "getstatusoutput"})
_GETATTR_BUILTIN_NAMES = frozenset({"eval", "exec", "compile", "system", "__import__"})
_ENUM_WRAPPERS = frozenset({"dict", "list", "set", "tuple", "sorted", "frozenset"})
_BASE64ISH = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-")

_OPAQUE_HARD_LEN = 4096
_OPAQUE_SOFT_LEN = 1024
_OPAQUE_ENTROPY = 4.5


class Finding(BaseModel):
    rule_id: str
    severity: Literal["deny", "flag", "lint"]
    capability: str | None = None
    message: str
    lineno: int = 0
    col: int = 0


class ScanReport(BaseModel):
    findings: list[Finding] = []
    capabilities: list[str] = []
    deny_count: int = 0
    passed: bool = True
    scanner_version: str = SCANNER_VERSION


def _dotted(node: ast.AST) -> str | None:
    """Literal dotted name of a Name/Attribute chain (``os.path.join``), else None."""
    parts: list[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return ".".join(reversed(parts))
    return None


def _shannon_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts = Counter(value)
    length = len(value)
    return -sum((count / length) * math.log2(count / length) for count in counts.values())


def _is_opaque_blob(text: str) -> bool:
    length = len(text)
    if length > _OPAQUE_HARD_LEN:
        return True
    if length < _OPAQUE_SOFT_LEN:
        return False
    alphabet_ratio = sum(1 for ch in text if ch in _BASE64ISH) / length
    if alphabet_ratio < 0.95:
        return False
    return _shannon_entropy(text) > _OPAQUE_ENTROPY


def _pip_in_call(node: ast.Call) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            if "pip" in re.split(r"[\s,;=]+", sub.value):
                return True
    return False


def _literal_command(node: ast.expr | None) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str | bytes):
        return True
    if isinstance(node, ast.List | ast.Tuple):
        return all(isinstance(el, ast.Constant) and isinstance(el.value, str | bytes) for el in node.elts)
    return False


def _yaml_is_safe(node: ast.Call) -> bool:
    for keyword in node.keywords:
        if keyword.arg == "Loader":
            name = _dotted(keyword.value) or ""
            return "SafeLoader" in name or "BaseLoader" in name
    if len(node.args) >= 2:
        name = _dotted(node.args[1]) or ""
        return "SafeLoader" in name or "BaseLoader" in name
    return False


class _Scanner:
    def __init__(self, source: str, tree: ast.Module):
        self.source = source
        self.tree = tree
        self.findings: list[Finding] = []
        self.import_map: dict[str, str] = {}
        self.imported_modules: set[str] = set()
        self.scope_of: dict[int, object] = {}
        self.decode_sites: dict[int, ast.Call] = {}
        self.taint: dict[tuple, set[int]] = {}
        self.consumed_decodes: set[int] = set()

    def run(self) -> ScanReport:
        self._build_import_map()
        self._annotate_scopes()
        self._collect_decode_sites()
        self._build_taint()
        self._walk()
        self._emit_leftover_serialization()
        capabilities = sorted({f.capability for f in self.findings if f.severity == "flag" and f.capability})
        self._env_read_lint(capabilities)
        deny_count = sum(1 for f in self.findings if f.severity == "deny")
        return ScanReport(
            findings=self.findings,
            capabilities=capabilities,
            deny_count=deny_count,
            passed=deny_count == 0,
        )

    def _add(self, rule_id, severity, message, node, capability=None):
        self.findings.append(
            Finding(
                rule_id=rule_id,
                severity=severity,
                capability=capability,
                message=message,
                lineno=getattr(node, "lineno", 0) or 0,
                col=getattr(node, "col_offset", 0) or 0,
            )
        )

    def _canonical(self, node: ast.AST) -> str | None:
        dotted = _dotted(node)
        if dotted is None:
            return None
        head, _, rest = dotted.partition(".")
        if head in self.import_map:
            base = self.import_map[head]
            return f"{base}.{rest}" if rest else base
        return dotted

    def _is_os_environ(self, node: ast.AST) -> bool:
        return self._canonical(node) == "os.environ"

    def _build_import_map(self):
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    self.imported_modules.add(top)
                    if alias.asname:
                        self.import_map[alias.asname] = alias.name
                    else:
                        self.import_map[top] = top
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level == 0 and module:
                    self.imported_modules.add(module.split(".")[0])
                for alias in node.names:
                    bound = alias.asname or alias.name
                    self.import_map[bound] = f"{module}.{alias.name}" if module else alias.name

    def _annotate_scopes(self):
        def visit(node, scope):
            self.scope_of[id(node)] = scope
            child_scope = id(node) if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda) else scope
            for child in ast.iter_child_nodes(node):
                visit(child, child_scope)

        visit(self.tree, "module")

    def _is_decode_call(self, node: ast.Call) -> bool:
        canonical = self._canonical(node.func)
        if canonical in {
            "codecs.decode",
            "zlib.decompress",
            "bz2.decompress",
            "lzma.decompress",
            "marshal.loads",
            "pickle.loads",
        }:
            return True
        if canonical and canonical.startswith("base64.") and "decode" in canonical.rsplit(".", 1)[-1].lower():
            return True
        if isinstance(node.func, ast.Attribute) and node.func.attr == "fromhex":
            return True
        return False

    def _collect_decode_sites(self):
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call) and self._is_decode_call(node):
                self.decode_sites[id(node)] = node

    def _build_taint(self):
        for node in ast.walk(self.tree):
            if not isinstance(node, ast.Assign | ast.AnnAssign):
                continue
            value = node.value
            if value is None:
                continue
            decode_ids = {id(n) for n in ast.walk(value) if id(n) in self.decode_sites}
            if not decode_ids:
                continue
            scope = self.scope_of.get(id(node), "module")
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for name_node in ast.walk(target):
                    if isinstance(name_node, ast.Name):
                        self.taint.setdefault((scope, name_node.id), set()).update(decode_ids)

    def _walk(self):
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import | ast.ImportFrom):
                self._check_import(node)
            elif isinstance(node, ast.Call):
                self._check_call(node)
            elif isinstance(node, ast.Subscript):
                self._check_subscript(node)
            elif isinstance(node, ast.Attribute):
                self._check_attribute(node)
            elif isinstance(node, ast.Name):
                self._check_name(node)
            elif isinstance(node, ast.Constant):
                self._check_constant(node)
            elif isinstance(node, ast.For | ast.AsyncFor):
                if self._is_os_environ(node.iter):
                    self._add("FF-SEC-012", "deny", "iterates os.environ (bulk env access)", node)
            elif isinstance(node, ast.ListComp | ast.SetComp | ast.DictComp | ast.GeneratorExp):
                for gen in node.generators:
                    if self._is_os_environ(gen.iter):
                        self._add("FF-SEC-012", "deny", "iterates os.environ (bulk env access)", node)

    def _check_import(self, node):
        names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""]
        for name in names:
            top = name.split(".")[0]
            if name == "importlib.util" or name.startswith("importlib.util."):
                self._add("FF-SEC-002", "deny", f"imports {name} (dynamic import machinery)", node)
            if isinstance(node, ast.ImportFrom) and (node.module or "") == "importlib":
                if any(a.name == "util" for a in node.names):
                    self._add("FF-SEC-002", "deny", "imports importlib.util (dynamic import machinery)", node)
            if top in _FFI_MODULES:
                self._add("FF-SEC-005", "deny", f"imports {name} (native FFI)", node)
            if top == "pty":
                self._add("FF-SEC-008", "deny", "imports pty (reverse-shell primitive)", node)
            if top in {"pip", "ensurepip"}:
                self._add("FF-SEC-010", "deny", f"imports {name} (runtime environment mutation)", node)
            if top in _NETWORK_MODULES:
                self._add("FF-SEC-101", "flag", f"imports {name} (network)", node, capability="network")
        if {"socket", "subprocess"} <= self.imported_modules:
            self._add("FF-SEC-008", "deny", "imports both socket and subprocess (reverse-shell shape)", node)

    def _check_call(self, node: ast.Call):
        canonical = self._canonical(node.func)
        if canonical in _EXEC_BUILTINS:
            self._add("FF-SEC-001", "deny", f"calls builtin {canonical}()", node)
            self._check_decode_to_exec(node)
        if (
            canonical == "__import__"
            or canonical in {"importlib.import_module", "importlib.__import__"}
            or (canonical and canonical.startswith("importlib.util"))
        ):
            self._add("FF-SEC-002", "deny", f"dynamic import via {canonical}", node)
        if canonical == "getattr":
            self._check_getattr(node)
        if (
            canonical == "os.system"
            or canonical == "posix.system"
            or (
                canonical
                and (
                    canonical.startswith("os.popen")
                    or canonical.startswith("os.exec")
                    or canonical.startswith("os.spawn")
                    or canonical.startswith("posix.popen")
                )
            )
        ):
            self._add("FF-SEC-006", "deny", f"command execution via {canonical}()", node)
        if canonical == "os.forkpty":
            self._add("FF-SEC-008", "deny", "os.forkpty (reverse-shell primitive)", node)
        if canonical in {"sys._getframe", "inspect.stack", "inspect.currentframe"}:
            self._add("FF-SEC-009", "deny", f"frame introspection via {canonical}()", node)
        if canonical and (canonical.startswith("subprocess.") or canonical.startswith("os.")) and _pip_in_call(node):
            self._add("FF-SEC-010", "deny", f"runtime pip invocation via {canonical}()", node)
        if canonical and canonical.startswith("subprocess.") and canonical.rsplit(".", 1)[-1] in _SUBPROCESS_FUNCS:
            self._check_subprocess(node, canonical)
        if canonical in {"os.environ.items", "os.environ.keys", "os.environ.values"}:
            self._add("FF-SEC-012", "deny", f"{canonical}() enumerates the environment", node)
        if canonical in _ENUM_WRAPPERS and any(self._is_os_environ(arg) for arg in node.args):
            self._add("FF-SEC-012", "deny", f"{canonical}(os.environ) enumerates the environment", node)
        self._check_fs_call(node, canonical)
        self._check_env_call(node, canonical)
        self._check_dynamic_call(node, canonical)
        self._check_serialization_call(node, canonical)

    def _check_decode_to_exec(self, node: ast.Call):
        scope = self.scope_of.get(id(node), "module")
        reached: set[int] = set()
        for arg in list(node.args) + [kw.value for kw in node.keywords]:
            for sub in ast.walk(arg):
                if id(sub) in self.decode_sites:
                    reached.add(id(sub))
                if isinstance(sub, ast.Name):
                    reached.update(self.taint.get((scope, sub.id), set()))
        if reached:
            self.consumed_decodes.update(reached)
            self._add("FF-SEC-003", "deny", "decoded/deserialized data flows into exec/eval/compile", node)

    def _check_getattr(self, node: ast.Call):
        if len(node.args) < 2:
            return
        name_arg = node.args[1]
        if isinstance(name_arg, ast.Constant) and isinstance(name_arg.value, str):
            if name_arg.value in _GETATTR_BUILTIN_NAMES:
                self._add("FF-SEC-004", "deny", f"getattr(..., {name_arg.value!r}) resolves a dangerous builtin", node)
        else:
            self._add("FF-SEC-004", "deny", "getattr with a non-constant attribute name (reflection)", node)

    def _check_subprocess(self, node: ast.Call, canonical: str):
        shell_true = any(
            kw.arg == "shell" and isinstance(kw.value, ast.Constant) and kw.value.value is True for kw in node.keywords
        )
        command = node.args[0] if node.args else next((kw.value for kw in node.keywords if kw.arg == "args"), None)
        if shell_true:
            self._add("FF-SEC-007", "deny", f"{canonical}(..., shell=True)", node)
        elif not _literal_command(command):
            self._add("FF-SEC-007", "deny", f"{canonical}() with non-literal command arguments", node)
        else:
            self._add("FF-SEC-102", "flag", f"{canonical}() with literal arguments", node, capability="subprocess")

    def _check_fs_call(self, node: ast.Call, canonical):
        if canonical in {"open", "io.open", "builtins.open"}:
            self._flag_open(node)
            return
        if canonical in {"os.remove", "os.unlink", "os.rmdir", "os.makedirs"}:
            self._add("FF-SEC-103", "flag", f"{canonical}() modifies the filesystem", node, capability="fs_write")
            return
        if canonical in {"os.walk", "os.listdir", "os.scandir"}:
            self._add("FF-SEC-105", "flag", f"{canonical}() reads the filesystem", node, capability="fs_read")
            return
        if (
            canonical == "shutil.rmtree"
            or canonical == "shutil.move"
            or (canonical and canonical.startswith("shutil.copy"))
        ):
            self._add("FF-SEC-103", "flag", f"{canonical}() modifies the filesystem", node, capability="fs_write")
            return
        if canonical in {"glob.glob", "glob.iglob"}:
            self._add("FF-SEC-105", "flag", f"{canonical}() reads the filesystem", node, capability="fs_read")
            return
        if isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr in {"write_text", "write_bytes"}:
                self._add("FF-SEC-103", "flag", f".{attr}() writes a file", node, capability="fs_write")
            elif attr in {"read_text", "read_bytes"}:
                self._add("FF-SEC-105", "flag", f".{attr}() reads a file", node, capability="fs_read")
            elif attr in {"glob", "rglob"}:
                self._add("FF-SEC-105", "flag", f".{attr}() reads the filesystem", node, capability="fs_read")

    def _flag_open(self, node: ast.Call):
        mode_node = (
            node.args[1] if len(node.args) >= 2 else next((kw.value for kw in node.keywords if kw.arg == "mode"), None)
        )
        if mode_node is None:
            self._add("FF-SEC-105", "flag", "open() reads a file", node, capability="fs_read")
            return
        if isinstance(mode_node, ast.Constant) and isinstance(mode_node.value, str):
            if any(ch in mode_node.value for ch in "wax+"):
                self._add(
                    "FF-SEC-103", "flag", f"open(..., {mode_node.value!r}) writes a file", node, capability="fs_write"
                )
            else:
                self._add(
                    "FF-SEC-105", "flag", f"open(..., {mode_node.value!r}) reads a file", node, capability="fs_read"
                )
        else:
            self._add("FF-SEC-103", "flag", "open() with a non-literal mode", node, capability="fs_write")

    def _check_env_call(self, node: ast.Call, canonical):
        if canonical == "os.getenv":
            self._add("FF-SEC-104", "flag", "os.getenv() reads an environment variable", node, capability="env_read")
        elif canonical == "os.environ.get":
            self._add(
                "FF-SEC-104", "flag", "os.environ.get() reads an environment variable", node, capability="env_read"
            )

    def _check_dynamic_call(self, node: ast.Call, canonical):
        if canonical in {"setattr", "delattr"} and len(node.args) >= 2:
            name_arg = node.args[1]
            if not (isinstance(name_arg, ast.Constant) and isinstance(name_arg.value, str)):
                self._add(
                    "FF-SEC-106",
                    "flag",
                    f"{canonical}() with a non-constant attribute name",
                    node,
                    capability="dynamic_code",
                )
        elif canonical == "types.FunctionType":
            self._add(
                "FF-SEC-106", "flag", "types.FunctionType builds a function object", node, capability="dynamic_code"
            )

    def _check_serialization_call(self, node: ast.Call, canonical):
        if canonical in {"pickle.load", "dill.load", "dill.loads", "marshal.load", "shelve.open"}:
            self._add("FF-SEC-107", "flag", f"{canonical}() deserializes data", node, capability="serialization")
        elif canonical == "yaml.load" and not _yaml_is_safe(node):
            self._add("FF-SEC-107", "flag", "yaml.load() without SafeLoader", node, capability="serialization")

    def _check_subscript(self, node: ast.Subscript):
        value = node.value
        if isinstance(value, ast.Call) and self._canonical(value.func) in {"globals", "vars"}:
            self._add("FF-SEC-004", "deny", "indexing globals()/vars() to resolve a name (reflection)", node)
        elif isinstance(value, ast.Name) and value.id == "__builtins__":
            self._add("FF-SEC-004", "deny", "indexing __builtins__ to resolve a builtin (reflection)", node)
        elif self._is_os_environ(value):
            self._add(
                "FF-SEC-104", "flag", "os.environ[...] reads an environment variable", node, capability="env_read"
            )

    def _check_attribute(self, node: ast.Attribute):
        if node.attr in {"secret_value", "SecretSelector", "AvailableSecrets"}:
            self._add("FF-SEC-108", "flag", f"references .{node.attr} (secret access)", node, capability="secrets")
        elif self._canonical(node) == "sys.argv":
            self._add("FF-SEC-104", "flag", "sys.argv reads process arguments", node, capability="env_read")

    def _check_name(self, node: ast.Name):
        if node.id in {"SecretSelector", "AvailableSecrets"}:
            self._add("FF-SEC-108", "flag", f"references {node.id} (secret access)", node, capability="secrets")

    def _check_constant(self, node: ast.Constant):
        value = node.value
        if isinstance(value, bytes):
            value = value.decode("latin-1", "ignore")
        if isinstance(value, str) and _is_opaque_blob(value):
            self._add("FF-SEC-011", "deny", f"opaque literal ({len(value)} chars) hides a payload from review", node)

    def _emit_leftover_serialization(self):
        for site_id, node in self.decode_sites.items():
            if site_id in self.consumed_decodes:
                continue
            canonical = self._canonical(node.func) or "decode"
            self._add(
                "FF-SEC-107",
                "flag",
                f"{canonical}() decodes data (not reaching exec)",
                node,
                capability="serialization",
            )

    def _env_read_lint(self, capabilities):
        if "env_read" in capabilities and "secrets" not in capabilities:
            self.findings.append(
                Finding(
                    rule_id="FF-SEC-201",
                    severity="lint",
                    message="reads the environment but declares no secrets capability",
                )
            )


def scan_source(source: str) -> ScanReport:
    """Scan node source and return a :class:`ScanReport`. Never raises on bad input."""
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        finding = Finding(
            rule_id="FF-SEC-000",
            severity="deny",
            message=f"source does not parse: {exc}",
            lineno=getattr(exc, "lineno", 0) or 0,
            col=getattr(exc, "offset", 0) or 0,
        )
        return ScanReport(findings=[finding], deny_count=1, passed=False)
    return _Scanner(source, tree).run()
