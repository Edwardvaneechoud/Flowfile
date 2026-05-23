import fnmatch
import os
import platform
import shutil
import subprocess
from pathlib import Path

# Locale subdirectories we keep inside _internal/. Everything else under any
# `locale/` directory is pruned to save ~10–30 MB across polars / pyarrow / numpy.
KEEP_LOCALES = {"C", "en", "en_US", "en_US.UTF-8", "en_US.utf8"}


def merge_directories(directories: list[str], target_dir: str, cleanup_after_merge: bool = True):
    """
    Merge all files from two folders into a new target directory.
    After successful merge, removes the original folders.
    """
    # Create target directory
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    for directory in directories:
        if os.path.exists(directory):
            shutil.copytree(directory, target_dir, dirs_exist_ok=True)
    print("Merged directories:", directories, "into", target_dir)
    if cleanup_after_merge:
        print("Cleaning up directories:", directories)
        for directory in directories:
            if os.path.exists(directory):
                shutil.rmtree(directory)


def create_spec_file(directory, script_name, output_name, hidden_imports):
    """Create an optimized spec file for faster startup"""
    spec_content = f'''
import sys
import os
from PyInstaller.utils.hooks import collect_data_files, collect_dynamic_libs, collect_submodules

# Add hook to fix connectorx metadata
def get_connectorx_metadata():
    print("Collecting connectorx metadata...")
    try:
        import site
        import connectorx

        # Find the site-packages directory
        site_packages = site.getsitepackages()[0]
        print(f"Site-packages directory: {{site_packages}}")

        # Try both common metadata formats with glob to catch any version
        import glob
        metadata_locations = []

        # Look for dist-info directories
        dist_info_pattern = os.path.join(site_packages, 'connectorx*.dist-info')
        for dist_info in glob.glob(dist_info_pattern):
            metadata_locations.append(dist_info)

        # Look for egg-info directories
        egg_info_pattern = os.path.join(site_packages, 'connectorx*.egg-info')
        for egg_info in glob.glob(egg_info_pattern):
            metadata_locations.append(egg_info)

        # Also try looking in the parent directory of the connectorx package
        connectorx_dir = os.path.dirname(connectorx.__file__)
        parent_dir = os.path.dirname(connectorx_dir)

        dist_info_pattern = os.path.join(parent_dir, 'connectorx*.dist-info')
        for dist_info in glob.glob(dist_info_pattern):
            metadata_locations.append(dist_info)

        egg_info_pattern = os.path.join(parent_dir, 'connectorx*.egg-info')
        for egg_info in glob.glob(egg_info_pattern):
            metadata_locations.append(egg_info)

        found_metadata = []
        for loc in metadata_locations:
            if os.path.exists(loc):
                dest_name = os.path.basename(loc)
                found_metadata.append((loc, dest_name))
                print(f"Found metadata at {{loc}}")

        if found_metadata:
            return found_metadata

        # If we can't find the metadata, create a fake one
        print("No connectorx metadata found, creating manual metadata...")
        import tempfile
        temp_dir = tempfile.mkdtemp()
        fake_meta_dir = os.path.join(temp_dir, 'connectorx-0.4.3.dist-info')
        os.makedirs(fake_meta_dir, exist_ok=True)

        # Create minimal METADATA file
        with open(os.path.join(fake_meta_dir, 'METADATA'), 'w') as f:
            f.write("""Metadata-Version: 2.1
Name: connectorx
Version: 0.4.3
Summary: ConnectorX: Fast and Reliable Data Loading
""")

        # Return the fake metadata directory
        print(f"Created fake metadata at {{fake_meta_dir}}")
        return [(fake_meta_dir, 'connectorx-0.4.3.dist-info')]
    except Exception as e:
        print(f"Error collecting connectorx metadata: {{e}}")
        return []

# Add runtime hook to handle connectorx metadata issues
def create_runtime_hook():
    return """
# Runtime hook to handle connectorx metadata issues
import sys
import importlib.metadata

# Store original version function
original_version = importlib.metadata.version

# Create patched version function
def patched_version(distribution_name):
    try:
        return original_version(distribution_name)
    except (importlib.metadata.PackageNotFoundError, StopIteration):
        # Handle specific packages
        if distribution_name == 'connectorx':
            return '0.4.3'  # Hardcode the version
        # Let other package errors propagate normally
        raise

# Apply the patch
importlib.metadata.version = patched_version
print("Applied connectorx metadata patch")
"""

# Collect Alembic migration files for runtime access
def get_alembic_datas():
    \"\"\"Collect Alembic migration files for PyInstaller bundling.\"\"\"
    import os as _os
    alembic_dir = _os.path.join("flowfile_core", "flowfile_core", "alembic")
    alembic_ini = _os.path.join("flowfile_core", "flowfile_core", "alembic.ini")
    datas = []
    if _os.path.isdir(alembic_dir):
        datas.append((alembic_dir, _os.path.join("flowfile_core", "alembic")))
    if _os.path.isfile(alembic_ini):
        datas.append((alembic_ini, "flowfile_core"))
    return datas

# Collect numpy and pyarrow data files
numpy_datas = collect_data_files('numpy')
pyarrow_datas = collect_data_files('pyarrow')
connectorx_datas = get_connectorx_metadata()
alembic_datas = get_alembic_datas()

# Polars plugins that have subpackages or compiled extensions. The plain
# `hiddenimports=['polars_ds']` directive only adds the top-level — it does
# NOT recurse into subpackages (polars_ds.exprs, polars_ds.eda, ...) and it
# does not necessarily bundle the .abi3.so binary. Use collect_submodules +
# collect_data_files + collect_dynamic_libs to grab everything.
_polars_plugins = [
    'polars_ds',
    'polars_expr_transformer',
    'polars_grouper',
    'polars_simed',
    'polars_distance',
]
plugin_hiddenimports = []
plugin_datas = []
plugin_binaries = []
for _pkg in _polars_plugins:
    try:
        plugin_hiddenimports += collect_submodules(_pkg)
        plugin_datas += collect_data_files(_pkg)
        plugin_binaries += collect_dynamic_libs(_pkg)
    except Exception as _e:
        print(f"WARN: could not collect plugin {{_pkg}}: {{_e}}")

# Create runtime hook file
with open('connectorx_hook.py', 'w') as f:
    f.write(create_runtime_hook())

a = Analysis(
    [r'{os.path.join(directory, script_name)}'],
    binaries=plugin_binaries,
    datas=numpy_datas + pyarrow_datas + connectorx_datas + alembic_datas + plugin_datas,
    hiddenimports={hidden_imports} + plugin_hiddenimports + [
        'numpy',
        'numpy.core._dtype_ctypes',
        'numpy.core._methods',
        'pyarrow',
        'pyarrow.lib',
        'fastexcel',
        'importlib.metadata',
    ],
    excludes=[
        # Standard library bloat we never use.
        'tkinter',
        'PIL',
        'pytest',
        'unittest',
        'pkg_resources',
        'pdb',
        'doctest',
        'turtle',
        'IPython',
        # ML stack — NOT a project dependency. Lives in some dev/conda envs
        # (e.g. via sentence-transformers) and gets pulled in by PyInstaller's
        # contrib hooks. ~530 MB of nothing we use. If a future feature
        # genuinely needs torch/sklearn, add it to pyproject.toml first and
        # remove the relevant line from here.
        'torch',
        'torchvision',
        'torchaudio',
        'torchao',
        'torchgen',
        'transformers',
        'sentence_transformers',
        'huggingface_hub',
        'datasets',
        'accelerate',
        'tokenizers',
        'sentencepiece',
        'tensorboard',
        'sklearn',
        'scikit-learn',
        'scipy',
        'tensorflow',
        # PyArrow optional subsystems we don't use (we use parquet/csv/ipc).
        'pyarrow.tests',
        'pyarrow.flight',
        'pyarrow.cuda',
        'pyarrow.gandiva',
        'pyarrow.orc',
        'pyarrow.parquet.encryption',
        # NumPy testing / build tooling.
        'numpy.testing',
        'numpy.f2py',
        'numpy.distutils',
        # Polars testing helpers — we never ship tests in production.
        'polars.testing',
        # Pandas is intentionally not a dependency — make sure it never sneaks in.
        'pandas',
        # matplotlib is installed in the dev env but no production code imports
        # it (only test files). PyInstaller's hooks pull it in as a transitive,
        # ~12 MB. If a future feature needs it, add to pyproject.toml first.
        'matplotlib',
    ],
    runtime_hooks=['connectorx_hook.py'],
    noarchive=False,
)

pyz = PYZ(a.pure, compress_level=9)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='{output_name}',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
    optimize=1,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='{output_name}',
)
'''
    spec_path = f"{output_name}.spec"
    with open(spec_path, "w") as f:
        f.write(spec_content)
    return spec_path


def build_backend(directory, script_name, output_name, hidden_imports=None):
    try:
        spec_path = create_spec_file(directory, script_name, output_name, hidden_imports)

        env = os.environ.copy()
        env["PYTHONOPTIMIZE"] = "1"

        command = [
            "pyinstaller",
            "--clean",
            "-y",
            "--dist",
            "./services_dist",
            "--workpath",
            "/tmp" if platform.system() != "Windows" else os.path.join(os.getenv("TEMP"), "pyinstaller"),
            spec_path,
        ]

        print(f"Building {output_name}...")
        subprocess.run(command, check=True, env=env)
        os.remove(spec_path)

        return True

    except subprocess.CalledProcessError as e:
        print(f"Error while building {script_name}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def combine_packages():
    """Reorganize the services_dist directory to have shared dependencies.

    PyInstaller onedir emits services_dist/<name>/{<name>,_internal/}. We move
    each <name> executable up to services_dist/<name> and merge the two
    `_internal/` directories into one shared services_dist/_internal/ so that
    polars/pyarrow/numpy/etc. are only stored once.
    """
    dist_dir = "services_dist"
    shared_internal = os.path.join(dist_dir, "_internal")
    core_internal = os.path.join(dist_dir, "flowfile_core", "_internal")
    worker_internal = os.path.join(dist_dir, "flowfile_worker", "_internal")
    merge_directories([core_internal, worker_internal], shared_internal, False)

    for project in ["flowfile_worker", "flowfile_core"]:
        src_dir = os.path.join(dist_dir, project)
        if os.path.exists(src_dir) and os.path.isdir(src_dir):
            exe_name = project + ".exe" if platform.system() == "Windows" else project
            src_exe = os.path.join(src_dir, exe_name)
            temp_target_exe = os.path.join(dist_dir, "_" + exe_name)
            target_exe = os.path.join(dist_dir, exe_name)
            if os.path.exists(src_exe) and os.path.isfile(src_exe):
                shutil.move(src_exe, temp_target_exe)
            shutil.rmtree(src_dir)
            if os.path.exists(temp_target_exe):
                shutil.move(temp_target_exe, target_exe)


def main():
    # Clean previous builds
    for dir_name in ["services_dist"]:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)

    # Common imports for both projects
    common_imports = [
        "fastexcel",
        "polars",
        "numpy",
        "numpy.core._methods",
        "pyarrow",
        "multiprocessing",
        "uvicorn.protocols.http",
        "uvicorn.protocols.websockets",
        "passlib.handlers.bcrypt",
        "connectorx",
        "alembic",
        # certifi ships cacert.pem; ssl uses it via certifi.where(). The
        # data_downloader builds its SSL context against this so urllib calls
        # in the bundled Python can verify TLS.
        "certifi",
        # Polars plugins. Most are imported at module level elsewhere and get
        # picked up automatically — `polars_ds` is the exception, lazily
        # imported inside ML training functions in shared/ml/trainers.py, so
        # PyInstaller's static scan misses it. List all of them explicitly so
        # future moves to lazy-import don't silently break the bundle.
        # NOTE: polars_simed and polars_distance look unused from a quick grep,
        # but they're imported transitively by pl_fuzzy_frame_match (the fuzzy-
        # join engine — see flowfile_core/schemas/transform_schema.py:8).
        # Removing them breaks startup.
        "polars_ds",
        "polars_distance",
        "polars_grouper",
        "polars_simed",
        "polars_expr_transformer",
    ]

    # Build both projects
    builds_successful = True

    # Build flowfile_worker

    if not build_backend(
        directory=os.path.join("flowfile_worker", "flowfile_worker"),
        script_name="main.py",
        output_name="flowfile_worker",
        hidden_imports=common_imports,
    ):
        builds_successful = False

    # Build flowfile_core

    if not build_backend(
        directory=os.path.join("flowfile_core", "flowfile_core"),
        script_name="main.py",
        output_name="flowfile_core",
        hidden_imports=common_imports,
    ):
        builds_successful = False

    if builds_successful:
        print("Reorganizing services_dist directory...")
        combine_packages()
        prune_locales(Path("services_dist/_internal"))
        prune_unused_data(Path("services_dist/_internal"))
        print("Build complete! Final structure created in services_dist/")


def prune_locales(root: Path) -> None:
    """Remove non-English locale data inside _internal/.

    Polars, pyarrow and a few stdlib helpers ship locale dirs we never use.
    We keep C / en / en_US variants and delete the rest.
    """
    if not root.exists():
        return
    removed_files = 0
    removed_bytes = 0
    for dirpath, _dirnames, filenames in os.walk(root, topdown=False):
        dp = Path(dirpath)
        if dp.name != "locale":
            continue
        for child in dp.iterdir():
            if child.is_dir() and child.name in KEEP_LOCALES:
                continue
            if child.is_dir():
                for f in child.rglob("*"):
                    if f.is_file():
                        removed_bytes += f.stat().st_size
                        removed_files += 1
                shutil.rmtree(child, ignore_errors=True)
            elif child.is_file() and child.suffix == ".mo":
                removed_bytes += child.stat().st_size
                removed_files += 1
                child.unlink(missing_ok=True)
        # Also drop stray .mo files under non-locale dirs that match common
        # translation patterns.
        for f in filenames:
            if fnmatch.fnmatch(f, "*.mo") and not any(k in dirpath for k in KEEP_LOCALES):
                # Be conservative: only touch files literally under .../locale/<lang>/...
                full = dp / f
                if "locale" in full.parts:
                    parent_lang = full.parent.name
                    if parent_lang not in KEEP_LOCALES and parent_lang != "LC_MESSAGES":
                        removed_bytes += full.stat().st_size
                        removed_files += 1
                        full.unlink(missing_ok=True)
    print(f"  pruned {removed_files} locale files ({removed_bytes / (1024 * 1024):.1f} MB)")


def prune_unused_data(root: Path) -> None:
    """Delete data files bundled by transitive deps that we never use.

    googleapiclient ships 568 pre-cached API discovery JSONs (~91 MB) under
    discovery_cache/documents/. The library downloads fresh ones on demand
    when missing, so removing the cache keeps it functional. None of our
    code imports googleapiclient directly — it gets pulled in transitively
    by a google.* dep.
    """
    targets = [root / "googleapiclient" / "discovery_cache" / "documents"]
    removed_bytes = 0
    for target in targets:
        if not target.exists():
            continue
        for f in target.rglob("*"):
            if f.is_file():
                removed_bytes += f.stat().st_size
        shutil.rmtree(target, ignore_errors=True)
        print(f"  removed {target} ({removed_bytes / (1024 * 1024):.1f} MB)")


# if __name__ == "__main__":
#     main()
