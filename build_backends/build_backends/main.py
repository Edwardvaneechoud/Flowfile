import os
import subprocess
import platform
import shutil
from pathlib import Path
from typing import List


def merge_directories(directories: List[str], target_dir: str, cleanup_after_merge: bool = True):
    """
    Merge all files from two folders into a new target directory.
    After successful merge, removes the original folders.
    """
    # Create target directory
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    for directory in directories:
        if os.path.exists(directory):
            shutil.copytree(directory, target_dir, dirs_exist_ok=True)
    print('Merged directories:', directories, 'into', target_dir)
    if cleanup_after_merge:
        print('Cleaning up directories:', directories)
        for directory in directories:
            if os.path.exists(directory):
                shutil.rmtree(directory)


def create_spec_file(directory, script_name, output_name, hidden_imports):
    """Create an optimized spec file for faster startup"""
    spec_content = f'''
import sys
import os
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

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

# Collect minimal snowflake dependencies
snowflake_imports = collect_submodules('snowflake.connector', 
    filter=lambda name: any(x in name for x in [
        'connection',
        'errors',
        'snow_logging',
        'auth',
        'network'
    ])
)

# Collect numpy and pyarrow data files
numpy_datas = collect_data_files('numpy')
pyarrow_datas = collect_data_files('pyarrow')
connectorx_datas = get_connectorx_metadata()

# Create runtime hook file
with open('connectorx_hook.py', 'w') as f:
    f.write(create_runtime_hook())

a = Analysis(
    [r'{os.path.join(directory, script_name)}'],
    binaries=[],
    datas=numpy_datas + pyarrow_datas + connectorx_datas,
    hiddenimports={hidden_imports} + snowflake_imports + [
        'numpy',
        'numpy.core._dtype_ctypes',
        'numpy.core._methods',
        'numpy._pyarray_api',
        'pyarrow',
        'pyarrow.lib',
        'fastexcel',
        'importlib.metadata',
    ],
    excludes=[
        'tkinter',
        'PIL',
        'pytest',
        'unittest'
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
    optimize=1
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='{output_name}'
)
'''
    spec_path = f'{output_name}.spec'
    with open(spec_path, 'w') as f:
        f.write(spec_content)
    return spec_path

def build_backend(directory, script_name, output_name, hidden_imports=None):
    try:
        spec_path = create_spec_file(directory, script_name, output_name, hidden_imports)

        env = os.environ.copy()
        env['PYTHONOPTIMIZE'] = "1"

        command = [
            "pyinstaller",
            "--clean",
            "-y",
            "--dist", "./services_dist",
            "--workpath", "/tmp" if platform.system() != "Windows" else os.path.join(os.getenv('TEMP'), 'pyinstaller'),
            spec_path
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
    """Reorganize the services_dist directory to have shared dependencies"""
    dist_dir = "services_dist"
    shared_internal = os.path.join(dist_dir, "_internal")
    core_internal = os.path.join(dist_dir, "flowfile_core", "_internal")
    worker_internal = os.path.join(dist_dir, "flowfile_worker", "_internal")
    merge_directories([core_internal, worker_internal], shared_internal, False)

    for project in ["flowfile_worker", "flowfile_core"]:
        src_dir = os.path.join(dist_dir, project)
        if os.path.exists(src_dir) and os.path.isdir(src_dir):
            # Move executable
            exe_name = project + ".exe" if platform.system() == "Windows" else project
            src_exe = os.path.join(src_dir, exe_name)
            temp_target_exe = os.path.join(dist_dir, "_" + exe_name)
            target_exe = os.path.join(dist_dir,  exe_name)
            if os.path.exists(src_exe) and os.path.isfile(src_exe):
                # Instead of removing, overwrite the target
                shutil.move(src_exe, temp_target_exe)
            if os.path.exists(target_exe) and os.path.isdir(target_exe):
                shutil.rmtree(target_exe)
            shutil.move(temp_target_exe, target_exe)
            if platform.system() == "Windows" and os.path.exists(os.path.join(dist_dir, project)):
                shutil.rmtree(os.path.join(dist_dir, project))

def main():
    # Clean previous builds
    for dir_name in ['services_dist']:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)

    # Common imports for both projects
    common_imports = [
        "fastexcel",
        "polars",
        "numpy",
        "numpy.core._methods",
        "pyarrow",
        "snowflake.connector",
        "snowflake.connector.snow_logging",
        "snowflake.connector.errors",
        "multiprocessing",
        "uvicorn.protocols.http",
        "uvicorn.protocols.websockets",
        "passlib.handlers.bcrypt",
        "connectorx",
    ]

    # Build both projects
    builds_successful = True

    # Build flowfile_worker

    if not build_backend(
            directory=os.path.join("flowfile_worker", "flowfile_worker"),
            script_name="main.py",
            output_name="flowfile_worker",
            hidden_imports=common_imports
    ):
        builds_successful = False

    # Build flowfile_core

    if not build_backend(
            directory=os.path.join("flowfile_core", "flowfile_core"),
            script_name="main.py",
            output_name="flowfile_core",
            hidden_imports=common_imports
    ):
        builds_successful = False

    if builds_successful:
        print("Reorganizing services_dist directory...")
        combine_packages()
        print("Build complete! Final structure created in services_dist/")


# if __name__ == "__main__":
#     main()