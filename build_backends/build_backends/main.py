import os
import subprocess
import platform
import shutil
from pathlib import Path
from typing import List
import inspect  # Added for finding package paths
import connectorx # Added to find its path

def merge_directories(directories: List[str], target_dir: str, cleanup_after_merge: bool = True):
    """
    Merge all files from specified directories into a new target directory.
    After successful merge, optionally removes the original directories.
    """
    # Create target directory
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    print(f"Merging into target directory: {target_dir}")
    for directory in directories:
        source_path = Path(directory)
        if source_path.exists() and source_path.is_dir():
            print(f"Copying contents from: {directory}")
            shutil.copytree(str(source_path), target_dir, dirs_exist_ok=True)
        else:
             print(f"Warning: Source directory not found or not a directory, skipping: {directory}")
    print('Finished merging directories:', directories)

    if cleanup_after_merge:
        print('Cleaning up source directories:', directories)
        for directory in directories:
             source_path = Path(directory)
             if source_path.exists() and source_path.is_dir():
                print(f"Removing directory: {directory}")
                shutil.rmtree(str(source_path))


def create_spec_file(directory, script_name, output_name, hidden_imports):
    """Create an optimized spec file for faster startup"""

    # --- Start: Add logic to find connectorx binary ---
    analysis_binaries = []
    try:
        connectorx_pkg_dir = os.path.dirname(inspect.getfile(connectorx))
        # Common binary names (adjust if connectorx uses something different)
        binary_name = "_native.pyd" if platform.system() == "Windows" else "_native.so"
        binary_path = os.path.join(connectorx_pkg_dir, binary_name)

        if os.path.exists(binary_path):
            # The format is [(source_path, destination_folder_in_bundle)]
            # Placing it in 'connectorx' folder within the bundle
            analysis_binaries.append((binary_path, 'connectorx'))
            print(f"Found and adding connectorx binary: {binary_path} to be placed in 'connectorx' folder")
        else:
            print(f"WARNING: Could not find connectorx binary at expected path: {binary_path}")
            # Optional: Try PyInstaller's helper (might work, might not)
            # try:
            #     from PyInstaller.utils.hooks import collect_dynamic_libs
            #     print("Attempting fallback using collect_dynamic_libs for connectorx...")
            #     analysis_binaries += collect_dynamic_libs('connectorx', destdir='connectorx')
            # except Exception as e_cdl:
            #     print(f"Fallback collect_dynamic_libs failed: {e_cdl}")

    except ImportError:
        print("WARNING: connectorx module not found during spec file generation. Cannot add its binary.")
    except Exception as e:
         print(f"WARNING: Error finding/processing connectorx binary path: {e}")
    # --- End: Add logic ---


    spec_content = f'''
# -*- mode: python ; coding: utf-8 -*-
# ^ Instructs PyInstaller about encoding

import sys
import os # Ensure os is imported within the spec context if used below
from PyInstaller.utils.hooks import collect_submodules, collect_data_files

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

a = Analysis(
    [os.path.join(r'{directory}', r'{script_name}')], # Use raw strings for paths
    pathex=[], # Explicitly empty or add project paths if needed
    binaries={analysis_binaries}, # <-- Use the list created above
    datas=numpy_datas + pyarrow_datas,
    hiddenimports={hidden_imports} + snowflake_imports + [
        # Ensure base packages are included if needed by collected submodules/data
        'numpy',
        'numpy.core._dtype_ctypes', # Check if really needed
        'numpy.core._methods',      # Check if really needed
        # 'numpy._pyarray_api', # <-- REMOVED - Caused build errors
        'pyarrow',
        'pyarrow.lib',
        'fastexcel',
        # Add other necessary base packages if submodules were collected
        'snowflake.connector'
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[
        'tkinter', # Good excludes for server apps
        'PIL',
        'pytest',
        'unittest',
        'FixTk',
        'tcl',
        'tk',
        '_tkinter',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None, # Default cipher is usually fine
    noarchive=False # Setting to False is standard for one-folder builds
)

pyz = PYZ(a.pure, compress_level=9) # Compressing the PYZ archive

exe = EXE(
    pyz,
    a.scripts,
    [], # Binaries are handled by COLLECT stage below
    exclude_binaries=True, # Exclude from EXE, include in COLLECT
    name='{output_name}',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False, # Set to True in production for smaller size?
    upx=False, # UPX often causes issues with complex packages
    console=True, # Assuming console applications
    optimize=1 # Corresponds to PYTHONOPTIMIZE=1
)

# COLLECT stage gathers everything into the output folder
coll = COLLECT(
    exe,
    a.binaries, # Include binaries found by Analysis (like connectorx)
    a.zipfiles, # Include the PYZ archive
    a.datas,    # Include data files (like numpy/pyarrow data)
    strip=False,
    upx=False,
    upx_exclude=[],
    name='{output_name}' # Name of the output folder
)
'''
    spec_path = f'{output_name}.spec'
    # Ensure directory path is properly escaped if needed, but should be okay with os.path.join
    spec_content_formatted = spec_content.format(
        directory=directory.replace('\\', '/'), # Ensure forward slashes for spec file paths
        script_name=script_name,
        output_name=output_name,
        hidden_imports=repr(hidden_imports), # Use repr for list representation in f-string
        analysis_binaries=repr(analysis_binaries) # Use repr for list representation
    )

    print(f"Writing spec file to: {spec_path}")
    with open(spec_path, 'w', encoding='utf-8') as f: # Specify encoding
        f.write(spec_content_formatted)
    return spec_path


def build_backend(directory, script_name, output_name, hidden_imports=None):
    """Builds a single backend component using PyInstaller"""
    if hidden_imports is None:
        hidden_imports = []

    try:
        # Ensure the output directory exists before creating the spec file in it potentially
        # Although spec is created in current dir by default
        # dist_path = Path("./services_dist")
        # dist_path.mkdir(parents=True, exist_ok=True) # Ensure distpath exists

        work_path_base = "/tmp" if platform.system() != "Windows" else os.getenv('TEMP', '.')
        work_path = os.path.join(work_path_base, 'pyinstaller_work', output_name)
        dist_path = os.path.abspath("./services_dist") # Use absolute path for distpath

        print(f"Generating spec file for {output_name}...")
        spec_path = create_spec_file(directory, script_name, output_name, hidden_imports)
        abs_spec_path = os.path.abspath(spec_path)
        print(f"Generated spec file: {abs_spec_path}")

        env = os.environ.copy()
        env['PYTHONOPTIMIZE'] = "1"

        command = [
            "pyinstaller",
            "--noconfirm", # Equivalent to -y
            "--clean", # Clean cache and temporary files
            "--distpath", dist_path,
            "--workpath", work_path,
            abs_spec_path # Use absolute path to spec file
        ]

        print(f"Building {output_name} with command: {' '.join(command)}")
        # Run from the directory containing the script, or ensure paths in spec are absolute/relative?
        # Running from the script's location is usually safest if paths are relative
        # Or ensure all paths within the spec are absolute or relative to the spec file location
        process = subprocess.run(command, check=True, env=env, capture_output=True, text=True)
        print(f"PyInstaller stdout for {output_name}:\n{process.stdout}")
        print(f"PyInstaller stderr for {output_name}:\n{process.stderr}")

        print(f"Removing spec file: {abs_spec_path}")
        os.remove(abs_spec_path)

        return True

    except subprocess.CalledProcessError as e:
        print(f"ERROR: PyInstaller failed while building {script_name} for {output_name}.")
        print(f"Return Code: {e.returncode}")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Stderr:\n{e.stderr}")
        print(f"Stdout:\n{e.stdout}")
        # Optionally remove spec file even on error
        if 'abs_spec_path' in locals() and os.path.exists(abs_spec_path):
             try:
                 os.remove(abs_spec_path)
             except OSError as ose:
                 print(f"Warning: Could not remove spec file on error: {ose}")
        return False
    except Exception as e:
        print(f"ERROR: Unexpected error during build of {output_name}: {e}")
        # Optionally remove spec file even on error
        if 'abs_spec_path' in locals() and os.path.exists(abs_spec_path):
             try:
                 os.remove(abs_spec_path)
             except OSError as ose:
                 print(f"Warning: Could not remove spec file on error: {ose}")
        return False


def combine_packages():
    """Reorganize the services_dist directory to have shared dependencies"""
    dist_dir = Path("services_dist").resolve() # Use resolved absolute path
    shared_internal = dist_dir / "_internal"
    core_internal = dist_dir / "flowfile_core" / "_internal"
    worker_internal = dist_dir / "flowfile_worker" / "_internal"

    print("\n--- Starting Package Combination ---")
    merge_directories([str(core_internal), str(worker_internal)], str(shared_internal), cleanup_after_merge=False) # Keep originals for now

    # Move executables and cleanup original folders carefully
    for project in ["flowfile_worker", "flowfile_core"]:
        src_dir = dist_dir / project
        if src_dir.exists() and src_dir.is_dir():
            exe_name = project + (".exe" if platform.system() == "Windows" else "")
            src_exe = src_dir / exe_name
            target_exe = dist_dir / exe_name

            if src_exe.exists() and src_exe.is_file():
                print(f"Moving executable: {src_exe} -> {target_exe}")
                # Ensure target doesn't exist or is removed before move
                if target_exe.exists():
                    if target_exe.is_file():
                        target_exe.unlink()
                    elif target_exe.is_dir(): # Should not happen if names are correct
                         shutil.rmtree(target_exe)
                shutil.move(str(src_exe), str(target_exe))
            else:
                 print(f"Warning: Executable not found at {src_exe}")

            # Now cleanup the original project folder *after* moving exe and merging _internal
            print(f"Removing original build directory: {src_dir}")
            shutil.rmtree(str(src_dir))
        else:
            print(f"Warning: Source directory for cleanup not found: {src_dir}")

    print("--- Finished Package Combination ---")


def main():
    # Clean previous builds
    dist_dir_path = Path('services_dist')
    if dist_dir_path.exists():
        print(f"Cleaning previous build directory: {dist_dir_path}")
        shutil.rmtree(str(dist_dir_path))

    # Common imports for both projects
    common_imports = [
        "fastexcel",
        "polars",
        "numpy", # Base numpy might still be needed
        # "numpy.core._methods", # Often automatically included if numpy is used
        "pyarrow",
        "snowflake.connector", # Base needed for collected submodules
        # Explicit submodules often needed if dynamically loaded
        "snowflake.connector.snow_logging",
        "snowflake.connector.errors",
        "snowflake.connector.auth", # Add others if used
        "multiprocessing", # Often needed explicitly
        "uvicorn", # Add base uvicorn if needed
        "uvicorn.lifespan",
        "uvicorn.loops",
        "uvicorn.loops.auto",
        "uvicorn.protocols",
        "uvicorn.protocols.http",
        "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets",
        "uvicorn.protocols.websockets.auto",
        "uvicorn.server",
        "uvicorn.config",
        "passlib", # Base passlib
        "passlib.handlers", # If specific handlers are used
        "passlib.handlers.bcrypt",
        "connectorx", # Explicitly add connectorx base
    ]

    # Ensure script paths are correct relative to the script's location
    script_dir = Path(__file__).parent.resolve()
    worker_dir = script_dir / "flowfile_worker" / "flowfile_worker"
    core_dir = script_dir / "flowfile_core" / "flowfile_core"

    # Build both projects
    builds_successful = True

    # Build flowfile_worker
    print("\n--- Building flowfile_worker ---")
    if not build_backend(
            directory=str(worker_dir), # Pass absolute or correct relative path
            script_name="main.py",
            output_name="flowfile_worker",
            hidden_imports=list(set(common_imports)) # Ensure unique list
    ):
        builds_successful = False
        print("!!! Build FAILED for flowfile_worker !!!")

    # Build flowfile_core
    if builds_successful: # Only proceed if previous build was successful
        print("\n--- Building flowfile_core ---")
        if not build_backend(
                directory=str(core_dir), # Pass absolute or correct relative path
                script_name="main.py",
                output_name="flowfile_core",
                hidden_imports=list(set(common_imports)) # Ensure unique list
        ):
            builds_successful = False
            print("!!! Build FAILED for flowfile_core !!!")

    if builds_successful:
        print("\n--- All builds successful ---")
        print("Reorganizing services_dist directory...")
        combine_packages()
        print("\nBuild complete! Final structure created in services_dist/")
    else:
        print("\n--- Build process finished with errors ---")


if __name__ == "__main__":
    # Ensure connectorx is importable when this script runs
    try:
        import connectorx
    except ImportError:
        print("FATAL ERROR: Cannot import 'connectorx'.")
        print("Please ensure 'connectorx' is installed in the environment running this build script.")
        exit(1)

    main()