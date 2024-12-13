import os
import subprocess
import platform
import shutil

def create_spec_file(directory, script_name, output_name, hidden_imports):
    """Create an optimized spec file for faster startup"""
    spec_content = f'''
import sys
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
    [r'{os.path.join(directory, script_name)}'],
    binaries=[],
    datas=numpy_datas + pyarrow_datas,  # Include numpy and pyarrow data files
    hiddenimports={hidden_imports} + snowflake_imports + [
        'numpy',
        'numpy.core._dtype_ctypes',
        'numpy.core._methods',
        'numpy._pyarray_api',
        'pyarrow',
        'pyarrow.lib',
        'fastexcel',
    ],
    excludes=[
        'matplotlib',
        'tkinter',
        'PIL',
        'pytest',
        'unittest'
    ],
    noarchive=False,  # Changed to False to help with numpy
)

# Use maximum compression level for better size/speed trade-off
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
    optimize=1  # Changed to 1 to prevent numpy docstring issues
)

# Create directory containing all files
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
        # Create optimized spec file
        spec_path = create_spec_file(directory, script_name, output_name, hidden_imports)

        # Set Python optimization environment variable
        # Changed to 1 to prevent numpy docstring issues
        env = os.environ.copy()
        env['PYTHONOPTIMIZE'] = "1"

        # Build command using spec file
        command = [
            "pyinstaller",
            "--clean",
            "-y",
            "--dist", "./dist",
            "--workpath", "/tmp" if platform.system() != "Windows" else os.path.join(os.getenv('TEMP'), 'pyinstaller'),
            spec_path
        ]

        print(f"Building {output_name}...")
        subprocess.run(command, check=True, env=env)

        # Cleanup spec file
        os.remove(spec_path)

        print(f"Successfully built {output_name}")

    except subprocess.CalledProcessError as e:
        print(f"Error while building {script_name}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def main():
    # Clean previous builds
    for dir_name in ['build', 'dist']:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)

    # Minimal required imports
    common_imports = [
        "fastexcel",
        "polars",
        "numpy",
        "numpy.core._methods",
        "pyarrow",
        # Add minimal Snowflake imports
        "snowflake.connector",
        "snowflake.connector.snow_logging",
        "snowflake.connector.errors"
    ]

    # Build flowfile_worker
    worker_imports = [
        "multiprocessing",
        "uvicorn.protocols.http",
        "uvicorn.protocols.websockets"
    ] + common_imports

    build_backend(
        directory=os.path.join("flowfile_worker", "flowfile_worker"),
        script_name="main.py",
        output_name="flowfile_worker",
        hidden_imports=worker_imports
    )

    # Build flowfile_core
    core_imports = [
        "passlib.handlers.bcrypt"
    ] + common_imports

    build_backend(
        directory=os.path.join("flowfile_core", "flowfile_core"),
        script_name="main.py",
        output_name="flowfile_core",
        hidden_imports=core_imports
    )

if __name__ == "__main__":
    main()