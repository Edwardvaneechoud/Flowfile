# Configuration file for the Sphinx documentation builder.
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

# Project information
project = 'Flowfile'
copyright = '2024, Flowfile Contributors'
author = 'Edward van Eechoud'
release = '0.1.2'  # Update this with your version

# Add any Sphinx extension module names here
extensions = [
    'sphinx.ext.autodoc',      # Core documentation engine
    'sphinx.ext.napoleon',     # Support for Google-style docstrings
    'sphinx.ext.viewcode',     # Add links to source code
    'sphinx.ext.githubpages',  # Generate .nojekyll file
    'myst_parser',            # Markdown support
    'sphinx_copybutton',      # Add copy button to code blocks
    'sphinx.ext.intersphinx',  # Link to other projects' documentation
    'sphinx_autodoc_typehints',  # Better type hints support
    'autoapi.extension',      # API documentation
]

# AutoAPI settings
autoapi_type = 'python'
autoapi_dirs = [
    '../flowfile_core',
    '../flowfile_worker',
]
autoapi_template_dir = '_templates/autoapi'
autoapi_python_class_content = 'both'

# Add any paths that contain templates here
templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# HTML theme settings
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
html_logo = '../.github/images/logo.png'
html_favicon = '../.github/images/logo.png'

# Theme options
html_theme_options = {
    'logo_only': True,
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'style_nav_header_background': '#2980B9',
    # Dark mode colors
    'dark_mode_theme': 'dark',
    'dark_mode_css_variables': {
        'color-brand-primary': '#2980B9',
        'color-brand-content': '#2980B9',
    }
}

# These folders are copied to the documentation's HTML output
html_static_path = ['_static']

# These paths are either relative to html_static_path or fully qualified paths
html_css_files = [
    'css/custom.css',
]

# Intersphinx configuration
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'polars': ('https://pola-rs.github.io/polars/py-polars/html/', None),
}

# MyST Markdown settings
myst_enable_extensions = [
    'colon_fence',
    'deflist',
]

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = True
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = True
napoleon_use_ivar = True
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_type_aliases = None