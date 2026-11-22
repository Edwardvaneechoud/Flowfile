"""Step-level diff and merge of saved flow documents.

Works on a flow as the plain dict a save writes (``FlowfileData`` dumped to JSON-mode data), never on a
``FlowGraph``, so a git merge driver, the CLI and the hub can call it without starting core. Import-pure:
stdlib and PyYAML only, nothing from ``flowfile_core``, and ``shared/__init__.py`` does not import it.

Modules:

* ``edges``: a node's inputs read once from the target side of the document.
* ``relabel``: rewrite step, group and comment ids everywhere a document names them.
* ``placement``: free canvas slots for steps added to a canvas that already holds steps.
* ``yaml_io``: the YAML styles of a saved flow and of a project file.
"""
