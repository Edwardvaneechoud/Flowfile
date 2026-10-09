"""Late binding of the package's swappable collaborators."""


def root():
    """The package root, where swappable collaborators are bound: the worker fetchers, the kernel
    manager accessor, the custom-node registry, the DB session factory and the remote Delta writers.
    Tests and the notebook build mode patch them there, so submodules resolve them through this
    accessor at call time instead of binding their own copies."""
    import flowfile_core.flowfile.flow_graph as package

    return package
