"""Late binding of the package's swappable collaborators."""


def root():
    """The package root, where the collaborators tests patch are bound; read them here at call time."""
    import flowfile_core.flowfile.flow_graph as package

    return package
