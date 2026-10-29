"""The desktop sidecars ship the metadata of every package the worker handshake reports by version.

``shared.compute_protocol.plugin_versions`` reads each through ``importlib.metadata``; a frozen core or worker
without a package's dist-info reports it absent, and placement then refuses every node that needs it.
"""

from build_backends.main import HANDSHAKE_METADATA_PACKAGES
from shared.compute_protocol import PLUGIN_PACKAGES


def test_sidecars_copy_metadata_of_every_handshake_package():
    assert set(HANDSHAKE_METADATA_PACKAGES) == set(PLUGIN_PACKAGES)
