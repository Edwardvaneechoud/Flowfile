"""Guards for the suite's core-port log sink (``tests/core_log_sink.py``).

The sink only pays off when it actually starts, and it must never start in a
session that really binds the core port — both directions are pinned here.
"""

import http.client
import socket
import types

import pytest

from tests.core_log_sink import _PORT_CLAIMING_FIXTURES, _PORT_CLAIMING_MODULES, CoreLogSink, session_claims_core_port


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _item(fspath="/repo/flowfile_core/tests/test_plain.py", fixturenames=(), module=None):
    return types.SimpleNamespace(fspath=fspath, fixturenames=fixturenames, module=module)


def _auth_e2e_item(module):
    return _item(fspath="/repo/flowfile_core/tests/test_auth_e2e.py", module=module)


class TestSessionClaim:
    def test_plain_session_does_not_claim_the_port(self):
        assert session_claims_core_port([_item(), _item()]) is False

    def test_collected_auth_e2e_that_will_not_run_leaves_the_sink_free(self):
        """The CI Windows case: collected, but skipped at fixture time."""
        module = types.SimpleNamespace(is_docker_available=lambda: False)
        items = [_item(), _auth_e2e_item(module), _auth_e2e_item(module)]
        assert session_claims_core_port(items) is False

    def test_collected_auth_e2e_that_will_run_claims_the_port(self):
        module = types.SimpleNamespace(is_docker_available=lambda: True)
        assert session_claims_core_port([_item(), _auth_e2e_item(module)]) is True

    def test_missing_module_predicate_fails_closed(self):
        assert session_claims_core_port([_auth_e2e_item(types.SimpleNamespace())]) is True

    def test_raising_module_predicate_fails_closed(self):
        def boom():
            raise RuntimeError("docker exploded")

        assert session_claims_core_port([_auth_e2e_item(types.SimpleNamespace(is_docker_available=boom))]) is True

    @pytest.mark.parametrize("docker_available", [True, False])
    def test_kernel_fixture_claim_follows_the_shared_docker_probe(self, monkeypatch, docker_available):
        from tests import flowfile_core_test_utils

        monkeypatch.setattr(flowfile_core_test_utils, "is_docker_available", lambda: docker_available)
        items = [_item(fixturenames=("tmp_path", "kernel_manager_with_core"))]
        assert session_claims_core_port(items) is docker_available

    def test_claimers_still_expose_the_predicates_the_claim_reads(self):
        """A renamed predicate must fail here, not silently disable the sink."""
        from tests import flowfile_core_test_utils, test_auth_e2e

        assert callable(getattr(test_auth_e2e, _PORT_CLAIMING_MODULES["test_auth_e2e.py"]))
        module_name, attr = _PORT_CLAIMING_FIXTURES["kernel_manager_with_core"]
        assert module_name == flowfile_core_test_utils.__name__
        assert callable(getattr(flowfile_core_test_utils, attr))

    def test_claim_runs_after_pytest_deselection(self, pytestconfig):
        """-m "not kernel" must have removed its items before the claim looks."""
        hook = pytestconfig.pluginmanager.hook.pytest_collection_modifyitems
        call_order = [impl.plugin_name for impl in reversed(hook.get_hookimpls())]
        conftest = [name for name in call_order if name.endswith("flowfile_core\\tests\\conftest.py")
                    or name.endswith("flowfile_core/tests/conftest.py")]
        assert conftest, f"core conftest not registered for the hook: {call_order}"
        assert call_order.index(conftest[0]) > call_order.index("mark")


class TestSinkServer:
    def test_serves_raw_logs_and_404s_everything_else(self):
        sink = CoreLogSink(port=_free_port())
        assert sink.start() is True
        try:
            conn = http.client.HTTPConnection(sink.host, sink.port, timeout=5)
            conn.request("POST", "/raw_logs", body=b'{"message":"hi"}')
            assert conn.getresponse().status == 200
            conn = http.client.HTTPConnection(sink.host, sink.port, timeout=5)
            conn.request("POST", "/some_other_route", body=b"{}")
            assert conn.getresponse().status == 404
            conn = http.client.HTTPConnection(sink.host, sink.port, timeout=5)
            conn.request("GET", "/docs")
            assert conn.getresponse().status == 404
        finally:
            sink.stop()

    def test_does_not_start_when_the_port_is_taken(self):
        port = _free_port()
        with socket.socket() as holder:
            holder.bind(("127.0.0.1", port))
            holder.listen(1)
            sink = CoreLogSink(port=port)
            try:
                assert sink.start() is False
            finally:
                sink.stop()
