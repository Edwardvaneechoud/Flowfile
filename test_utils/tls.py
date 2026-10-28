"""Self-signed TLS for tests: a certificate and key, and an HTTPS forwarder in front of a plain-HTTP service."""

import asyncio
import ipaddress
import ssl
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


def self_signed_certificate(common_name: str, dns: Sequence[str] = (), ips: Sequence[str] = ()) -> tuple[str, str]:
    """A self-signed P-256 certificate for *common_name*, valid for two hours, and its private key, both PEM.

    *dns* and *ips* become its subject alternative names; with neither it has none.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=2))
    )
    alternative_names = [x509.DNSName(d) for d in dns] + [x509.IPAddress(ipaddress.ip_address(i)) for i in ips]
    if alternative_names:
        builder = builder.add_extension(x509.SubjectAlternativeName(alternative_names), critical=False)
    cert = builder.sign(key, hashes.SHA256())
    private_key = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode(), private_key.decode()


@contextmanager
def tls_forwarder(upstream: str, folder: Path) -> Iterator[tuple[str, Path]]:
    """An HTTPS endpoint on 127.0.0.1 that forwards every connection to *upstream* (``http://host:port``).

    Its certificate is self-signed for ``localhost`` and ``127.0.0.1`` and written to *folder*. Yields the endpoint's
    ``https://localhost:<port>`` URL and the certificate's path. The Host header passes through, so a URL presigned on
    the endpoint is accepted upstream; a rejected handshake is swallowed, as some tests make one on purpose.
    """
    cert, key = self_signed_certificate("localhost", dns=["localhost"], ips=["127.0.0.1"])
    (folder / "cert.pem").write_text(cert)
    (folder / "key.pem").write_text(key)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(folder / "cert.pem", folder / "key.pem")
    upstream_host, upstream_port = upstream.removeprefix("http://").split(":")

    async def pipe(reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def forward(client_reader, client_writer):
        upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, int(upstream_port))
        await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda _loop, _context: None)
    server = loop.run_until_complete(asyncio.start_server(forward, "127.0.0.1", 0, ssl=context))
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield f"https://localhost:{server.sockets[0].getsockname()[1]}", folder / "cert.pem"
    finally:
        loop.call_soon_threadsafe(server.close)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


@contextmanager
def minio_over_tls(folder: Path) -> Iterator[tuple[str, Path]]:
    """MinIO behind ``tls_forwarder``: the forwarder's URL and the certificate's path.

    MinIO is started as the cloud end-to-end suite starts it, and the calling test is skipped when Docker cannot give
    one.
    """
    import pytest

    from test_utils.s3 import fixtures as s3

    available = s3.is_docker_available() and (
        s3.is_container_running(s3.MINIO_CONTAINER_NAME) or s3.start_minio_container()
    )
    if not (available and s3.wait_for_minio(max_retries=10)):
        pytest.skip("MinIO (Docker) is not available")
    with tls_forwarder(s3.MINIO_ENDPOINT_URL, folder) as endpoint:
        yield endpoint
