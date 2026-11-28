import base64
import hashlib
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs

import httpx
import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from shared.sharepoint import acquire_token


def test_certificate_token_exchange_uses_signed_client_assertion():
    tenant = "11111111-1111-1111-1111-111111111111"
    client_id = "22222222-2222-2222-2222-222222222222"
    endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "SharePoint test")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    pem = certificate.public_bytes(serialization.Encoding.PEM) + key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )

    def exchange(request):
        assert str(request.url) == endpoint
        fields = parse_qs(request.content.decode())
        assert fields["grant_type"] == ["client_credentials"]
        assert "client_secret" not in fields
        assertion = fields["client_assertion"][0]
        thumbprint = hashlib.sha256(certificate.public_bytes(serialization.Encoding.DER)).digest()
        assert jwt.get_unverified_header(assertion)["x5t#S256"] == base64.urlsafe_b64encode(thumbprint).decode().rstrip(
            "="
        )
        claims = jwt.decode(assertion, key.public_key(), algorithms=["PS256"], audience=endpoint, issuer=client_id)
        assert claims["sub"] == client_id
        assert 0 < claims["exp"] - claims["iat"] <= 300
        return httpx.Response(200, json={"access_token": "test-token"})

    with httpx.Client(transport=httpx.MockTransport(exchange)) as client:
        assert acquire_token(client, tenant, client_id, pem.decode(), auth_method="certificate") == "test-token"
