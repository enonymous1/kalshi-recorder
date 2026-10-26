"""Kalshi API authentication: request signing and WS headers.

Private endpoints and WS handshake require Ed25519 (default) or RSA-PSS
signatures. REST market data is public (no auth needed).
"""

from __future__ import annotations

import base64
import re
import time

from core.logging import get_logger

LOG = get_logger(__name__)


def load_private_key(data: bytes):
    """Parse a Kalshi API private key from dashboard-downloaded material.

    Accepts:
    - PEM-armored key (any file extension; leading UTF-8 BOM tolerated),
    - a PEM block embedded in surrounding text (some dashboards bundle the
      key ID and key in one file),
    - a raw 32-byte Ed25519 seed as base64 or hex (no PEM armor).
    Returns a private-key object suitable for :func:`sign_request`.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    blob = data.lstrip(b"\xef\xbb\xbf").strip()
    if b"-----BEGIN" not in blob:
        # Maybe a PEM block buried in surrounding text (key ID, labels, ...).
        m = re.search(
            br"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
            blob, re.DOTALL,
        )
        if m:
            blob = m.group(0)
    if b"-----BEGIN" in blob:
        return serialization.load_pem_private_key(blob, password=None)
    # Raw key material: base64 or hex -> 32-byte Ed25519 seed. (Note: a hex
    # string is also valid base64, so each decoding is length-checked.)
    compact = re.sub(rb"\s+", b"", blob)
    raw = None
    for decode in (lambda b: base64.b64decode(b, validate=True),
                   lambda b: bytes.fromhex(b.decode("ascii"))):
        try:
            cand = decode(compact)
        except Exception:
            continue
        if len(cand) == 32:
            raw = cand
            break
    if raw is None:
        raise ValueError(
            "unrecognized private key format: expected a PEM block or a 32-byte "
            "Ed25519 seed (base64/hex)")
    return ed25519.Ed25519PrivateKey.from_private_bytes(raw)


def sign_request(key_id: str, private_key_pem: bytes, method: str, path: str) -> tuple[str, str, str]:
    """Build Kalshi auth headers (Ed25519 or RSA-PSS).

    Kalshi issues Ed25519 keys by default (RSA 2048-bit optional). The key type
    is determined from the parsed key, per Kalshi's docs: Ed25519 signs the
    pre-sign text directly (RFC 8032); RSA signs with RSA-PSS/SHA-256
    (MGF1-SHA256, salt length = digest length). Signs
    ``{timestamp_ms}{METHOD}{path}`` (path WITHOUT query string).
    Returns (key_id, timestamp_ms, base64_signature).
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

    key = load_private_key(private_key_pem)
    ts_ms = str(int(time.time() * 1000))
    message = f"{ts_ms}{method.upper()}{path}".encode()
    if isinstance(key, ed25519.Ed25519PrivateKey):
        sig = key.sign(message)
    elif isinstance(key, rsa.RSAPrivateKey):
        sig = key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                                            salt_length=padding.PSS.DIGEST_LENGTH),
                       hashes.SHA256())
    else:
        raise TypeError(f"unsupported Kalshi API key type: {type(key).__name__}")
    return key_id, ts_ms, base64.b64encode(sig).decode()


_WS_HEADERS_KWARG: str | None = None


def ws_headers_kwarg() -> str:
    """Header kwarg for websockets.connect(), version-agnostic.

    websockets>=14 renamed connect()'s ``extra_headers`` to
    ``additional_headers``; older versions only accept ``extra_headers``.
    Inspects the installed library instead of pinning a version.
    """
    global _WS_HEADERS_KWARG
    if _WS_HEADERS_KWARG is None:
        import inspect

        import websockets

        try:
            params = inspect.signature(websockets.connect).parameters
        except (TypeError, ValueError):
            params = {}
        _WS_HEADERS_KWARG = ("additional_headers" if "additional_headers" in params
                             else "extra_headers")
    return _WS_HEADERS_KWARG
