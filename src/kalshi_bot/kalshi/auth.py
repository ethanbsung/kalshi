from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key


class KalshiSigner:
    def __init__(self, api_key_id: str, private_key_path: Path) -> None:
        if not api_key_id:
            raise ValueError("api_key_id is required")
        self._api_key_id = api_key_id
        self._private_key_path = private_key_path
        self._private_key = None

    def _load_private_key(self):
        if self._private_key is None:
            data = self._private_key_path.read_bytes()
            self._private_key = load_pem_private_key(data, password=None)
        return self._private_key

    def build_headers(
        self, method: str, path: str, timestamp_ms: int | None = None
    ) -> tuple[dict[str, str], str, str]:
        timestamp_ms = timestamp_ms or int(time.time() * 1000)
        timestamp = str(timestamp_ms)
        path_no_query = path.split("?")[0]
        message = f"{timestamp}{method}{path_no_query}".encode("utf-8")
        signature = self._load_private_key().sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        signature_b64 = base64.b64encode(signature).decode("ascii")
        headers = {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature_b64,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }
        return headers, timestamp, signature_b64
