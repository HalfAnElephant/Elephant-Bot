"""Bounded image loading outside Pi; temporary QQ URLs never enter its transcript."""

import base64
import time
from urllib.parse import urljoin, urlsplit

from qqbot.harness.research import PinnedHTTPS, public_addresses

MAX_IMAGES = 4
MAX_IMAGE_BYTES = 8 * 1024 * 1024


def fetch_image(url, max_bytes):
    deadline = time.monotonic() + 15
    for _ in range(4):
        parsed = urlsplit(url)
        # NapCat sometimes emits HTTP links for QQ CDNs that also serve HTTPS.
        if (parsed.scheme == "http" and parsed.hostname and not parsed.username and not parsed.password
                and any(
            parsed.hostname == suffix or parsed.hostname.endswith("." + suffix)
            for suffix in ("qpic.cn", "qq.com", "qq.com.cn")
        ) and parsed.port in {None, 80}):
            url = "https://" + parsed.hostname + (parsed.path or "/") + ("?" + parsed.query if parsed.query else "")
            parsed = urlsplit(url)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                or parsed.port not in {None, 443}):
            raise ValueError("image requires public HTTPS")
        connection = PinnedHTTPS(parsed.hostname, 443, public_addresses(parsed.hostname, 443))
        connection.timeout = max(0.1, min(10, deadline - time.monotonic()))
        response = None
        try:
            connection.request("GET", (parsed.path or "/") + ("?" + parsed.query if parsed.query else ""),
                               headers={"Accept-Encoding": "identity", "User-Agent": "ElephantBot/0.1"})
            sock = connection.sock
            response = connection.getresponse()
            if response.status in {301, 302, 303, 307, 308}:
                location = response.getheader("Location")
                if not location:
                    raise ValueError("image redirect without location")
                url = urljoin(url, location)
                continue
            if response.status != 200 or response.getheader("Content-Encoding", "identity") != "identity":
                raise ValueError("image download failed")
            content = bytearray()
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("image download timed out")
                sock.settimeout(min(10, remaining))
                chunk = response.read1(min(65536, max_bytes + 1 - len(content)))
                if not chunk:
                    break
                content.extend(chunk)
                if len(content) > max_bytes:
                    raise ValueError("image exceeds size limit")
            data = bytes(content)
            if data.startswith(b"\x89PNG\r\n\x1a\n"):
                mime = "image/png"
            elif data.startswith(b"\xff\xd8\xff"):
                mime = "image/jpeg"
            elif data[:6] in {b"GIF87a", b"GIF89a"}:
                mime = "image/gif"
            elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
                mime = "image/webp"
            else:
                raise ValueError("unsupported image format")
            return "data:" + mime + ";base64," + base64.b64encode(data).decode("ascii")
        finally:
            if response is not None:
                response.close()
            connection.close()
    raise ValueError("too many image redirects")
