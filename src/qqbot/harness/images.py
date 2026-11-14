"""Bounded image loading outside Pi; temporary QQ URLs never enter its transcript."""

import base64
import http.client
import socket
import threading
import time
from urllib.parse import urljoin, urlsplit

from qqbot.harness.public_web import PinnedHTTPS, public_addresses

MAX_IMAGES = 4
MAX_IMAGE_BYTES = 8 * 1024 * 1024


def fetch_public_bytes(url, max_bytes, *, headers_for_url=None):
    if not 0 < max_bytes <= 20 * 1024 * 1024:
        raise ValueError("invalid attachment byte limit")
    deadline = time.monotonic() + 15
    for _ in range(4):
        parsed = urlsplit(url)
        # QQ sometimes emits HTTP links for QQ CDNs that also serve HTTPS.
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
        addresses = public_addresses(parsed.hostname, 443, timeout=deadline - time.monotonic())
        connection = PinnedHTTPS(parsed.hostname, 443, addresses, deadline=deadline)
        response, timer = None, None
        try:
            connection.timeout = connection.remaining()
            headers = {"Accept-Encoding": "identity", "User-Agent": "ElephantBot/0.1"}
            if headers_for_url is not None:
                headers.update(headers_for_url(url))
            connection.request("GET", (parsed.path or "/") + ("?" + parsed.query if parsed.query else ""),
                               headers=headers)
            sock = connection.sock
            def interrupt(sock=sock):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            timer = threading.Timer(connection.remaining(), interrupt)
            timer.daemon = True
            timer.start()
            sock.settimeout(connection.remaining())
            response = connection.getresponse()
            connection.remaining()
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
                sock.settimeout(connection.remaining())
                chunk = response.read1(min(65536, max_bytes + 1 - len(content)))
                if not chunk:
                    break
                content.extend(chunk)
                if len(content) > max_bytes:
                    raise ValueError("image exceeds size limit")
            connection.remaining()
            return bytes(content)
        except (OSError, http.client.HTTPException):
            connection.remaining()
            raise
        finally:
            if timer is not None:
                timer.cancel()
            if response is not None:
                response.close()
            connection.close()
    raise ValueError("too many image redirects")


def fetch_image(url, max_bytes):
    data = fetch_public_bytes(url, max_bytes)
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        mime = "image/png"
    elif data.startswith(b"\xff\xd8\xff"):
        mime = "image/jpeg"
    elif data[:6] in {b"GIF87a", b"GIF89a"}:
        mime = "image/gif"
    elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        mime = "image/webp"
    else:
        raise ValueError("unsupported image format (PNG, JPEG, GIF or WebP required)")
    return "data:" + mime + ";base64," + base64.b64encode(data).decode("ascii")
