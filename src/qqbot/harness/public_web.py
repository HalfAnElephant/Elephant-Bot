"""Trusted bounded public HTTPS fetch and JSON parsing."""

import hashlib
import http.client
import ipaddress
import json
import socket
import ssl
from datetime import UTC, datetime
from urllib.parse import urljoin, urlsplit

from fastapi import HTTPException

MAX_PAGE = 1024 * 1024


def public_addresses(host, port):
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    if not addresses or len(addresses) > 16:
        raise ValueError('public DNS response is empty or exceeds address limit')
    for item in addresses:
        address = ipaddress.ip_address(item[4][0])
        if not address.is_global or address.is_multicast or address.is_reserved:
            raise ValueError('only public unicast addresses are allowed')
    return addresses


class PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self, host, port, addresses):
        super().__init__(host, port, timeout=10, context=ssl.create_default_context())
        self.addresses = addresses

    def connect(self):
        last_error = None
        for family, kind, protocol, _, address in self.addresses:
            connection = socket.socket(family, kind, protocol)
            connection.settimeout(self.timeout)
            try:
                connection.connect(address)
                self.sock = self._context.wrap_socket(connection, server_hostname=self.host)
                return
            except OSError as error:
                connection.close()
                last_error = error
        raise last_error or OSError("no public address")


def fetch_public(url):
    for _ in range(4):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("only public HTTPS URLs without credentials are allowed")
        if parsed.port not in {None, 443}:
            raise ValueError("only HTTPS port 443 is allowed")
        connection = PinnedHTTPS(parsed.hostname, 443, public_addresses(parsed.hostname, 443))
        try:
            connection.request(
                "GET",
                (parsed.path or "/") + ("?" + parsed.query if parsed.query else ""),
                headers={"User-Agent": "ElephantBot/0.1", "Accept-Encoding": "identity"},
            )
            response = connection.getresponse()
            if response.status in {301, 302, 303, 307, 308}:
                location = response.getheader("Location")
                if not location:
                    raise ValueError("redirect without location")
                url = urljoin(url, location)
                continue
            if response.status != 200:
                raise ValueError("public source returned HTTP " + str(response.status))
            media_type = response.getheader("Content-Type", "").split(";")[0].lower()
            if media_type not in {
                "text/plain",
                "text/html",
                "application/json",
                "text/markdown",
                "application/xml",
                "text/xml",
            }:
                raise ValueError("unsupported public source type")
            if response.getheader("Content-Encoding", "identity") != "identity":
                raise ValueError("compressed responses are disabled")
            content = response.read(MAX_PAGE + 1)
            if len(content) > MAX_PAGE:
                raise ValueError("public source exceeds size limit")
            text = content.decode("utf-8", errors="replace")
            return {
                "url": url,
                "retrieved_at": datetime.now(UTC).isoformat(),
                "sha256": hashlib.sha256(content).hexdigest(),
                "text": text,
            }
        finally:
            connection.close()
    raise ValueError("too many redirects")


async def bounded_json(request, limit):
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > limit:
            raise HTTPException(413, "request exceeded limit")
    try:
        result = json.loads(content)
        if not isinstance(result, dict):
            raise TypeError("object required")
        return result
    except (ValueError, TypeError) as error:
        raise HTTPException(422, "invalid JSON object") from error
