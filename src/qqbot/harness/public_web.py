"""Trusted bounded public HTTPS fetch and JSON parsing."""

import hashlib
import http.client
import ipaddress
import json
import socket
import ssl
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from urllib.parse import urljoin, urlsplit

from fastapi import HTTPException

MAX_PAGE = 1024 * 1024


def public_addresses(host, port, *, timeout=None):
    if timeout is None:
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    else:
        if timeout <= 0:
            raise TimeoutError("public DNS deadline exceeded")
        # A resolver child can be killed when libc getaddrinfo exceeds the fetch budget.
        script = ("import json,socket,sys; print(json.dumps(socket.getaddrinfo("
                  "sys.argv[1], int(sys.argv[2]), type=socket.SOCK_STREAM)))")
        try:
            result = subprocess.run([sys.executable, "-I", "-c", script, host, str(port)],
                                    capture_output=True, text=True, timeout=timeout, check=False)
        except subprocess.TimeoutExpired:
            raise TimeoutError("public DNS deadline exceeded") from None
        if result.returncode:
            raise OSError("public DNS lookup failed")
        addresses = [(family, kind, protocol, canon, tuple(address))
                     for family, kind, protocol, canon, address in json.loads(result.stdout)]
    if not addresses:
        raise ValueError('public DNS response is empty')
    for item in addresses:
        address = ipaddress.ip_address(item[4][0])
        if not address.is_global or address.is_multicast or address.is_reserved:
            raise ValueError('only public unicast addresses are allowed')
    return addresses


class PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self, host, port, addresses, *, deadline=None):
        super().__init__(host, port, timeout=10, context=ssl.create_default_context())
        self.addresses = addresses
        self.deadline = deadline if deadline is not None else time.monotonic() + 15

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("public fetch deadline exceeded")
        return remaining

    def connect(self):
        last_error = None
        for family, kind, protocol, _, address in self.addresses[:4]:
            connection = socket.socket(family, kind, protocol)
            try:
                connection.settimeout(min(self.timeout, self.remaining()))
                connection.connect(address)
                connection.settimeout(self.remaining())
                self.sock = self._context.wrap_socket(connection, server_hostname=self.host)
                return
            except OSError as error:
                connection.close()
                last_error = error
        raise last_error or OSError("no public address")


def fetch_public(url, *, authorize_url=None):
    deadline = time.monotonic() + 15
    for _ in range(4):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("only public HTTPS URLs without credentials are allowed")
        if parsed.port not in {None, 443}:
            raise ValueError("only HTTPS port 443 is allowed")
        if authorize_url:
            authorize_url(url)
        addresses = public_addresses(parsed.hostname, 443, timeout=deadline - time.monotonic())
        connection = PinnedHTTPS(parsed.hostname, 443, addresses, deadline=deadline)
        response, timer = None, None
        try:
            connection.timeout = connection.remaining()
            connection.request(
                "GET",
                (parsed.path or "/") + ("?" + parsed.query if parsed.query else ""),
                headers={"User-Agent": "ElephantBot/0.1", "Accept-Encoding": "identity"},
            )
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
            content = bytearray()
            while len(content) <= MAX_PAGE:
                sock.settimeout(connection.remaining())
                chunk = response.read1(min(65536, MAX_PAGE + 1 - len(content)))
                if not chunk:
                    break
                content.extend(chunk)
            if len(content) > MAX_PAGE:
                raise ValueError("public source exceeds size limit")
            connection.remaining()
            text = content.decode("utf-8", errors="replace")
            return {
                "url": url,
                "retrieved_at": datetime.now(UTC).isoformat(),
                "sha256": hashlib.sha256(content).hexdigest(),
                "text": text,
            }
        except (OSError, http.client.HTTPException):
            connection.remaining()
            raise
        finally:
            if timer is not None:
                timer.cancel()
            if response is not None:
                response.close()
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
