"""Official DeepSeek tools, using the configured model's dedicated credential."""
import hashlib
import json
from urllib.parse import urlsplit

import httpx


class DeepSeekError(ValueError):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class DeepSeekAPI:
    base_url = "https://api.deepseek.com"

    def __init__(self, key, *, transport=None):
        self.key, self.transport = key, transport
        self.account = hashlib.scrypt(key.encode(), salt=b"ElephantBot DeepSeek cache",
            n=16384, r=8, p=1, dklen=32).hex()

    @classmethod
    def from_model(cls, endpoint, key, *, transport=None):
        try:
            parsed = urlsplit(endpoint or "")
            official = (parsed.scheme == "https" and parsed.hostname == "api.deepseek.com"
                and parsed.port in {None, 443} and not parsed.username and not parsed.password)
        except ValueError:
            return None
        if official and key:
            return cls(key, transport=transport)
        return None

    async def _request(self, method, path, *, timeout=25, **kwargs):
        try:
            async with (
                httpx.AsyncClient(transport=self.transport, trust_env=False,
                    follow_redirects=False, timeout=httpx.Timeout(timeout, connect=10)) as client,
                client.stream(method, self.base_url + path,
                    headers={"Authorization": "Bearer " + self.key}, **kwargs) as response,
            ):
                if response.status_code != 200:
                    raise DeepSeekError(f"DeepSeek tool returned HTTP {response.status_code}", response.status_code)
                content = bytearray()
                async for chunk in response.aiter_bytes():
                    content.extend(chunk)
                    if len(content) > 2 * 1024 * 1024:
                        raise DeepSeekError("DeepSeek tool response exceeds 2 MiB")
        except httpx.HTTPError:
            raise DeepSeekError("DeepSeek tool connection failed or timed out") from None
        try:
            result = json.loads(content)
        except (ValueError, UnicodeDecodeError):
            raise DeepSeekError("DeepSeek tool returned invalid JSON") from None
        if not isinstance(result, dict) or result.get("error"):
            raise DeepSeekError("DeepSeek tool returned an invalid result")
        return result

    async def search(self, query):
        return await self._request("POST", "/v1/tools/search", json={"query": query, "need_content": True})

    async def fetch(self, url):
        return await self._request("POST", "/v1/tools/fetch", json={"url": url})

    async def transcribe(self, data, filename):
        result = await self._request("POST", "/v1/tools/asr", timeout=150,
            files={"file": (filename, data, "application/octet-stream")})
        if (not isinstance(result.get("text"), str) or len(result["text"]) > 10000
                or type(result.get("duration_ms")) is not int or not 0 <= result["duration_ms"] <= 120000):
            raise DeepSeekError("DeepSeek returned an invalid audio transcript")
        return {name: result[name] for name in ("text", "duration_ms", "request_id") if name in result}

    async def ocr(self, data, filename):
        result = await self._request("POST", "/v1/tools/ocr", timeout=220,
            data={"purpose": "text_recognition"}, files={"file": (filename, data)})
        pages = result.get("pages")
        count = result.get("page_count")
        if (result.get("purpose") != "text_recognition" or type(count) is not int
                or not 1 <= count <= 1000 or not isinstance(pages, list) or len(pages) != count):
            raise DeepSeekError("DeepSeek returned an invalid OCR result")
        for index, page in enumerate(pages):
            if (not isinstance(page, dict) or type(page.get("index")) is not int or page["index"] != index
                    or not isinstance(page.get("text"), str) or page.get("finish_reason") not in ("stop", "length")):
                raise DeepSeekError("DeepSeek returned an invalid OCR page")
        return {"purpose": result["purpose"], "page_count": count,
                "pages": [{name: page[name] for name in ("index", "text", "finish_reason")} for page in pages]}

    async def upload(self, data, filename, purpose):
        return await self._request("POST", "/files", timeout=260,
            data={"purpose": purpose, "expires_after[anchor]": "created_at",
                  "expires_after[seconds]": "2592000"},
            files={"file": (filename, data)})

    async def files(self, purpose, after=None):
        params = {"purpose": purpose, "limit": 1000, "order": "desc"}
        if after:
            params["after"] = after
        return await self._request("GET", "/files", params=params)
