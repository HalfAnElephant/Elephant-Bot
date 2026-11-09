import math

import httpx


def cosine(a, b):
    if len(a) != len(b) or not a or not all(math.isfinite(v) for v in (*a, *b)):
        raise ValueError("incompatible vectors")
    denominator = math.sqrt(sum(v * v for v in a) * sum(v * v for v in b))
    return sum(x * y for x, y in zip(a, b)) / denominator if denominator else 0.0


class HTTPEmbedding:
    def __init__(self, endpoint, model, version, api_key=None, dimensions=None, client=None):
        self.endpoint, self.model, self.version = endpoint, model, version
        self.api_key, self.dimensions, self.client = api_key, dimensions, client

    async def embed(self, text):
        if not self.api_key:
            return None
        try:
            if self.client:
                response = await self.client.post(
                    self.endpoint,
                    json={"model": self.model, "input": text},
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    timeout=1.5,
                )
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self.endpoint,
                        json={"model": self.model, "input": text},
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        timeout=1.5,
                    )
            response.raise_for_status()
            vector = response.json()["data"][0]["embedding"]
            if (
                not isinstance(vector, list)
                or not vector
                or any(type(v) not in (float, int) or not math.isfinite(v) for v in vector)
                or self.dimensions is not None
                and len(vector) != self.dimensions
            ):
                raise ValueError("invalid embedding")
            return tuple(float(v) for v in vector)
        except (httpx.HTTPError, KeyError, ValueError, TypeError, IndexError):
            return None
