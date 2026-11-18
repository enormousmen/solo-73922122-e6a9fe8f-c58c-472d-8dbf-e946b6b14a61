"""Controllable local package origin used to demo/test the caching proxy.

Serves complete packages from PKG_DIR (a 32 MiB package is generated on
first boot). Control endpoints let you change content, add per-chunk delay,
cut the stream mid-body, and inspect how many times the proxy fetched.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

PKG_DIR = Path(os.environ.get("PKG_DIR", "./packages")).resolve()
DEFAULT_PACKAGES = {
    "pkg-32m.bin": 32 * 1024 * 1024,
    "pkg-1m.bin": 1 * 1024 * 1024,
}

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")

behavior = {
    "chunk_delay_ms": 0,
    "truncate_after_bytes": None,  # int | None
    "chunk_size": 256 * 1024,
}
hits = {"total": 0, "paths": {}}
digests: dict[str, str] = {}


class UpstreamAbort(Exception):
    """Raised mid-stream to simulate an upstream connection abort."""


def validate_key(raw: str) -> str:
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _regenerate(key: str, size: int) -> str:
    p = PKG_DIR / key
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    h = hashlib.sha256()
    with open(tmp, "wb") as f:
        left = size
        while left > 0:
            b = os.urandom(min(1024 * 1024, left))
            f.write(b)
            h.update(b)
            left -= len(b)
    os.replace(tmp, p)
    digests[key] = h.hexdigest()
    return digests[key]


def _hash_file(key: str) -> str:
    h = hashlib.sha256()
    with open(PKG_DIR / key, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    digests[key] = h.hexdigest()
    return digests[key]


@asynccontextmanager
async def lifespan(app: FastAPI):
    PKG_DIR.mkdir(parents=True, exist_ok=True)
    for name, size in DEFAULT_PACKAGES.items():
        if (PKG_DIR / name).is_file():
            await asyncio.to_thread(_hash_file, name)
        else:
            await asyncio.to_thread(_regenerate, name, size)
    yield


app = FastAPI(title="pkg-origin", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    # Count every fetch attempt against the origin, successful or not.
    hits["total"] += 1
    hits["paths"][key] = hits["paths"].get(key, 0) + 1

    p = PKG_DIR / key
    if not p.is_file():
        return PlainTextResponse("package not found", status_code=404)

    size = p.stat().st_size
    beh = dict(behavior)  # snapshot for this request

    async def body():
        sent = 0
        chunk_size = max(1, int(beh["chunk_size"]))
        delay = max(0, int(beh["chunk_delay_ms"])) / 1000.0
        trunc = beh["truncate_after_bytes"]
        with open(p, "rb") as f:
            while True:
                if delay:
                    await asyncio.sleep(delay)
                chunk = await asyncio.to_thread(f.read, chunk_size)
                if not chunk:
                    break
                if trunc is not None and sent + len(chunk) > trunc:
                    remain = max(0, int(trunc) - sent)
                    if remain:
                        sent += remain
                        yield chunk[:remain]
                    # Simulate upstream abort: die mid-body after a partial
                    # response so the client sees a truncated payload.
                    raise UpstreamAbort("simulated upstream abort")
                sent += len(chunk)
                yield chunk

    headers = {"Content-Length": str(size)}
    sha = digests.get(key)
    if sha:
        headers["X-Package-Sha256"] = sha
    return StreamingResponse(body(), media_type="application/octet-stream", headers=headers)


class Behavior(BaseModel):
    chunk_delay_ms: int = Field(0, ge=0, description="sleep per streamed chunk")
    truncate_after_bytes: int | None = Field(
        None, ge=0, description="abort the stream after this many bytes"
    )
    chunk_size: int = Field(256 * 1024, ge=1, description="origin stream chunk size")


@app.post("/control/behavior")
async def set_behavior(b: Behavior):
    """Set delay / truncation for subsequent package downloads."""
    behavior.update(b.model_dump())
    return behavior


@app.get("/control/behavior")
async def get_behavior():
    return behavior


@app.post("/control/content/{path:path}")
async def change_content(path: str, size: int | None = None):
    """Regenerate the package with fresh random content (new sha256)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    p = PKG_DIR / key
    if size is None:
        size = p.stat().st_size if p.is_file() else 32 * 1024 * 1024
    if size <= 0:
        return PlainTextResponse("size must be positive", status_code=400)
    sha = await asyncio.to_thread(_regenerate, key, size)
    return {"path": key, "size": size, "sha256": sha}


@app.get("/control/stats")
async def stats():
    """Origin fetch counters (what the proxy pulled) and current digests."""
    packages = {}
    for key, sha in digests.items():
        try:
            packages[key] = {"size": (PKG_DIR / key).stat().st_size, "sha256": sha}
        except OSError:
            packages[key] = {"size": None, "sha256": sha}
    return {
        "total_hits": hits["total"],
        "paths": dict(hits["paths"]),
        "packages": packages,
        "behavior": dict(behavior),
    }


@app.post("/control/reset")
async def reset():
    hits["total"] = 0
    hits["paths"] = {}
    return {"ok": True}
