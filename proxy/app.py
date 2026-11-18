"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Every download holds a one-shot :class:`Lease` on its generation. The ASGI
  response releases the lease in a ``finally`` even when the client
  disconnects before the body iterator starts (whose generator-level
  ``finally`` would otherwise never run), so a torn-down request never pins a
  cached package or its files.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches. Reaps compare on-disk file identity
  (generation metadata + inode) before unlinking, so a late release of an old
  generation can never remove a newer generation's file at the same path.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, StreamingResponse

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


def _file_id(path: Path) -> tuple[int, int] | None:
    """``(device, inode)`` identity of *path*, or ``None`` if it is absent."""
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


def _unlink_if_id(path: Path, identity: tuple[int, int] | None) -> None:
    """Unlink *path* only while it still holds *identity*.

    A purged generation must never delete a newer generation's file that was
    published at the same path: if the path now points at a different inode
    (or identity was never recorded), leave it alone.
    """
    if identity is None:
        return
    if _file_id(path) != identity:
        return
    _unlink_quiet(path)


class Lease:
    """A single reader's claim on one :class:`Entry` generation.

    Release is one-shot and lives behind the ASGI response's ``finally``, so
    disconnects and cancellations decrement exactly once: a request that is
    torn down before the body generator starts (and whose generator-level
    cleanup therefore never runs) still releases its own lease, and repeated
    cancels cannot touch another reader's lease.
    """

    def __init__(self, cache: "Cache", entry: "Entry") -> None:
        self._cache = cache
        self.entry = entry
        self.released = False

    async def release(self) -> None:
        if self.released:
            return
        self.released = True
        await self._cache._release_lease(self)


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \-> failed             (origin error; part file removed)
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None
        # On-disk identity for the fixed-name published files; reaps unlink
        # them only while the path still points at this generation's inode.
        # Part files need no guard: their name embeds the generation UUID.
        self.final_id: tuple[int, int] | None = None
        self.meta_id: tuple[int, int] | None = None

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
        }


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        e.final_id = _file_id(final)
        e.meta_id = _file_id(e.meta_path)
        self.entries[key] = e
        return e

    async def attach(self, key: str) -> Lease:
        """Return a lease on the current generation, starting a fill if needed."""
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                e.fill_task = asyncio.create_task(self._fill(e))
            e.readers += 1
            return Lease(self, e)

    async def _release_lease(self, lease: Lease) -> None:
        """Return one reader; reap the generation's files when it was the last."""
        e = lease.entry
        async with self.lock:
            if e.readers <= 0:  # defensive: leases are one-shot, this must not happen
                return
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader."""
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk.
            existed = False
            final = CACHE_DIR / key
            for p in (final, CACHE_DIR / f"{key}.meta"):
                if p.exists():
                    _unlink_quiet(p)
                    existed = True
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    _unlink_quiet(p)
                    existed = True
            except OSError:
                pass
            return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once no readers remain and the fill is over.

        Unlinks go through identity checks: an old generation must never
        remove a newer generation's file that now occupies the same path.
        """
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.part_path)  # generation-unique name; never keep half
        elif e.purged:  # complete or closed
            _unlink_quiet(e.part_path)
            _unlink_if_id(e.final_path, e.final_id)
            _unlink_if_id(e.meta_path, e.meta_id)

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
            async with self.lock:
                e.size = written if e.size == -1 else e.size
                e.meta["size"] = written
                if e.purged:
                    # Purged mid-fill: keep the part file for current readers
                    # only; never register this generation.
                    e.state = "closed"
                else:
                    os.replace(e.part_path, e.final_path)  # atomic publish
                    e.final_id = _file_id(e.final_path)
                    try:
                        e.meta_path.write_text(json.dumps(e.meta))
                    except OSError:
                        pass
                    e.meta_id = _file_id(e.meta_path)
                    e.state = "complete"
                async with e.cond:
                    e.cond.notify_all()
                self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry):
        try:
            return open(e.part_path, "rb")
        except FileNotFoundError:
            return open(e.final_path, "rb")

    async def stream(self, lease: Lease):
        """Yield the package from byte 0, tailing the fill file if needed."""
        e = lease.entry
        offset = 0
        f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                await lease.release()


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills and orphan meta, keep complete packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.0.0", lifespan=lifespan)


class ReleasingStreamingResponse(StreamingResponse):
    """Streaming response that always releases the reader's lease.

    Starlette/uvicorn (ASGI 2.3) may cancel the response task before the body
    iterator is ever entered when the client disconnects early; a never-started
    async generator's ``finally`` does not run, which used to leak the reader.
    Releasing at the ASGI boundary instead makes cleanup independent of whether
    streaming began.
    """

    def __init__(self, lease: Lease, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._lease = lease

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._lease.release()


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    lease = await cache.attach(key)
    entry = lease.entry
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        await lease.release()  # no response object exists yet to do it for us
        raise

    if entry.state == "failed":
        await lease.release()
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(f"origin fetch failed: {entry.error}", status_code=status)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return ReleasingStreamingResponse(
        lease,
        cache.stream(lease),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}
