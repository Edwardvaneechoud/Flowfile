"""In-app client for the community node registry.

Reads ``index.json`` / ``popularity.json`` and downloads sha256-pinned artifacts.
Two modes, selected by ``settings.get_community_index_url()``:

- **Fixture mode** — the configured location is a local filesystem path (no
  ``http(s)`` scheme). Index, popularity and artifacts are read from disk
  relative to the index file's parent. This is what makes the MVP browse/install
  loop testable with no GitHub repo.
- **Remote mode** — an ``https://`` URL. Index/popularity are fetched with ETag
  revalidation + a disk cache under ``user_defined_nodes/community_cache/``;
  artifact URLs are composed from ``registry.commit`` (primary CDN, then
  fallback). All downloads are https-only, no cross-host redirects, and every
  artifact is sha256-verified before its bytes are returned.
"""

import hashlib
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from flowfile_core.configs import logger
from flowfile_core.configs.settings import (
    get_community_artifact_base,
    get_community_artifact_fallback,
    get_community_cache_ttl,
    get_community_index_url,
    get_community_popularity_url,
)
from flowfile_core.flowfile.community_nodes.models import (
    CommunityIndex,
    CommunityIndexEntry,
    PinnedFile,
    PopularityData,
)
from shared import storage

_ARTIFACT_HARD_CAP = 8 * 1024 * 1024
_ARTIFACT_SIZE_SLACK = 4096
_INDEX_CAP = 4 * 1024 * 1024
_HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)


class CommunityClientError(Exception):
    """Base class for community client failures."""


class CommunityUnavailableError(CommunityClientError):
    """The registry could not be reached and no usable cache exists (route → 503)."""


class PinMismatchError(CommunityClientError):
    """A downloaded artifact's sha256 did not match the pinned hash (route → 502)."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_fixture(url: str) -> bool:
    return not (url.startswith("https://") or url.startswith("http://"))


def _servable_media(entry: CommunityIndexEntry) -> dict[str, PinnedFile]:
    """Basename → PinnedFile for the icon + screenshots only (README is served as text elsewhere)."""
    files: dict[str, PinnedFile] = {}
    if entry.artifacts.icon is not None:
        files[Path(entry.artifacts.icon.path).name] = entry.artifacts.icon
    for shot in entry.artifacts.screenshots:
        files[Path(shot.path).name] = shot
    return files


class CommunityClient:
    def __init__(self):
        self._lock = threading.Lock()
        self._http_client: httpx.Client | None = None
        self._mem_index: CommunityIndex | None = None
        self._mem_url: str = ""
        self._mem_at: float = 0.0
        self._mem_fetched_at: str = ""
        # Popularity is memory-cached (None included) so a browse never re-fetches
        # within the TTL and a dead endpoint is not hammered per request.
        self._mem_popularity: PopularityData | None = None
        self._mem_popularity_url: str = ""
        self._mem_popularity_at: float = 0.0

    def _http(self) -> httpx.Client:
        if self._http_client is None:
            self._http_client = httpx.Client(timeout=_HTTP_TIMEOUT, follow_redirects=False)
        return self._http_client

    # ---- cache paths ----------------------------------------------------

    def _cache_dir(self) -> Path:
        return storage.user_defined_nodes_directory / "community_cache"

    def _index_cache_file(self) -> Path:
        return self._cache_dir() / "index.json"

    def _etag_cache_file(self) -> Path:
        return self._cache_dir() / "index.etag"

    def _popularity_cache_file(self) -> Path:
        return self._cache_dir() / "popularity.json"

    def _media_file(self, node_id: str, file_name: str) -> Path:
        return self._cache_dir() / "media" / node_id / file_name

    # ---- index ----------------------------------------------------------

    def get_index(self, refresh: bool = False) -> tuple[CommunityIndex, dict]:
        url = get_community_index_url()
        if _is_fixture(url):
            return self._fixture_index(url)
        with self._lock:
            return self._remote_index(url, refresh)

    def _fixture_index(self, url: str) -> tuple[CommunityIndex, dict]:
        path = Path(url)
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as e:
            raise CommunityUnavailableError(f"Fixture index not found: {path}") from e
        try:
            index = CommunityIndex.model_validate_json(raw)
        except ValueError as e:
            raise CommunityUnavailableError(f"Fixture index is not valid: {e}") from e
        return index, {"fetched_at": _now_iso(), "source": "fixture", "stale": False}

    def _remote_index(self, url: str, refresh: bool) -> tuple[CommunityIndex, dict]:
        if not url.startswith("https://"):
            raise CommunityUnavailableError("Community registry URL must be https://")

        now = time.monotonic()
        ttl = get_community_cache_ttl()
        if not refresh and self._mem_index is not None and self._mem_url == url and (now - self._mem_at) < ttl:
            return self._mem_index, {"fetched_at": self._mem_fetched_at, "source": "remote", "stale": False}

        etag = self._read_cached_etag()
        headers = {"If-None-Match": etag} if etag else {}
        try:
            resp = self._http().get(url, headers=headers)
        except httpx.HTTPError as e:
            logger.warning("Community index fetch failed (%s); trying cache", e)
            return self._index_from_cache_or_raise()

        if resp.status_code == 304:
            cached = self._read_cached_index()
            if cached is not None:
                self._store_mem(url, cached)
                return cached, {"fetched_at": self._mem_fetched_at, "source": "remote", "stale": False}
            try:
                resp = self._http().get(url)
            except httpx.HTTPError as e:
                logger.warning("Community index refetch failed (%s); trying cache", e)
                return self._index_from_cache_or_raise()

        if resp.status_code >= 400:
            logger.warning("Community index returned HTTP %s; trying cache", resp.status_code)
            return self._index_from_cache_or_raise()

        data = resp.content
        if len(data) > _INDEX_CAP:
            logger.warning("Community index exceeds size cap; trying cache")
            return self._index_from_cache_or_raise()
        try:
            index = CommunityIndex.model_validate_json(data)
        except ValueError as e:
            logger.warning("Community index is not valid (%s); trying cache", e)
            return self._index_from_cache_or_raise()

        self._write_cached_index(data, resp.headers.get("etag", ""))
        self._store_mem(url, index)
        self._prune_media_cache(index)
        return index, {"fetched_at": self._mem_fetched_at, "source": "remote", "stale": False}

    def _prune_media_cache(self, index: CommunityIndex) -> None:
        """Drop cached media for nodes no longer in the index so the cache cannot grow unbounded."""
        media_root = self._cache_dir() / "media"
        if not media_root.is_dir():
            return
        keep = {entry.id for entry in index.nodes}
        for child in media_root.iterdir():
            if child.is_dir() and child.name not in keep:
                shutil.rmtree(child, ignore_errors=True)

    def _store_mem(self, url: str, index: CommunityIndex) -> None:
        self._mem_index = index
        self._mem_url = url
        self._mem_at = time.monotonic()
        self._mem_fetched_at = _now_iso()

    def _index_from_cache_or_raise(self) -> tuple[CommunityIndex, dict]:
        cached = self._read_cached_index()
        if cached is None:
            raise CommunityUnavailableError("Community registry is unavailable and no cached copy exists")
        try:
            mtime = datetime.fromtimestamp(self._index_cache_file().stat().st_mtime, timezone.utc).isoformat()
        except OSError:
            mtime = _now_iso()
        return cached, {"fetched_at": mtime, "source": "cache", "stale": True}

    def _read_cached_etag(self) -> str:
        try:
            return self._etag_cache_file().read_text(encoding="utf-8").strip()
        except OSError:
            return ""

    def _read_cached_index(self) -> CommunityIndex | None:
        try:
            raw = self._index_cache_file().read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            return CommunityIndex.model_validate_json(raw)
        except ValueError:
            return None

    def _write_cached_index(self, data: bytes, etag: str) -> None:
        cache_dir = self._cache_dir()
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
            self._index_cache_file().write_bytes(data)
            self._etag_cache_file().write_text(etag, encoding="utf-8")
        except OSError as e:
            logger.warning("Could not write community index cache: %s", e)

    # ---- popularity -----------------------------------------------------

    def get_popularity(self, refresh: bool = False) -> PopularityData | None:
        """Absent or failed popularity never raises — the UI simply hides ratings."""
        url = get_community_popularity_url()
        if _is_fixture(url):
            return self._read_popularity_file(Path(url))
        if not url.startswith("https://"):
            return None
        cached_url_matches = self._mem_popularity_url == url
        within_ttl = (time.monotonic() - self._mem_popularity_at) < get_community_cache_ttl()
        if not refresh and cached_url_matches and within_ttl:
            return self._mem_popularity
        popularity = self._fetch_popularity(url)
        self._mem_popularity = popularity
        self._mem_popularity_url = url
        self._mem_popularity_at = time.monotonic()
        return popularity

    def _fetch_popularity(self, url: str) -> PopularityData | None:
        try:
            resp = self._http().get(url)
        except httpx.HTTPError:
            return self._read_popularity_file(self._popularity_cache_file())
        if resp.status_code >= 400:
            return self._read_popularity_file(self._popularity_cache_file())
        data = resp.content
        if len(data) > _INDEX_CAP:
            return None
        try:
            popularity = PopularityData.model_validate_json(data)
        except ValueError:
            return self._read_popularity_file(self._popularity_cache_file())
        try:
            self._cache_dir().mkdir(parents=True, exist_ok=True)
            self._popularity_cache_file().write_bytes(data)
        except OSError:
            pass
        return popularity

    @staticmethod
    def _read_popularity_file(path: Path) -> PopularityData | None:
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return None
        try:
            return PopularityData.model_validate_json(raw)
        except ValueError:
            return None

    # ---- artifacts ------------------------------------------------------

    def download_pinned(self, pinned: PinnedFile, commit: str) -> bytes:
        """Fetch an artifact and verify its sha256 before returning its bytes."""
        if pinned.size > _ARTIFACT_HARD_CAP:
            raise PinMismatchError(f"Pinned artifact {pinned.path!r} declares an oversized {pinned.size}-byte payload")
        cap = min(pinned.size + _ARTIFACT_SIZE_SLACK, _ARTIFACT_HARD_CAP)
        data = self._fetch_artifact_bytes(pinned.path, commit, cap)
        digest = hashlib.sha256(data).hexdigest()
        if digest != pinned.sha256:
            raise PinMismatchError(f"sha256 mismatch for {pinned.path!r}: expected {pinned.sha256}, got {digest}")
        return data

    def _fetch_artifact_bytes(self, path: str, commit: str, cap: int) -> bytes:
        if Path(path).is_absolute() or ".." in Path(path).parts:
            raise CommunityUnavailableError(f"Illegal artifact path: {path!r}")

        index_url = get_community_index_url()
        if _is_fixture(index_url):
            fixture_root = Path(index_url).parent.resolve()
            file_path = (fixture_root / path).resolve()
            # An index file is untrusted input: never read outside its own tree.
            if not file_path.is_relative_to(fixture_root):
                raise CommunityUnavailableError(f"Illegal artifact path: {path!r}")
            try:
                data = file_path.read_bytes()
            except OSError as e:
                raise CommunityUnavailableError(f"Fixture artifact not found: {file_path}") from e
            if len(data) > cap:
                raise PinMismatchError(f"Artifact {path!r} exceeds its declared size")
            return data

        templates = [get_community_artifact_base(), get_community_artifact_fallback()]
        last_error: Exception | None = None
        for template in templates:
            url = template.format(commit=commit, path=path)
            if not url.startswith("https://"):
                continue
            try:
                return self._http_stream_capped(url, cap)
            except (httpx.HTTPError, CommunityUnavailableError) as e:
                last_error = e
                continue
        raise CommunityUnavailableError(f"Could not download artifact {path!r}: {last_error}")

    def _http_stream_capped(self, url: str, cap: int) -> bytes:
        with self._http().stream("GET", url) as resp:
            if resp.status_code >= 400:
                raise CommunityUnavailableError(f"HTTP {resp.status_code} for {url}")
            chunks: list[bytes] = []
            total = 0
            for chunk in resp.iter_bytes():
                total += len(chunk)
                if total > cap:
                    raise PinMismatchError(f"Artifact at {url} exceeds its declared size cap")
                chunks.append(chunk)
            return b"".join(chunks)

    def get_media(self, node_id: str, file_name: str, index: CommunityIndex) -> Path:
        """Return a local, sha256-verified path to a servable media file (icon/screenshot)."""
        entry = index.entry(node_id)
        if entry is None:
            raise CommunityUnavailableError(f"Unknown community node: {node_id}")
        servable = _servable_media(entry)
        pinned = servable.get(file_name)
        if pinned is None:
            raise CommunityUnavailableError(f"{file_name!r} is not a servable media file for {node_id}")

        cached = self._media_file(node_id, file_name)
        if cached.exists():
            try:
                if hashlib.sha256(cached.read_bytes()).hexdigest() == pinned.sha256:
                    return cached
            except OSError:
                pass

        data = self.download_pinned(pinned, index.registry.commit)
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(data)
        return cached


_client_instance: CommunityClient | None = None
_client_lock = threading.Lock()


def get_community_client() -> CommunityClient:
    global _client_instance
    with _client_lock:
        if _client_instance is None:
            _client_instance = CommunityClient()
        return _client_instance
