from __future__ import annotations

import shutil
import zipfile
from pathlib import Path
from typing import Any

import requests

DEFAULT_MAX_BYTES = 200 * 1024 * 1024
DEFAULT_TIMEOUT = 60.0
ZIP_MAGIC = b"PK\x03\x04"


class DownloadError(Exception):
    pass


def ensure_zip(path: Path) -> None:
    """Fail loudly unless `path` is a real zip (Box often serves an HTML page instead)."""
    with path.open("rb") as f:
        head = f.read(512)
    if head[:4] != ZIP_MAGIC:
        if head.lstrip()[:1] == b"<":
            raise DownloadError("got HTML instead of a zip (Box login or error page?)")
        raise DownloadError("file is not a zip (missing PK header)")
    if not zipfile.is_zipfile(path):
        raise DownloadError("file is not a valid zip")


def _fetch(
    url: str, dest: Path, session: Any, headers: dict[str, str], max_bytes: int, timeout: float
) -> None:
    resp = session.get(url, stream=True, timeout=timeout, headers=headers)
    try:
        resp.raise_for_status()
        size = 0
        with dest.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 16):
                size += len(chunk)
                if size > max_bytes:
                    raise DownloadError(f"download exceeded size cap of {max_bytes} bytes")
                f.write(chunk)
    finally:
        resp.close()


def download(
    urls: tuple[str, ...],
    dest: Path,
    *,
    source_url: str | None = None,
    session: Any = None,
    user_agent: str = "fishbaseball-data",
    max_bytes: int = DEFAULT_MAX_BYTES,
    timeout: float = DEFAULT_TIMEOUT,
) -> Path:
    """Fetch a zip to `dest`, trying each URL in turn. `source_url` overrides all `urls`
    and may be a local file path or file:// URL. Never leaves a non-zip at `dest`."""
    from pipelines.core.box import download_folder, is_box_share  # box imports this module

    dest.parent.mkdir(parents=True, exist_ok=True)
    candidates = (source_url,) if source_url else urls
    if not candidates:
        raise DownloadError("no download URL configured; pass --source-url")
    session = session or requests.Session()
    headers = {"User-Agent": user_agent}
    errors: list[str] = []
    for url in candidates:
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            local = Path(url.removeprefix("file://"))
            if is_box_share(url):
                download_folder(
                    url, tmp, session, user_agent=user_agent, max_bytes=max_bytes, timeout=timeout
                )
            elif not url.startswith(("http://", "https://")) and local.is_file():
                if local.stat().st_size > max_bytes:
                    raise DownloadError(f"download exceeded size cap of {max_bytes} bytes")
                shutil.copyfile(local, tmp)
            else:
                _fetch(url, tmp, session, headers, max_bytes, timeout)
            ensure_zip(tmp)
            tmp.replace(dest)
            return dest
        except (DownloadError, requests.RequestException, RuntimeError, OSError) as e:
            errors.append(f"{url}: {e}")
        finally:
            tmp.unlink(missing_ok=True)
    raise DownloadError("all downloads failed:\n" + "\n".join(errors))
