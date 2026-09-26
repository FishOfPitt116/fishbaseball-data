"""Download a public Box shared *folder* of CSVs as one deterministic zip.

Box has no stable direct-download URL for a folder, so we read the shared page (paginated with
`?page=N`, ~20 items each), take each file's id from the JSON embedded in it, and fetch files
through Box's shared-file download endpoint. Pagination continues until a page adds no new files,
so files SABR adds later are picked up without a code change.
"""

from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from pipelines.core.download import DEFAULT_MAX_BYTES, DEFAULT_TIMEOUT, DownloadError

BOX_SHARE = re.compile(r"^https://(?:[a-z0-9-]+\.)*box\.com/s/([A-Za-z0-9]+)/?$")
ITEM_START = re.compile(r'\{"typedID"\s*:\s*"f_')
MAX_PAGES = 100
ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)  # fixed timestamps: same content -> same zip bytes


@dataclass(frozen=True)
class BoxFile:
    name: str
    file_id: str  # e.g. "f_2084259918153"


def is_box_share(url: str) -> bool:
    return BOX_SHARE.match(url) is not None


def parse_items(html: str) -> list[BoxFile]:
    """File items from the JSON objects embedded in a shared-folder page."""
    decoder = json.JSONDecoder()
    files: dict[str, BoxFile] = {}
    for m in ITEM_START.finditer(html):
        try:
            obj, _ = decoder.raw_decode(html, m.start())
        except ValueError:
            continue
        if obj.get("type") == "file" and obj.get("name") and obj.get("typedID"):
            files.setdefault(obj["typedID"], BoxFile(obj["name"], obj["typedID"]))
    return list(files.values())


def list_folder(
    share_url: str,
    session: Any,
    *,
    headers: dict[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[BoxFile]:
    base = share_url.rstrip("/")
    seen: dict[str, BoxFile] = {}
    for page in range(1, MAX_PAGES + 1):
        resp = session.get(f"{base}?page={page}", timeout=timeout, headers=headers or {})
        resp.raise_for_status()
        new = [f for f in parse_items(resp.text) if f.file_id not in seen]
        if not new:
            break
        seen.update({f.file_id: f for f in new})
    return list(seen.values())


def download_folder(
    share_url: str,
    dest: Path,
    session: Any = None,
    *,
    user_agent: str = "fishbaseball-data",
    max_bytes: int = DEFAULT_MAX_BYTES,
    timeout: float = DEFAULT_TIMEOUT,
) -> Path:
    """Zip every CSV in the folder (sorted, fixed timestamps) to `dest`."""
    session = session or requests.Session()
    headers = {"User-Agent": user_agent}
    files = [
        f for f in list_folder(share_url, session, headers=headers, timeout=timeout)
        if f.name.lower().endswith(".csv")
    ]  # fmt: skip
    if not files:
        raise DownloadError(f"no files found in Box folder {share_url}")
    parsed = urlparse(share_url)
    shared_name = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    total = 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(files, key=lambda f: f.name):
                url = (
                    f"https://{parsed.netloc}/index.php?rm=box_download_shared_file"
                    f"&shared_name={shared_name}&file_id={f.file_id}"
                )
                resp = session.get(url, stream=True, timeout=timeout, headers=headers)
                try:
                    resp.raise_for_status()
                    if resp.headers.get("Content-Type", "").startswith("text/html"):
                        raise DownloadError(f"got HTML from Box for {f.name}")
                    chunks = []
                    for chunk in resp.iter_content(chunk_size=1 << 16):
                        total += len(chunk)
                        if total > max_bytes:
                            raise DownloadError(f"download exceeded size cap of {max_bytes} bytes")
                        chunks.append(chunk)
                finally:
                    resp.close()
                info = zipfile.ZipInfo(f.name, ZIP_EPOCH)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                zf.writestr(info, b"".join(chunks))
    except BaseException:
        dest.unlink(missing_ok=True)
        raise
    return dest
