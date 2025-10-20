import hashlib
import os
import sys
import re
import urllib.parse as ul
from pathlib import Path
from tqdm import tqdm
from argparse import ArgumentParser

import requests

DATADRYAD_URL = "https://datadryad.org/{}"
API_FILE   = DATADRYAD_URL.format("/api/v2/files/{}")
TIMEOUT    = (5, 30)           # connect, read
CHUNK      = 1024 * 1024       # 1 MiB

def extract_file_id(arg: str) -> str:
    """Return the numeric file-id from an id *or* full download URL."""
    m = re.search(r'/files/(\d+)', arg)
    return m.group(1) if m else arg.strip()

def get_metadata(fid: str) -> dict:
    url = API_FILE.format(fid)
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def sha256sum(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()

def download(url: str, dest: Path, total_bytes: int):
    with requests.get(url, stream=True, timeout=TIMEOUT, allow_redirects=True) as r:
        r.raise_for_status()
        with dest.open('wb') as fh, tqdm(
            total=total_bytes,
            unit='B', unit_scale=True, unit_divisor=1024,
            desc=dest.name, ascii=False, dynamic_ncols=True
        ) as bar:
            for chunk in r.iter_content(CHUNK):
                if chunk:
                    fh.write(chunk)
                    bar.update(len(chunk))

def main():
    parser = ArgumentParser(
        description="Download one file from Dryad, save it with its original name, and verify integrity with Dryad's MD5 checksum.",
        usage="python download_dryad_file.py <file-id|download-URL> <output-directory>"
    )
    parser.add_argument("--fid", help="Dryad file ID or full download link", required=True)
    parser.add_argument("--outdir", help="Output directory", required=True)
    args = parser.parse_args()

    fid    = extract_file_id(args.fid)
    outdir = Path(args.outdir).expanduser().resolve()

    outdir.mkdir(parents=True, exist_ok=True)

    meta = get_metadata(fid)
    print(meta)
    filename = meta["path"]
    expected = meta["digest"]
    size     = meta["size"]
    target   = outdir / filename

    print(f"downloading {filename} ({size} bytes) to {target}...")
    download_link = DATADRYAD_URL.format(meta["_links"]["stash:download"]["href"])
    download(download_link, target, size)
    print("↪ download complete, verifying checksum...")

    actual = sha256sum(target)
    if actual.lower() != expected.lower():
        target.unlink(missing_ok=True)
        sys.exit(f"ERROR: MD5 mismatch! Expected {expected}, got {actual}")
    print("OK - checksums match.")

if __name__ == "__main__":
    main()
