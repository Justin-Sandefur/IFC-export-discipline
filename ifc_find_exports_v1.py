#!/usr/bin/env python3
"""
Scan IFC disclosure pages for references to 'export' and capture the sentences.

Usage (test a small batch first):
  python -u ifc_find_exports_v1.py \
    --input "ifc_investment_services_projects_11-05-2025.csv" \
    --output "out_ifc_exports.csv" \
    --url-col "Project Url" \
    --name-col "Project Name" \
    --max-rows 50
"""

import argparse, csv, json, re, time, random
from dataclasses import dataclass, asdict
from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- sentence splitting & keyword matching ---
SENT_SPLIT = re.compile(r'(?<=[\.\?\!])\s+|[\r\n]+')
EXPORT_RE = re.compile(r'\bexport\w*', re.IGNORECASE)   # export, exports, exporting, exporter(s)

def normalize_ws(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "").replace("\xa0", " ")).strip()

def soup_text(s: str) -> str:
    """Strip HTML/XML to text safely."""
    if not s:
        return ""
    try:
        # pick xml parser if it looks like xml; else html
        looks_xml = s.strip().lower().startswith("<?xml") or s.strip().lower().startswith("<xml")
        bs = BeautifulSoup(s, "xml" if looks_xml else "html.parser")
        return bs.get_text("\n", strip=True)
    except Exception:
        return s

def walk_strings(obj: Any, out: List[str]) -> None:
    """Collect all string leaves from nested JSON."""
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, list):
        for it in obj:
            walk_strings(it, out)
    elif isinstance(obj, dict):
        for v in obj.values():
            walk_strings(v, out)

def parse_id_and_type(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract projectId and doc type (SPI/SII) from a disclosure URL."""
    try:
        p = urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        for i, seg in enumerate(parts):
            if seg == "project-detail" and i + 2 < len(parts):
                dtype = parts[i + 1].upper()  # SPI or SII
                pid = re.sub(r"^0+", "", parts[i + 2])
                return pid, dtype
    except Exception:
        pass
    return None, None

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) IFC-Export-Scanner/1.0",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
    })
    s.timeout = 30
    return s

def get_json(session: requests.Session, url: str, retries: int = 3) -> Tuple[Optional[Any], Optional[int]]:
    last = None
    for _ in range(retries):
        try:
            r = session.get(url, timeout=30)
            last = r.status_code
            if r.ok:
                return r.json(), r.status_code
        except Exception:
            pass
        time.sleep(0.5 + random.random()*0.7)
    return None, last

def sentences_with_export(text: str, max_sentences: int = 20) -> List[str]:
    """Return deduplicated sentences containing 'export*' with light cleanup."""
    hits, seen = [], set()
    if not text:
        return hits
    for sent in SENT_SPLIT.split(text):
        s = normalize_ws(sent)
        if not s:
            continue
        if EXPORT_RE.search(s):
            key = s.lower()
            if key not in seen:
                seen.add(key)
                hits.append(s)
                if len(hits) >= max_sentences:
                    break
    return hits

@dataclass
class OutRow:
    project_id: str
    project_name: str
    url: str
    http_status: Optional[int]
    fetch_status: str
    used_json_endpoints: str
    export_hits: int
    export_sentences: str  # pipe-separated
    text_scanned_chars: int

def fetch_one(session: requests.Session, url: str, name: str) -> OutRow:
    pid, _ = parse_id_and_type(url)
    if not pid:
        return OutRow("", name, url, None, "error:bad_url", "", 0, "", 0)

    endpoints = [
        f"https://disclosuresservice.ifc.org/api/ProjectAccess/SPIProject?projectId={pid}",
        f"https://disclosuresservice.ifc.org/api/ProjectAccess/SIIProject?projectId={pid}",
        f"https://disclosuresservice.ifc.org/api/ProjectAccess/validateProjectUrl?ProjectNumber={pid}&documentType=SPI",
        f"https://disclosuresservice.ifc.org/api/searchprovider/landingPageDetails?isLanding=1",
    ]

    used, statuses, payload_strings = [], [], []
    for ep in endpoints:
        j, st = get_json(session, ep)
        if st is not None:
            statuses.append(st)
        if j is not None:
            used.append(ep)
            walk_strings(j, payload_strings)

    # Sort strings by length so bigger narrative blocks are scanned first
    payload_strings.sort(key=lambda s: len(s or ""), reverse=True)
    # Convert all to plain text and join a subset to keep it fast
    texts = [soup_text(s) for s in payload_strings[:120]]
    big = "\n".join(t for t in texts if t)

    hits = sentences_with_export(big, max_sentences=24)
    status = statuses[-1] if statuses else None
    fetch_status = "ok" if texts else "ok_but_no_text"
    return OutRow(
        project_id=pid,
        project_name=name,
        url=url,
        http_status=status,
        fetch_status=fetch_status,
        used_json_endpoints=" | ".join(used),
        export_hits=len(hits),
        export_sentences=" || ".join(hits),
        text_scanned_chars=len(big)
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV/TSV with at least the disclosure URL")
    ap.add_argument("--output", required=True, help="Output CSV with export hits")
    ap.add_argument("--url-col", default="Project Url")
    ap.add_argument("--name-col", default="Project Name")
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()

    # Let pandas sniff delimiter (CSV or TSV)
    df = pd.read_csv(args.input, sep=None, engine="python")
    cols = list(df.columns)
    if args.url_col not in cols:
        raise SystemExit(f"[fatal] URL column not found: {args.url_col}\nAvailable: {cols}")
    if args.name_col not in cols:
        print(f"[warn] name column '{args.name_col}' not found; using empty names.")
    rows = df.to_dict(orient="records")
    if args.max_rows and args.max_rows > 0:
        rows = rows[:args.max_rows]

    out_fields = [
        "project_id","project_name","url","http_status","fetch_status",
        "used_json_endpoints","export_hits","export_sentences","text_scanned_chars"
    ]

    with open(args.output, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_fields)
        writer.writeheader()
        fh.flush()
        with make_session() as s:
            for i, r in enumerate(rows, 1):
                url = str(r[args.url_col]).strip()
                name = str(r.get(args.name_col, "")) if args.name_col in r else ""
                if not url or not url.startswith("http"):
                    print(f"[{i}] skip (no url): {name}")
                    continue
                print(f"[{i}/{len(rows)}] {url}")
                try:
                    out = fetch_one(s, url, name)
                except Exception as e:
                    print(f"[{i}] ERROR: {e}")
                    out = OutRow("", name, url, None, f"error:{type(e).__name__}:{e}", "", 0, "", 0)
                writer.writerow(asdict(out))
                if i % 10 == 0:
                    fh.flush()
                time.sleep(0.12)  # be polite

    print(f"[done] wrote: {args.output}")

if __name__ == "__main__":
    main()
