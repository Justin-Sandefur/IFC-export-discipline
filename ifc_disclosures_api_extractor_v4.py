#!/usr/bin/env python3
import argparse, csv, io, re, json
from dataclasses import dataclass, asdict
from typing import Any, List, Tuple, Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text

# --------------------------
# Section heading heuristics
# --------------------------
DESC_TITLES = [
    "project overview",
    "project description",
    "summary of project information",
    "project description and background",
    "description of company",
    "description of company and purpose of project",
]

# Conservative stop cues (we REMOVED “environmental”)
NEXT_CUES = [
    "summary of investment information",
    "proposed ifc investment",
    "location",
    "status",
    "contacts",
    "sponsor",
    "client",
    "financing plan",
    "project components",
    "documents",
    "disclosure",
    "additionality",
    "expected development impact",
    "ifc's role",
    "ifc’s role",
    "risk",
    "legal",
]

# SPI boilerplate avoidance
BOILERPLATE_PREFIXES = [
    "summary of project information (spi) is prepared and distributed to the public in advance of the ifc board of directors' consideration",
    "summary of project information (spi) is prepared and distributed to the public in advance of the ifc board of directors’ consideration",
]
LIKELY_SPI_MARKERS = [
    "project name:", "region:", "sector:", "project no:", "project number:",
    "company name:", "description of company", "purpose of project",
    "environmental category", "and issues", "description of location:"
]

def looks_like_boilerplate(text_low: str) -> bool:
    t = text_low.strip()
    return any(t.startswith(p) for p in BOILERPLATE_PREFIXES)

def looks_like_real_spi_body(text_low: str) -> bool:
    return any(m in text_low for m in LIKELY_SPI_MARKERS)

# --------------------------
# Amount extraction heuristics
# --------------------------
AMOUNT_REGEXES = [
    r'(?:US\$|USD|\$)\s?([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s?(billion|million|thousand|bn|mn|m|k)?',
    r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s?(billion|million|thousand|bn|mn|m|k)\s?(?:US\$|USD|\$)'
]
UNIT_MULT = {None:1,'billion':1e9,'bn':1e9,'million':1e6,'mn':1e6,'m':1e6,'thousand':1e3,'k':1e3}

# Precise IFC commitment phrase patterns
IFC_COMMIT_PATTERNS = [
    r"ifc[’']?s?\s+(?:equity|loan|debt|investment|guarantee)\s+(?:would be|will be|is|of|amounts? to)\s+(?:up to\s+)?(?P<cur>US\$|USD|\$)?\s?(?P<num>[0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s?(?P<unit>billion|million|thousand|bn|mn|m|k)?",
    r"ifc\s+(?:proposes?|intends?|would|will)\s+to\s+(?:invest|provide|lend)\s+(?:up to\s+)?(?P<cur>US\$|USD|\$)?\s?(?P<num>[0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)\s?(?P<unit>billion|million|thousand|bn|mn|m|k)?"
]
IFC_ANCHORS = [
    'ifc investment',"ifc's investment",'ifc proposes to invest','ifc intends to invest','ifc would invest',
    'ifc investment of up to','ifc is considering an investment','ifc is proposing an investment',
    'ifc loan','ifc equity','ifc guarantee','with ifc investment of','ifc participation','ifc commit'
]
FACILITY_TERMS = ['facility','portfolio','program','ceiling','envelope','guarantee capacity','up to']
SENT_SPLIT = re.compile(r'(?<=[\.\?\!])\s+')

# --------------------------
# Text utils
# --------------------------
def normalize_ws(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "").replace("\xa0"," ")).strip()

def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    return soup.get_text("\n", strip=True)

def slice_section_from_html_block(html: str) -> Tuple[str, str]:
    """
    Parse HTML, find a heading whose text matches one of DESC_TITLES,
    then collect following siblings until the next heading.
    """
    soup = BeautifulSoup(html or "", "lxml")
    candidates = soup.find_all(["h1","h2","h3","h4","h5","h6","strong","b"])
    def norm(s): return re.sub(r"[ \t]+"," ", (s or "").strip()).lower()
    anchor = None
    matched = ""
    for tag in candidates:
        t = norm(tag.get_text(" ", strip=True))
        if t in DESC_TITLES:
            anchor = tag
            matched = t
            break
    if not anchor:
        # fall back to plain-text slice
        txt = soup.get_text("\n", strip=True)
        sec, title = slice_section(txt)
        return sec, title

    out = []
    for sib in anchor.next_siblings:
        name = getattr(sib, "name", None)
        if name in ["h1","h2","h3","h4","h5","h6","strong","b"]:
            break
        if name in ["p","div","li","td","ul","ol"]:
            t = sib.get_text("\n", strip=True)
            if t: out.append(t)
        if name == "br":
            out.append("\n")
        if isinstance(sib, str):
            s = sib.strip()
            if s: out.append(s)

    section = normalize_ws("\n".join(out).strip())
    return (section, matched) if section else ("","")

def slice_section(full_text: str) -> Tuple[str, str]:
    """
    Softer text fallback: start after a known heading; only stop at cues
    that appear after 400 chars to avoid chopping immediately.
    """
    t = full_text or ""
    low = t.lower()
    for key in DESC_TITLES:
        start = low.find(key)
        if start == -1:
            continue
        # body after heading line
        body = t[start:].split("\n", 1)
        candidate = body[1] if len(body) > 1 else body[0]
        end = len(candidate)
        low_cand = candidate.lower()
        for cue in NEXT_CUES:
            j = low_cand.find(cue)
            if j != -1 and j > 400:  # require sufficient content
                end = min(end, j)
        chunk = normalize_ws(candidate[:end].strip())
        if chunk:
            return chunk, key
    return "", ""

# --------------------------
# Amount parsing helpers
# --------------------------
def _to_float(num_str: str) -> Optional[float]:
    try:
        return float(num_str.replace(',', ''))
    except Exception:
        return None

def _normalize_amount(num_str: str, unit: Optional[str]) -> Optional[float]:
    base = _to_float(num_str)
    if base is None: return None
    unit = (unit or '').lower()
    mult = UNIT_MULT.get(unit, 1.0)
    return base * mult

def find_amounts(text: str) -> List[dict]:
    hits = []
    for pat in AMOUNT_REGEXES:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            num = m.group(1)
            unit = m.group(2) if m.lastindex and m.lastindex >= 2 else None
            val = _normalize_amount(num, unit)
            if val:
                hits.append({'raw': m.group(0), 'amount': val})
    # dedupe by raw
    seen, out = set(), []
    for h in hits:
        if h['raw'] in seen: continue
        seen.add(h['raw']); out.append(h)
    return out

def amounts_with_context(text: str) -> List[dict]:
    results = []
    sentences = SENT_SPLIT.split(text)
    for sent in sentences:
        a = find_amounts(sent)
        for hit in a:
            results.append({
                'raw': hit['raw'],
                'amount_usd': hit['amount'],
                'context': normalize_ws(sent)[:300]
            })
    return results

def pick_ifc_investment(text: str) -> Tuple[Optional[float], Optional[str]]:
    # 1) Direct phrase-level matches (highest confidence)
    for pat in IFC_COMMIT_PATTERNS:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            num, unit = m.group('num'), m.group('unit')
            val = _normalize_amount(num, unit)
            if val:
                start = max(0, m.start() - 140); end = min(len(text), m.end() + 140)
                ctx = normalize_ws(text[start:end])
                return val, f"direct phrase: {ctx}"
    # 2) Sentence-level heuristic with IFC + invest/loan/equity/guarantee
    sentences = SENT_SPLIT.split(text)
    candidates = []
    for sent in sentences:
        l = sent.lower()
        if 'ifc' in l and any(k in l for k in ['invest','loan','equity','guarantee','provide','lend','commit']):
            for a in find_amounts(sent):
                candidates.append((a['amount'], normalize_ws(sent)))
    if candidates:
        chosen = min(candidates, key=lambda x: x[0])
        return chosen[0], f"ifc-sentence: {chosen[1]}"
    # 3) Small window around “IFC”
    for i, sent in enumerate(sentences):
        if 'ifc' in sent.lower():
            window = " ".join(sentences[max(0, i-1):min(len(sentences), i+2)])
            amts = find_amounts(window)
            if amts:
                chosen = min(amts, key=lambda x: x['amount'])
                return chosen['amount'], f"ifc-window: {normalize_ws(window)}"
    return None, None

def pick_facility_notional(text: str) -> Tuple[Optional[float], Optional[str]]:
    sentences = SENT_SPLIT.split(text)
    cands = []
    for sent in sentences:
        low = sent.lower()
        if any(term in low for term in FACILITY_TERMS):
            for a in find_amounts(sent):
                cands.append((a['amount'], normalize_ws(sent)))
    if cands:
        chosen = max(cands, key=lambda x: x[0])
        return chosen[0], f"facility sentence: {chosen[1]}"
    amts_all = find_amounts(text)
    if amts_all:
        top = max(amts_all, key=lambda x: x['amount'])
        return top['amount'], f"page max: {top['raw']}"
    return None, None

# --------------------------
# JSON & PDF processing
# --------------------------
def walk_strings(obj: Any, out: List[str]):
    if isinstance(obj, dict):
        for v in obj.values(): walk_strings(v, out)
    elif isinstance(obj, list):
        for it in obj: walk_strings(it, out)
    elif isinstance(obj, str):
        out.append(obj)

def from_json_payload(j: Any) -> Tuple[str, str, str]:
    strings: List[str] = []
    walk_strings(j, strings)
    strings.sort(key=lambda s: len(s or ""), reverse=True)

    # 1) HTML-aware block (preferred)
    for s in strings:
        sec, title = slice_section_from_html_block(s)
        if sec:
            low = sec.lower()
            if not looks_like_boilerplate(low) and looks_like_real_spi_body(low):
                return sec, title, "json_payload(html_block)"
    # 2) HTML->text
    for s in strings:
        txt = html_to_text(s)
        sec, title = slice_section(txt)
        if sec:
            low = sec.lower()
            if not looks_like_boilerplate(low) and looks_like_real_spi_body(low):
                return sec, title, "json_payload(html_text)"
    # 3) Raw text
    for s in strings:
        sec, title = slice_section(s)
        if sec:
            low = sec.lower()
            if not looks_like_boilerplate(low) and looks_like_real_spi_body(low):
                return sec, title, "json_payload(raw)"
    return "", "", ""

def extract_from_pdf_bytes(b: bytes) -> Tuple[str, str]:
    try:
        txt = pdf_extract_text(io.BytesIO(b)) or ""
    except Exception:
        return "", ""
    return slice_section(txt)

def find_pdf_urls_in_json(j: Any) -> List[str]:
    urls: List[str] = []
    strings: List[str] = []
    walk_strings(j, strings)
    for s in strings:
        for m in re.finditer(r"https?://[^\s\"']+\.pdf\b", s, flags=re.IGNORECASE):
            urls.append(m.group(0))
        for m in re.finditer(r"https?://[^\s\"']+/api/File/downloadfile\?id=[^\"'\s]+", s, flags=re.IGNORECASE):
            urls.append(m.group(0))
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def parse_id_and_type(url: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        p = urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        for i, seg in enumerate(parts):
            if seg == "project-detail" and i+2 < len(parts):
                doc_type = parts[i+1].upper()  # SPI or SII
                proj_id = parts[i+2]
                proj_id = re.sub(r"^0+","", proj_id) or proj_id
                return proj_id, doc_type
    except Exception:
        pass
    return None, None

# --------------------------
# Output row
# --------------------------
@dataclass
class RowOut:
    project_id: str
    project_name: str
    url: str
    http_status: Optional[int]
    fetch_status: str
    section_text: str
    section_title_found: str
    extraction_method: str
    used_json_endpoint: str
    used_pdf_url: str
    ifc_investment_usd: Optional[float]
    ifc_investment_note: Optional[str]
    facility_notional_usd: Optional[float]
    facility_note: Optional[str]
    all_amount_mentions: str
    amounts_json: str

# --------------------------
# Main fetcher
# --------------------------
def fetch_one(session: requests.Session, url: str, pname: str, timeout=30) -> RowOut:
    proj_id, doc_type = parse_id_and_type(url)
    if not proj_id or doc_type not in ("SPI","SII"):
        return RowOut(proj_id or "", pname, url, None, "error:bad_url_format", "", "", "", "", None, None, None, None, "", "")

    api = f"https://disclosuresservice.ifc.org/api/ProjectAccess/{doc_type}Project?projectId={proj_id}"
    try:
        r = session.get(api, timeout=timeout)
        status = r.status_code
        j = r.json()
    except Exception as e:
        return RowOut(proj_id, pname, url, None, f"error:api:{type(e).__name__}:{e}", "", "", api, "", None, None, None, None, "", "")

    # 1) Section text
    sec, title, method = from_json_payload(j)

    # 2) If no section, try PDFs mentioned in JSON
    used_pdf = ""
    if not sec:
        pdf_urls = find_pdf_urls_in_json(j)
        for pu in pdf_urls[:5]:
            try:
                pr = session.get(pu, timeout=timeout)
                if pr.status_code == 200 and pr.content and len(pr.content) > 200:
                    sec2, title2 = extract_from_pdf_bytes(pr.content)
                    if sec2:
                        sec, title, method = sec2, title2, "pdf_fallback"
                        used_pdf = pu
                        break
            except Exception:
                continue

    # 3) Build corpus for amounts
    if sec:
        text_corpus = sec
    else:
        strings: List[str] = []
        walk_strings(j, strings)
        strings.sort(key=lambda s: len(s or ""), reverse=True)
        text_corpus = "\n\n".join(html_to_text(s) for s in strings[:10])

    # 4) Amount extraction
    amount_hits = amounts_with_context(text_corpus)
    ifc_amt, ifc_note = pick_ifc_investment(text_corpus)
    fac_amt, fac_note = pick_facility_notional(text_corpus)

    all_mentions = " | ".join([f"{h['raw']}=>{int(h['amount_usd'])}" for h in amount_hits]) if amount_hits else ""
    amounts_json = json.dumps(amount_hits, ensure_ascii=False)

    fetch_status = "ok" if sec else "ok_but_no_section_found"

    return RowOut(
        project_id=proj_id, project_name=pname, url=url,
        http_status=status, fetch_status=fetch_status,
        section_text=sec, section_title_found=title, extraction_method=method,
        used_json_endpoint=api, used_pdf_url=used_pdf,
        ifc_investment_usd=ifc_amt, ifc_investment_note=ifc_note,
        facility_notional_usd=fac_amt, facility_note=fac_note,
        all_amount_mentions=all_mentions, amounts_json=amounts_json
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--url-col", default="Project Url")
    ap.add_argument("--name-col", default="Project Name")
    ap.add_argument("--max-rows", type=int, default=0)
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    rows = df.to_dict(orient="records")
    if args.max_rows and args.max_rows > 0:
        rows = rows[:args.max_rows]

    out_header = [
        "project_id","project_name","url","http_status","fetch_status",
        "section_text","section_title_found","extraction_method",
        "used_json_endpoint","used_pdf_url",
        "ifc_investment_usd","ifc_investment_note",
        "facility_notional_usd","facility_note",
        "all_amount_mentions","amounts_json"
    ]

    with requests.Session() as s, open(args.output, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=out_header)
        writer.writeheader()
        for i, r in enumerate(rows, 1):
            row = fetch_one(s, str(r[args.url_col]).strip(), str(r.get(args.name_col,"")))
            writer.writerow(asdict(row))
            if i % 10 == 0: fh.flush()

if __name__ == "__main__":
    main()
