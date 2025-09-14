#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STRICT Smart Match scraper for Huizemark (Agent 75570 only)

Fixes:
- Beds come from structured sources first (JSON-LD, labelled rows), NOT the title.
- For types without bedrooms (vacant-land, commercial, etc.), beds = 0 (accepted).
- Area is parsed from the URL location segment (city/area), never the property type.
- Ownership is verified for agent 75570.
- listings.json contains ONLY complete items: ref, title, url, price, beds, area.

Run (from smart-match):
  pip install requests beautifulsoup4 lxml
  python scripts/fetch_stock.py
"""

import re
import sys
import json
import time
from pathlib import Path
from typing import List, Dict, Optional, Iterable

import requests
from bs4 import BeautifulSoup

# -------------------- CONSTANTS --------------------

AGENT_ID   = "75570"
AGENT_SLUG = "blessing-nsibande"

RESULTS_URL = f"https://www.huizemark.com/results/agent/{AGENT_ID}/"
PROFILE_URL = f"https://www.huizemark.com/agents/{AGENT_SLUG}/{AGENT_ID}/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (SmartMatchBot; +https://github.com/) PythonRequests",
    "Accept-Language": "en-ZA,en;q=0.9",
}
TIMEOUT          = 30
MAX_PAGES        = 20
LIST_SLEEP_SEC   = 0.8
DETAIL_SLEEP_SEC = 0.5

# Paths
SCRIPT_DIR  = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OUT_JSON    = PROJECT_DIR / "data" / "listings.json"
OUT_DEBUG   = PROJECT_DIR / "data" / "debug_skipped.json"

# Regex helpers
RESIDENTIAL_HREF_RE = re.compile(r"/results/residential/")
MONEY_RE            = re.compile(r"(R\s*[\d\s,.'’]+)", re.I)
BEDS_LABEL_RE       = re.compile(r"(Bedrooms?|Beds?)\s*[:\-]?\s*(\d+)", re.I)
BEDS_WORD_RE        = re.compile(r"(\d+)\s*bed(?:room)?s?\b", re.I)

# Ref patterns (RL codes or ≥5-digit numeric) with label variants
REF_RL_RE       = re.compile(r"\b(RL\d{3,})\b", re.I)
REF_NUMERIC_RE  = re.compile(r"\b(\d{5,})\b")
REF_LABELLED_RE = re.compile(
    r"(?:Web\s*Ref(?:erence)?|Ref(?:erence)?(?:\s*No\.?)?)\s*[:#\-\u00A0]?\s*([A-Za-z]{0,3}\d{5,}|\d{5,})",
    re.I
)

# Agent verification signals
AGENT_NAME_RE      = re.compile(r"\bblessing\b.*\bnsibande\b", re.I)
AGENT_URL_SNIPPETS = [
    f"/agents/{AGENT_SLUG}/{AGENT_ID}/",
    f"/results/agent/{AGENT_ID}/",
]

# Types that do/don’t require bedrooms
TYPES_REQUIRE_BEDS = {
    "house", "apartment", "flat", "townhouse", "cluster", "duplex",
    "simplex", "loft", "cottage", "villa", "maisonette", "penthouse"
}
TYPES_NO_BEDS = {
    "vacant-land", "land", "plot", "farm", "smallholding",
    "commercial", "industrial", "office", "retail", "warehouse",
    "development", "site", "stand"
}

# -------------------- utilities --------------------

def make_abs(url: str) -> str:
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return "https://www.huizemark.com" + url
    return "https://www.huizemark.com/" + url

def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def to_int_money(txt: Optional[str]) -> Optional[int]:
    if not txt:
        return None
    digits = re.sub(r"[^\d]", "", str(txt))
    return int(digits) if digits else None

def to_int(txt: Optional[str]) -> Optional[int]:
    if not txt:
        return None
    m = re.search(r"\d+", str(txt))
    return int(m.group(0)) if m else None

def parse_location_from_url(url: str) -> Optional[str]:
    """
    /results/residential/for-sale/<city>/<area>/<type>/<id>/
      -> area = parts[i+2]  (city is i+1; we keep just area to match your JSON shape)
    """
    try:
        path = re.sub(r"^https?://[^/]+", "", url).strip("/")
        parts = re.split(r"/+", path)
        if "for-sale" in parts:
            i = parts.index("for-sale")
        elif "to-let" in parts:
            i = parts.index("to-let")
        else:
            return None
        if len(parts) > i + 2:
            area_slug = parts[i + 2]
            return area_slug.replace("-", " ").title()
    except Exception:
        pass
    return None

def property_type_from_url(url: str) -> Optional[str]:
    try:
        path = re.sub(r"^https?://[^/]+", "", url).strip("/")
        parts = re.split(r"/+", path)
        if "for-sale" in parts:
            i = parts.index("for-sale")
        elif "to-let" in parts:
            i = parts.index("to-let")
        else:
            return None
        if len(parts) > i + 3:
            return parts[i + 3].lower()
    except Exception:
        pass
    return None

def last_numeric_segment_from_url(url: str) -> Optional[str]:
    m = re.search(r"/(\d{5,})(?:/|$)", url)
    return m.group(1) if m else None

# -------------------- JSON-LD helpers --------------------

def iter_jsonld(soup: BeautifulSoup) -> Iterable[dict]:
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue
        yield data

def flatten_json(obj) -> Iterable[dict]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from flatten_json(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from flatten_json(v)

def any_url_points_to_agent(url_val: Optional[str]) -> bool:
    if not isinstance(url_val, str):
        return False
    u = url_val.lower()
    return any(snippet in u for snippet in AGENT_URL_SNIPPETS)

def any_name_is_agent(name_val: Optional[str]) -> bool:
    if not isinstance(name_val, str):
        return False
    return bool(AGENT_NAME_RE.search(name_val))

# -------------------- collect detail URLs --------------------

def parse_results_for_detail_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: List[str] = []
    for a in soup.find_all("a", href=RESIDENTIAL_HREF_RE):
        href = a.get("href")
        if not href:
            continue
        url = make_abs(href)
        if "/results/residential/" in url:
            urls.append(url)
    seen = set(); uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

def parse_profile_for_detail_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: List[str] = []
    for a in soup.find_all("a", href=RESIDENTIAL_HREF_RE):
        href = a.get("href")
        if not href:
            continue
        urls.append(make_abs(href))
    # JSON-LD fallback
    for data in iter_jsonld(soup):
        for node in flatten_json(data):
            if not isinstance(node, dict):
                continue
            url = node.get("url") or node.get("@id")
            if isinstance(url, str) and "/results/residential/" in url:
                urls.append(make_abs(url))
    seen = set(); uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

def find_next_link(html: str) -> Optional[str]:
    m = re.search(r'<a[^>]+rel="next"[^>]*href="([^"]+)"', html, flags=re.I)
    if m: return m.group(1)
    m2 = re.search(r'<a[^>]*href="([^"]+)"[^>]*>\s*(Next|»|Next\s*Page)\s*<', html, flags=re.I)
    if m2: return m2.group(1)
    return None

# -------------------- extraction --------------------

TITLE_SELECTORS = [
    "h1", "h1.property-title", ".property-title", ".title", "h1[itemprop='name']",
    "meta[property='og:title']"
]

def get_meta_content(soup: BeautifulSoup, prop: str) -> Optional[str]:
    tag = soup.select_one(f"meta[property='{prop}']")
    if tag and tag.get("content"):
        return tag["content"]
    return None

def verify_agent_ownership(soup: BeautifulSoup, full_text: str) -> bool:
    # 1) Anchor hrefs to agent pages
    for a in soup.find_all("a", href=True):
        href = (a["href"] or "").lower()
        if f"/agents/{AGENT_SLUG}/{AGENT_ID}/" in href:
            return True
        if f"/results/agent/{AGENT_ID}/" in href:
            return True
    # 2) JSON-LD agent signals
    for data in iter_jsonld(soup):
        for node in flatten_json(data):
            if not isinstance(node, dict):
                continue
            for key in ("url", "@id", "sameAs"):
                val = node.get(key)
                if isinstance(val, str) and any_url_points_to_agent(val):
                    return True
                if isinstance(val, list) and any(any_url_points_to_agent(x) for x in val if isinstance(x, str)):
                    return True
            for key in ("name", "agent", "seller", "brand", "author"):
                val = node.get(key)
                if isinstance(val, str) and any_name_is_agent(val):
                    return True
                if isinstance(val, dict):
                    if any_name_is_agent(val.get("name")) or any_url_points_to_agent(val.get("url")):
                        return True
    # 3) Fallbacks
    canonical = (get_meta_content(soup, "og:url") or "").lower()
    if any_url_points_to_agent(canonical):
        return True
    lt = full_text.lower()
    if any(snippet in lt for snippet in AGENT_URL_SNIPPETS):
        return True
    if any_name_is_agent(full_text) and "/results/residential/" in lt:
        return True
    return False

def extract_price(soup: BeautifulSoup, text: str) -> Optional[int]:
    # meta
    meta_price = soup.select_one("meta[itemprop='price']")
    if meta_price and meta_price.get("content"):
        p = to_int_money(meta_price["content"])
        if p: return p
    # css
    for sel in (".price", ".property-price", "[class*='price']"):
        n = soup.select_one(sel)
        if n:
            p = to_int_money(n.get_text(" ", strip=True))
            if p: return p
    # regex
    m = MONEY_RE.search(text)
    if m:
        return to_int_money(m.group(1))
    return None

def extract_ref(soup: BeautifulSoup, text: str, url: str) -> Optional[str]:
    # labelled anywhere
    m = REF_LABELLED_RE.search(text)
    if m: return m.group(1).upper()
    # 'ref' containers
    for sel in ("[class*='ref']", ".property-ref", ".web-ref"):
        n = soup.select_one(sel)
        if n:
            t = norm_space(n.get_text(" ", strip=True))
            m2 = REF_LABELLED_RE.search(t) or REF_RL_RE.search(t) or REF_NUMERIC_RE.search(t)
            if m2: return m2.group(1).upper()
    # rl anywhere
    m = REF_RL_RE.search(text)
    if m: return m.group(1).upper()
    # numeric windows near 'ref'
    for window in re.finditer(r"(?:web\s*ref(?:erence)?|ref(?:erence)?(?:\s*no\.?)?)\s*[:#-]?\s*([A-Za-z]{0,3}\d{5,}|\d{5,})",
                              text, flags=re.I):
        val = window.group(1)
        if val: return val.upper()
    # avoid money-like numerics; fallback to URL id
    monies = {m.group(0) for m in MONEY_RE.finditer(text)}
    for m in REF_NUMERIC_RE.finditer(text):
        seq = m.group(1)
        if not any(seq in mon for mon in monies):
            return seq
    url_id = last_numeric_segment_from_url(url)
    return url_id

def extract_beds(soup: BeautifulSoup, text: str, ptype: str) -> Optional[int]:
    """
    Priority:
      1) JSON-LD numberOfBedrooms / numberOfRooms
      2) Labelled rows: 'Bedrooms: 3'
      3) Bed icon/blocks: class contains 'bed'
      4) Title phrase: '3 Bedroom' (last resort)
    For non-bedroom types → return 0 if no value found.
    """
    # 1) JSON-LD
    for data in iter_jsonld(soup):
        for node in flatten_json(data):
            if not isinstance(node, dict):
                continue
            for key in ("numberOfBedrooms", "numberOfRooms"):
                val = node.get(key)
                if isinstance(val, (int, float)) and int(val) >= 0:
                    return int(val)
                if isinstance(val, str) and re.search(r"\d+", val):
                    return to_int(val)

    # 2) Labelled row
    m = BEDS_LABEL_RE.search(text)
    if m:
        return int(m.group(2))

    # 3) Icon / class blocks
    for sel in ("[class*='bed']", ".icon-bed", ".beds", ".property-beds"):
        n = soup.select_one(sel)
        if n:
            v = to_int(n.get_text(" ", strip=True))
            if v is not None:
                return v

    # 4) Title phrasing
    m = BEDS_WORD_RE.search(text)
    if m:
        return int(m.group(1))

    # Types without bedrooms => 0
    if ptype in TYPES_NO_BEDS:
        return 0

    # Unknown type: still require bedrooms
    return None

def extract_complete_item_from_detail(html: str, url: str, skip_log: Dict) -> Optional[Dict]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # Verify ownership
    if not verify_agent_ownership(soup, text):
        skip_log["reason"] = "not_owned_by_agent_75570"
        return None

    # Property type & location
    ptype = (property_type_from_url(url) or "").lower()
    area  = parse_location_from_url(url)
    if not area:
        skip_log["reason"] = "missing_area"
        return None

    # Title
    title = None
    for sel in TITLE_SELECTORS:
        n = soup.select_one(sel)
        if n:
            title = n["content"] if (n.name == "meta" and n.get("content")) else n.get_text(" ", strip=True)
            title = norm_space(title)
            if title:
                break
    if not title and soup.title:
        title = norm_space(soup.title.get_text(" ", strip=True))
    if not title:
        skip_log["reason"] = "missing_title"
        return None

    # Price
    price = extract_price(soup, text)
    if price is None:
        skip_log["reason"] = "missing_price"
        return None

    # Beds (with type-aware rule)
    beds = extract_beds(soup, text, ptype)
    if beds is None:
        skip_log["reason"] = "missing_beds_required_for_residential"
        skip_log["property_type"] = ptype or "unknown"
        return None

    # Ref
    ref = extract_ref(soup, text, url)
    if not ref:
        skip_log["reason"] = "missing_ref"
        return None

    return {
        "ref":   ref,
        "title": title,
        "url":   url,
        "price": price,
        "beds":  beds,
        "area":  area,
    }

# -------------------- fetching & collection --------------------

def fetch_html(session: requests.Session, url: str) -> Optional[str]:
    try:
        r = session.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            print(f"[warn] status {r.status_code} → {url}", file=sys.stderr)
            return None
        return r.text
    except Exception as e:
        print(f"[warn] fetch failed: {e} → {url}", file=sys.stderr)
        return None

def collect_from_results(session: requests.Session) -> List[str]:
    url = RESULTS_URL
    pages = 0
    collected: List[str] = []
    print(f"[collect] RESULTS start: {url}", file=sys.stderr)
    while url and pages < MAX_PAGES:
        pages += 1
        html = fetch_html(session, url)
        if not html:
            break
        urls = parse_results_for_detail_urls(html)
        print(f"[collect] results page {pages}: {len(urls)} detail URLs", file=sys.stderr)
        for u in urls:
            if u not in collected:
                collected.append(u)
        nxt = find_next_link(html)
        if nxt:
            url = make_abs(nxt)
            time.sleep(LIST_SLEEP_SEC)
        else:
            url = None
    return collected

def collect_from_profile(session: requests.Session) -> List[str]:
    print(f"[collect] PROFILE: {PROFILE_URL}", file=sys.stderr)
    html = fetch_html(session, PROFILE_URL)
    if not html:
        return []
    urls = parse_profile_for_detail_urls(html)
    print(f"[collect] profile page: {len(urls)} detail URLs", file=sys.stderr)
    return urls

# -------------------- main --------------------

def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    urls_profile = collect_from_profile(session)
    urls_results = collect_from_results(session)

    # Merge (profile first), de-dupe
    merged: List[str] = []
    seen = set()
    for u in urls_profile + urls_results:
        if u not in seen:
            seen.add(u); merged.append(u)

    print(f"[info] total candidate URLs (merged): {len(merged)}", file=sys.stderr)

    items: List[Dict] = []
    skipped: List[Dict] = []

    for i, u in enumerate(merged, 1):
        print(f"[detail] {i}/{len(merged)} GET {u}", file=sys.stderr)
        html = fetch_html(session, u)
        if not html:
            skipped.append({"url": u, "reason": "detail_fetch_failed"})
            time.sleep(DETAIL_SLEEP_SEC)
            continue

        skip_log = {"url": u}
        item = extract_complete_item_from_detail(html, u, skip_log)
        if item:
            items.append(item)
        else:
            skipped.append(skip_log)

        time.sleep(DETAIL_SLEEP_SEC)

    # De-duplicate and sort
    dedup: List[Dict] = []
    seen.clear()
    for it in items:
        if it["url"] not in seen:
            seen.add(it["url"]); dedup.append(it)

    dedup.sort(key=lambda x: ((x.get("price") or 0), x.get("title") or ""), reverse=True)

    # Write outputs
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(dedup, f, ensure_ascii=False, indent=2)

    OUT_DEBUG.parent.mkdir(parents=True, exist_ok=True)
    with OUT_DEBUG.open("w", encoding="utf-8") as f:
        json.dump(skipped, f, ensure_ascii=False, indent=2)

    print(f"[done] wrote {OUT_JSON} with {len(dedup)} items (agent {AGENT_ID} only)", file=sys.stderr)
    print(f"[info] wrote {OUT_DEBUG} with {len(skipped)} skip records", file=sys.stderr)

if __name__ == "__main__":
    main()
