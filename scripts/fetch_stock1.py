#!/usr/bin/env python3
"""
Fetch Huizemark agent stock and write smart-match/data/listings.json

Correct target:
- Full, crawlable stock: https://www.huizemark.com/results/agent/75570/
Wrong target (shows only a couple & may be JS-rendered):
- Agent profile: https://www.huizemark.com/agents/blessing-nsibande/75570/

This script:
- Normalizes any supplied AGENT_URL to the results URL
- Follows pagination via rel="next" or 'Next' links
- Extracts title/url/price/beds/ref/area with multiple heuristics
"""

import os, re, json, sys, time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

AGENT_ID = os.environ.get("HUIZEMARK_AGENT_ID", "75570")
PROFILE_URL = f"https://www.huizemark.com/agents/blessing-nsibande/{AGENT_ID}/"
RESULTS_URL = f"https://www.huizemark.com/results/agent/{AGENT_ID}/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (SmartMatchBot; +https://example.com) PythonRequests",
    "Accept-Language": "en-ZA,en;q=0.9"
}

OUTPATH = Path("smart-match/data/listings.json")
MAX_PAGES = 12
TIMEOUT = 30

def normalize_start_url(url: str) -> str:
    # If someone passes the profile URL, change to the results URL.
    if "/agents/" in url:
        return RESULTS_URL
    return url

def to_number(txt):
    if not txt:
        return None
    n = re.sub(r"[^\d]", "", str(txt))
    return int(n) if n else None

def area_from_url(url):
    try:
        parts = re.split(r"/+", re.sub(r"^https?://[^/]+", "", url).strip("/"))
        if "for-sale" in parts:
            i = parts.index("for-sale")
        elif "to-let" in parts:
            i = parts.index("to-let")
        else:
            return None
        area_slug = None
        if len(parts) > i+2:
            area_slug = parts[i+2]
        elif len(parts) > i+1:
            area_slug = parts[i+1]
        if area_slug:
            return area_slug.replace("-", " ").title()
    except Exception:
        pass
    return None

def extract_from_card(card):
    a = card.find("a", href=True)
    url = None
    if a and re.search(r"/results/residential/", a["href"]):
        url = a["href"]
        if url.startswith("/"):
            url = "https://www.huizemark.com" + url

    # Title
    title = None
    if a and a.get("title"):
        title = a["title"].strip()
    if not title and a and a.get_text(strip=True):
        title = a.get_text(" ", strip=True)

    block_text = card.get_text(" ", strip=True)

    # Price
    price = None
    m_price = re.search(r"(R\s*[\d\s,.'’]+)", block_text, flags=re.I)
    if not m_price:
        m_price = re.search(r"Price:\s*(R\s*[\d\s,.'’]+)", block_text, flags=re.I)
    if m_price:
        price = to_number(m_price.group(1))

    # Beds
    beds = None
    m_beds = re.search(r"(\d+)\s*bed(?:room)?s?", block_text, flags=re.I)
    if m_beds:
        beds = int(m_beds.group(1))

    # WebRef
    ref = None
    m_ref = re.search(r"\b(RL\d{3,})\b", block_text, flags=re.I)
    if m_ref:
        ref = m_ref.group(1).upper()
    else:
        m_ref2 = re.search(r"\bWeb\s*Ref[:\s]*([A-Z]{1,3}\d{3,}|\d{5,})\b", block_text, flags=re.I)
        if m_ref2:
            ref = m_ref2.group(1).upper()

    area = area_from_url(url) if url else None

    if url and title:
        return {
            "ref": ref,
            "title": title,
            "url": url,
            "price": price,
            "beds": beds,
            "area": area
        }
    return None

def extract_listings_from_html(html):
    soup = BeautifulSoup(html, "lxml")

    # First try: obvious listing card containers (site-dependent)
    candidates = soup.select(
        ".property, .listing, .card, .result, .property-item, .property__card, .property-card"
    )

    cards = []
    if candidates:
        for c in candidates:
            item = extract_from_card(c)
            if item:
                cards.append(item)

    # Fallback: any anchor pointing to a residential listing, expand context
    if not cards:
        anchors = soup.find_all("a", href=re.compile(r"/results/residential/"))
        for a in anchors:
            parent = a
            for _ in range(2):
                parent = parent.parent or parent
            item = extract_from_card(parent)
            if not item:
                # last resort: parse inner anchor text only
                url = a["href"]
                if url.startswith("/"):
                    url = "https://www.huizemark.com" + url
                title = a.get("title") or a.get_text(" ", strip=True) or "Huizemark Property"
                # window of nearby text
                item = {
                    "ref": None,
                    "title": title,
                    "url": url,
                    "price": None,
                    "beds": None,
                    "area": area_from_url(url)
                }
            cards.append(item)

    # De-dupe by URL
    seen = set()
    uniq = []
    for it in cards:
        if it and it.get("url") and it["url"] not in seen:
            seen.add(it["url"])
            uniq.append(it)
    return uniq

def find_next_link(html):
    # rel="next"
    m = re.search(r'<a[^>]+rel="next"[^>]*href="([^"]+)"', html, flags=re.I)
    if m:
        return m.group(1)
    # text: Next / » / Next Page
    m2 = re.search(r'<a[^>]*href="([^"]+)"[^>]*>\s*(Next|»|Next\s*Page)\s*<', html, flags=re.I)
    if m2:
        return m2.group(1)
    return None

def main():
    start_url = normalize_start_url(RESULTS_URL)  # make sure we use RESULTS
    session = requests.Session()
    session.headers.update(HEADERS)

    url = start_url
    all_items = []
    pages = 0

    while url and pages < MAX_PAGES:
        pages += 1
        print(f"[fetch] {url}", file=sys.stderr)
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        html = resp.text

        items = extract_listings_from_html(html)
        print(f"[page {pages}] found {len(items)} items", file=sys.stderr)
        all_items.extend(items)

        nxt = find_next_link(html)
        if nxt:
            if nxt.startswith("/"):
                url = "https://www.huizemark.com" + nxt
            elif nxt.startswith("http"):
                url = nxt
            else:
                url = "https://www.huizemark.com" + ("/" + nxt.lstrip("/"))
            time.sleep(1.0)  # be polite
        else:
            url = None

    # De-dupe
    dedup = []
    seen = set()
    for it in all_items:
        u = it.get("url")
        if u and u not in seen:
            seen.add(u)
            dedup.append(it)

    # Sort by price desc if available
    dedup.sort(key=lambda x: (x.get("price") or 0), reverse=True)

    OUTPATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPATH.open("w", encoding="utf-8") as f:
        json.dump(dedup, f, ensure_ascii=False, indent=2)

    print(f"[done] wrote {OUTPATH} with {len(dedup)} items", file=sys.stderr)

if __name__ == "__main__":
    main()
