
import requests
from bs4 import BeautifulSoup
import time
import re
from urllib.parse import urljoin, urlparse
import pandas as pd

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; QuoteScraper/1.0)"}
REQUEST_DELAY = 0.8
TARGET_QUOTES = 1000
OUTPUT_CSV = "wisdomquotes.csv"

SEEDS = [
    "https://wisdomquotes.com/quotes/",
    "https://wisdomquotes.com/life-quotes/",
    "https://wisdomquotes.com/inspirational-quotes/",
    "https://wisdomquotes.com/happiness-quotes/",
    "https://wisdomquotes.com/love-quotes/",
    "https://wisdomquotes.com/success-quotes/",
    "https://wisdomquotes.com/motivational-quotes/",
]

def fetch_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None

def extract_quotes_from_textpage(html):

    quotes = []
    soup = BeautifulSoup(html, "html.parser")

    for blk in soup.find_all(["blockquote", "p"]):
        text = blk.get_text(" ", strip=True)
        if not text:
            continue
        if len(text) < 40 or len(text.split()) < 5:
            continue
        if blk.find("a"):
            continue

        author = ""
        quote_text = text
        if "—" in text or "–" in text:
            parts = re.split(r'—|–', text)
            if len(parts) >= 2:
                quote_text = parts[0].strip()
                author = parts[-1].strip()

        quotes.append({"quote": quote_text, "author": author})

    return quotes


def crawl_wisdom(seeds, target):
    collected = []
    seen = set()
    to_visit = list(seeds)

    while to_visit and len(collected) < target:
        url = to_visit.pop(0)
        print(f"Fetching: {url}  (collected so far: {len(collected)})")
        html = fetch_html(url)
        time.sleep(REQUEST_DELAY)
        if html is None:
            continue

        quotes_here = extract_quotes_from_textpage(html)
        for item in quotes_here:
            if len(collected) >= target:
                break
            qtxt = item["quote"]
            if qtxt not in seen:
                collected.append({
                    "quote": qtxt,
                    "author": item["author"],
                    "source": url
                })
                seen.add(qtxt)

        soup = BeautifulSoup(html, "html.parser")
        base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(url))
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:") or href.startswith("javascript:"):
                continue
            full = urljoin(base, href)
            if "wisdomquotes.com" in full and full not in to_visit and full not in seen:
                if re.search(r'\.(jpg|png|gif|pdf|zip|mp3|mp4)(\?|$)', full, re.I):
                    continue
                to_visit.append(full)

    return collected

if __name__ == "__main__":
    quotes = crawl_wisdom(SEEDS, TARGET_QUOTES)
    print(f"\nTotal collected: {len(quotes)}")
    df = pd.DataFrame(quotes)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Saved {len(quotes)} quotes to {OUTPUT_CSV}")
