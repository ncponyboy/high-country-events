"""
High Country Events Scraper
Scrapes multiple local event sites and writes high_country_events.json
to the repo root for GitHub Pages / mobile app consumption.

Required environment variables (GitHub Secrets):
  GEEKFLARE_API_KEY  — headless Chrome scraping API
  NPS_API_KEY        — NPS developer API key (free at nps.gov/subjects/developer)
"""

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
GEEKFLARE_API_URL = "https://api.geekflare.com/webscraping"
GEEKFLARE_API_KEY = os.environ.get("GEEKFLARE_API_KEY", "")
NPS_API_KEY = os.environ.get("NPS_API_KEY", "")

# Set output paths relative to script location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "..", "high_country_events.json")
MANUAL_EVENTS_FILE = os.path.join(BASE_DIR, "..", "manual_events.json")


def log_info(msg):    print(f"[INFO]  {msg}")
def log_warn(msg):    print(f"[WARN]  {msg}")
def log_error(msg):   print(f"[ERROR] {msg}")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def create_event_id(title: str, date: str, source: str) -> str:
    return hashlib.md5(f"{title}_{date}_{source}".encode()).hexdigest()[:12]


def parse_date_time(month: str, day: str, year: int = None, time_str: str = "7pm") -> Optional[datetime]:
    try:
        if year is None:
            year = datetime.now().year
        event_date = datetime.strptime(f"{month} {day} {year}", "%B %d %Y")
        if event_date < datetime.now() - timedelta(days=30):
            event_date = datetime.strptime(f"{month} {day} {year + 1}", "%B %d %Y")
        
        hour, minute = 19, 0
        time_lower = time_str.lower().strip()
        if '-' in time_lower:
            time_lower = time_lower.split('-')[0].strip()
        
        time_match = re.search(r'(\d+)(?::(\d+))?\s*(am|pm)?', time_lower)
        if time_match:
            hour = int(time_match.group(1))
            if time_match.group(2):
                minute = int(time_match.group(2))
            am_pm = time_match.group(3)
            if am_pm == 'pm' and hour < 12:
                hour += 12
            elif am_pm == 'am' and hour == 12:
                hour = 0
            elif not am_pm and hour < 12:
                hour += 12
        
        event_date = event_date.replace(hour=hour, minute=minute)
        return event_date
    except Exception as e:
        log_error(f"Error parsing date: {e}")
        return None


def deduplicate_events(events: List[Dict]) -> List[Dict]:
    if not events:
        return []
    unique_events = []
    for event in events:
        is_duplicate = False
        for existing in unique_events:
            if event['date'][:10] != existing['date'][:10]:
                continue
            et = event['title'].lower().strip()
            ext = existing['title'].lower().strip()
            if et == ext or et in ext or ext in et:
                is_duplicate = True
                if len(event['title']) > len(existing['title']):
                    existing['title'] = event['title']
                    existing['description'] = event.get('description', '')
                break
        if not is_duplicate:
            unique_events.append(event)
    return unique_events


async def fetch_with_geekflare(url: str, session: aiohttp.ClientSession) -> Optional[str]:
    if not GEEKFLARE_API_KEY:
        log_warn("GEEKFLARE_API_KEY not set — skipping JS-rendered scrape")
        return None
    payload = {"url": url, "format": "html"}
    headers = {"x-api-key": GEEKFLARE_API_KEY, "Content-Type": "application/json"}
    try:
        async with session.post(
            GEEKFLARE_API_URL, json=payload, headers=headers,
            timeout=aiohttp.ClientTimeout(total=45)
        ) as response:
            if response.status == 429:
                log_error(f"Geekflare Rate Limit (429) for {url}")
                return None
            if response.status not in (200, 201):
                log_error(f"Geekflare returned {response.status} for {url}")
                return None
            
            data = await response.json()
            html = data.get("data", {}).get("content", "") or data.get("content", "")
            return html
    except Exception as e:
        log_error(f"Geekflare fetch error for {url}: {e}")
        return None


def extract_title_from_html(html: str, match_pos: int, default_title: str) -> str:
    if match_pos <= 0:
        return default_title
    before_text = html[max(0, match_pos - 500):match_pos]
    title_patterns = [
        r'<h[1-4][^>]*>([^<]+)</h[1-4]>',
        r'<strong>([^<]+)</strong>',
        r'class="[^"]*title[^"]*"[^>]*>([^<]+)<'
    ]
    for pattern in title_patterns:
        match = re.search(pattern, before_text, re.IGNORECASE)
        if match:
            return clean_text(match.group(1)).title()
    return default_title


# ─────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────

async def scrape_high_country_host(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping High Country Host...")
    events = []
    url = "https://highcountryhost.com/High-Country-Events-Calendar"
    html = await fetch_with_geekflare(url, session)
    if not html: return events
    
    soup = BeautifulSoup(html, 'html.parser')
    page_text = soup.get_text(separator='\n')
    lines = [l.strip() for l in page_text.split('\n') if l.strip()]
    
    date_pattern = re.compile(
        r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
        r'(\d{1,2})?[,:]?\s*'
        r'(\d{1,2}(?::\d{2})?(?:\s*-\s*\d{1,2}(?::\d{2})?)?\s*(?:am|pm)),\s*'
        r'([^,\n]{3,})', re.IGNORECASE
    )

    seen = set()
    for i, line in enumerate(lines):
        match = date_pattern.match(line)
        if match:
            month, day, time_raw, venue = match.groups()
            event_date = parse_date_time(month, day, time_str=time_raw)
            if event_date:
                title = lines[i-1] if i > 0 else "High Country Event"
                events.append({
                    "id": create_event_id(title, event_date.isoformat(), "High Country Host"),
                    "title": clean_text(title).title(),
                    "date": event_date.isoformat(),
                    "location": clean_text(venue),
                    "source": "High Country Host",
                    "url": url,
                    "latitude": 36.2168, "longitude": -81.6746
                })
    return events


async def scrape_alleghany_chamber(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Alleghany Chamber...")
    url = "https://alleghanycountychamber.memberclicks.net/index.php?option=com_mc_events&view=calendar"
    html = await fetch_with_geekflare(url, session)
    if not html: return []
    
    # Simplified parsing for the generic request
    events = []
    soup = BeautifulSoup(html, "html.parser")
    for item in soup.find_all("div", class_=lambda c: c and "event" in c.lower()):
        title_el = item.find(["h2", "h3", "a"])
        if title_el:
            title = title_el.get_text(strip=True)
            events.append({
                "id": create_event_id(title, datetime.now().isoformat(), "Alleghany Chamber"),
                "title": title,
                "date": datetime.now().isoformat(),
                "location": "Sparta, NC",
                "source": "Alleghany Chamber",
                "url": url,
                "latitude": 36.4905, "longitude": -81.1701
            })
    return events


async def main():
    async with aiohttp.ClientSession() as session:
        all_events = []
        
        # 1. High Country Host
        all_events.extend(await scrape_high_country_host(session))
        await asyncio.sleep(2) # Prevent Rate Limiting
        
        # 2. Alleghany Chamber
        all_events.extend(await scrape_alleghany_chamber(session))
        
        # Deduplicate
        final_events = deduplicate_events(all_events)
        
        # Save output
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(final_events, f, indent=2)
            
        log_info(f"Done! Total unique events saved: {len(final_events)}")

if __name__ == "__main__":
    asyncio.run(main())
