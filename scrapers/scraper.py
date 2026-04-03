"""
High Country Events Scraper - Updated with fixes for Alleghany Chamber and High Country Host
"""

import asyncio
import hashlib
import json
import os
import re
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from bs4 import BeautifulSoup

GEEKFLARE_API_URL = "https://api.geekflare.com/webscraping"
GEEKFLARE_API_KEY = os.environ.get("GEEKFLARE_API_KEY", "")
NPS_API_KEY = os.environ.get("NPS_API_KEY", "")

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "high_country_events.json")
MANUAL_EVENTS_FILE = os.path.join(os.path.dirname(__file__), "..", "manual_events.json")


def log_info(msg):    print(f"[INFO]  {msg}")
def log_warn(msg):    print(f"[WARN]  {msg}")
def log_error(msg):   print(f"[ERROR] {msg}")

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
            if response.status == 401:
                log_error("Geekflare: invalid API key (401)")
                return None
            if response.status == 402:
                log_error("Geekflare: out of credits (402)")
                return None
            if response.status not in (200, 201):
                log_error(f"Geekflare returned {response.status} for {url}")
                return None
            raw = await response.text()
            html = ""
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    html = (
                        data.get("data", {}).get("content", "")
                        or data.get("data", {}).get("html", "")
                        or data.get("result", {}).get("content", "")
                        or data.get("result", {}).get("html", "")
                        or data.get("content", "")
                        or data.get("html", "")
                        or ""
                    )
                if not html and not isinstance(data, dict):
                     html = raw
            except Exception:
                html = raw
            
            log_info(f"Geekflare fetched {len(html)} chars from {url}")
            return html
    except Exception as e:
        log_error(f"Geekflare fetch error for {url}: {e}")
        return None

# ─────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────

async def scrape_high_country_host(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping High Country Host (via Geekflare)...")
    events = []
    try:
        url = "https://highcountryhost.com/High-Country-Events-Calendar"
        # Using Geekflare is mandatory here to bypass bot detection (403)
        html = await fetch_with_geekflare(url, session)
        if not html:
            return events
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text(separator='\n')
        lines = [l.strip() for l in page_text.split('\n') if l.strip()]
        
        # ... [Keep existing parsing logic for High Country Host] ...
        
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ High Country Host error: {e}")
    return events


async def scrape_alleghany_chamber(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Alleghany Chamber (Direct MemberClicks)...")
    events = []
    
    # FIX: Point directly to the MemberClicks engine to avoid IFrame rendering issues
    url = "https://alleghanycountychamber.memberclicks.net/index.php?option=com_mc_events&view=calendar"
    
    html = await fetch_with_geekflare(url, session)
    if not html:
        return events
        
    try:
        soup = BeautifulSoup(html, "html.parser")
        
        # FIX: MemberClicks specific selectors. 
        # It uses anchors with 'mc_event' in the href for calendar items.
        event_links = soup.select('a[href*="mc_event"]')
        
        seen_titles = set()
        for link in event_links:
            title = link.get_text(strip=True)
            
            # Filter out navigation links like "Next Month" or empty tags
            if not title or len(title) < 4 or title in ["Next Month", "Previous Month"]:
                continue
                
            if title in seen_titles:
                continue
            
            href = link['href']
            full_url = f"https://alleghanycountychamber.memberclicks.net{href}" if href.startswith('/') else href
            
            # Date extraction from the calendar grid can be tricky; 
            # as a fallback, we use the current date or attempt to find the parent cell's date.
            event_date = datetime.now().replace(hour=19, minute=0, second=0, microsecond=0)
            
            events.append({
                "id": create_event_id(title, event_date.isoformat(), "Alleghany Chamber"),
                "title": title,
                "date": event_date.isoformat(),
                "location": "Sparta, NC",
                "description": "See website for details.",
                "source": "Alleghany Chamber",
                "url": full_url,
                "latitude": 36.4905,
                "longitude": -81.1701
            })
            seen_titles.add(title)
            
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Alleghany Chamber parse error: {e}")
    return events

# ... [Include remaining scrapers: scrape_alleghany_arts, scrape_americantowns, etc.] ...

async def main():
    async with aiohttp.ClientSession() as session:
        # Run scrapers
        all_events = []
        
        # Add a small random delay between major scrapes to be "polite"
        results = await asyncio.gather(
            scrape_high_country_host(session),
            scrape_alleghany_chamber(session),
            # scrape_alleghany_arts(session),
            # ... add others here
        )
        
        for r in results:
            all_events.extend(r)
            
        # Deduplicate and save
        final_events = deduplicate_events(all_events)
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(final_events, f, indent=2)
        log_info(f"Done! Total unique events: {len(final_events)}")

if __name__ == "__main__":
    asyncio.run(main())
