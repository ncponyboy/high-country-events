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
import sys
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
EVENTBRITE_API_KEY = os.environ.get("EVENTBRITE_API_KEY", "")

# ─────────────────────────────────────────────
# Fetch failure tracking
# ─────────────────────────────────────────────
_current_source: Optional[str] = None
_fetch_failures: Dict[str, str] = {}


def _record_fetch_failure(reason: str) -> None:
    if _current_source and _current_source not in _fetch_failures:
        _fetch_failures[_current_source] = reason


def _clear_fetch_failure() -> None:
    """Call this after a successful Geekflare fallback to cancel a prior fetch_url failure."""
    if _current_source and _current_source in _fetch_failures:
        del _fetch_failures[_current_source]


OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "high_country_events.json")

MANUAL_EVENTS_FILE = os.path.join(os.path.dirname(__file__), "..", "manual_events.json")


def log_info(msg):    print(f"[INFO]  {msg}")
def log_warn(msg):    print(f"[WARN]  {msg}")
def log_error(msg):   print(f"[ERROR] {msg}")


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def clean_text(text: str) -> str:
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
        if event_date > datetime.now() + timedelta(days=365):
            return None
        return event_date
    except Exception as e:
        log_error(f"Error parsing date '{month} {day} {time_str}': {e}")
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


async def fetch_url(url: str, session: aiohttp.ClientSession, extra_headers: dict = None) -> Optional[str]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    if extra_headers:
        headers.update(extra_headers)
    for attempt in range(2):
        try:
            response = await session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20))
            if response.status == 200:
                text = await response.text()
                await response.release()
                return text
            log_warn(f"Got status {response.status} for {url}")
            await response.release()
        except Exception as e:
            if attempt == 1:
                log_error(f"Error fetching {url}: {e}")
            else:
                log_info(f"Retry for {url}")
    _record_fetch_failure(f"could not fetch {url}")
    return None


def extract_title_from_html(html: str, match_pos: int, default_title: str) -> str:
    if match_pos <= 0:
        return default_title
    before_text = html[max(0, match_pos - 500):match_pos]
    title_patterns = [
        r'<h1[^>]*>([^<]+)</h1>\s*$',
        r'<h2[^>]*>([^<]+)</h2>\s*$',
        r'<h3[^>]*>([^<]+)</h3>\s*$',
        r'<h4[^>]*>([^<]+)</h4>\s*$',
        r'<strong>([^<]+)</strong>\s*$',
        r'class="[^"]*title[^"]*"[^>]*>([^<]+)<',
        r'([A-Z][A-Z\s\-\'&]{10,})\s*$',
    ]
    for pattern in title_patterns:
        match = re.search(pattern, before_text, re.IGNORECASE)
        if match:
            potential_title = clean_text(match.group(1))
            if len(potential_title.split()) >= 2 and not potential_title.isdigit():
                return potential_title.title()
    return default_title


# ─────────────────────────────────────────────
# Geekflare Web Scraping API helper
# ─────────────────────────────────────────────
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
                    if not html:
                        log_error(f"Geekflare JSON but no html field. Keys: {list(data.keys())}")
                        return None
                else:
                    log_error(f"Geekflare JSON but not a dict: {raw[:200]}")
                    return None
            except Exception:
                if raw.strip().startswith("<") or "<html" in raw.lower():
                    html = raw
                else:
                    log_error(f"Geekflare response not JSON or HTML: {raw[:300]}")
                    return None
            log_info(f"Geekflare fetched {len(html)} chars from {url}")
            return html
    except Exception as e:
        log_error(f"Geekflare fetch error for {url}: {e}")
        return None


# ─────────────────────────────────────────────
# iCal Parser Helper
# ─────────────────────────────────────────────
def parse_ical_feed(raw_ical: str, source_name: str, default_location: str,
                    default_lat: float, default_lon: float, source_url: str) -> List[Dict]:
    events = []
    seen = set()
    cutoff = datetime.now() - timedelta(hours=2)
    unfolded = re.sub(r'\r?\n[ \t]', '', raw_ical)
    vevent_re = re.compile(r'BEGIN:VEVENT(.*?)END:VEVENT', re.DOTALL)
    field_re = re.compile(r'^([A-Z][A-Z0-9-]*(?:;[^:]+)?):(.*)', re.MULTILINE)
    for vevent_match in vevent_re.finditer(unfolded):
        block = vevent_match.group(1)
        fields = {}
        for fm in field_re.finditer(block):
            key_full = fm.group(1)
            val = fm.group(2).strip()
            key = key_full.split(';')[0].upper()
            if key not in fields:
                fields[key] = val
        summary = fields.get('SUMMARY', '').replace('\\n', ' ').replace('\\,', ',').replace('\\;', ';').strip()
        dtstart = fields.get('DTSTART', '').strip()
        location = fields.get('LOCATION', default_location).replace('\\n', ', ').replace('\\,', ',').strip() or default_location
        description = fields.get('DESCRIPTION', '').replace('\\n', ' ').replace('\\,', ',').replace('\\;', ';').strip()
        url_field = fields.get('URL', source_url).strip()
        geo = fields.get('GEO', '')
        if not summary or not dtstart:
            continue
        dtstart_clean = dtstart.upper().rstrip('Z')
        try:
            if 'T' in dtstart_clean:
                dt_part, t_part = dtstart_clean.split('T', 1)
                t_part = t_part[:6].ljust(6, '0')
                event_date = datetime.strptime(dt_part + t_part, '%Y%m%d%H%M%S')
            else:
                event_date = datetime.strptime(dtstart_clean[:8], '%Y%m%d')
                event_date = event_date.replace(hour=19, minute=0)
        except Exception:
            continue
        if event_date < cutoff:
            continue
        title = clean_text(summary)
        if not title:
            continue
        key = f"{title.lower()}_{event_date.date()}"
        if key in seen:
            continue
        seen.add(key)
        lat, lon = default_lat, default_lon
        if geo:
            geo_parts = geo.split(';')
            if len(geo_parts) == 2:
                try:
                    lat = float(geo_parts[0])
                    lon = float(geo_parts[1])
                except Exception:
                    pass
        events.append({
            "id": create_event_id(title, event_date.isoformat(), source_name),
            "title": title,
            "date": event_date.isoformat(),
            "location": clean_text(location)[:100],
            "description": clean_text(description)[:200] if description else '',
            "source": source_name,
            "url": url_field or source_url,
            "latitude": lat,
            "longitude": lon
        })
    return events


# ─────────────────────────────────────────────
# Scrapers
# ─────────────────────────────────────────────
async def scrape_high_country_host(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping High Country Host...")
    events = []
    try:
        url = "https://highcountryhost.com/High-Country-Events-Calendar"
        html = await fetch_with_geekflare(url, session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text(separator='\n')
        lines = [l.strip() for l in page_text.split('\n') if l.strip()]
        date_pattern = re.compile(
            r'^(January|February|March|April|May|June|July|August|September|October|November|December|Daily)\s+'
            r'(\d{1,2})?[,:]?\s*'
            r'(\d{1,2}(?::\d{2})?(?:\s*-\s*\d{1,2}(?::\d{2})?)?\s*(?:am|pm)),\s*'
            r'([^,\n]{3,})',
            re.IGNORECASE
        )
        skip_lines = {
            'banner elk', 'beech mountain', 'blowing rock', 'boone', 'sparta',
            'west jefferson', 'wilkesboro', 'blue ridge parkway', 'lodging',
            'shopping', 'mountain towns', 'deals', 'things to do', 'dining',
            'high country events calendar', 'this week',
        }
        seen = set()
        i = 0
        while i < len(lines):
            line = lines[i]
            date_match = date_pattern.match(line)
            if not date_match:
                i += 1
                continue
            month_or_daily = date_match.group(1)
            day = date_match.group(2)
            time_raw = date_match.group(3).strip()
            venue = clean_text(date_match.group(4))
            time_match = re.search(r'(\d{1,2})(?::\d{2})?\s*(am|pm)', time_raw, re.IGNORECASE)
            time_str = f"{time_match.group(1)}{time_match.group(2)}" if time_match else "7pm"
            if month_or_daily.lower() == 'daily' or not day:
                i += 1
                continue
            event_date = parse_date_time(month_or_daily, day, time_str=time_str)
            if not event_date or event_date < datetime.now() - timedelta(hours=2):
                i += 1
                continue
            title = "High Country Event"
            for back in range(1, min(4, i + 1)):
                candidate = lines[i - back]
                if candidate.lower() in skip_lines or len(candidate) < 3 or len(candidate) > 80:
                    continue
                if date_pattern.match(candidate):
                    continue
                if candidate.isupper() or candidate.istitle() or re.match(r'[A-Z][A-Za-z\s\-&]{2,}', candidate):
                    title = candidate.title()
                    break
            key = f"{month_or_daily}{day}{title[:10]}"
            if key in seen:
                i += 1
                continue
            seen.add(key)
            nc_towns = ['NC', 'Boone', 'Blowing Rock', 'Banner Elk', 'West Jefferson',
                        'Sparta', 'Wilkesboro', 'Deep Gap', 'Beech Mountain', 'Valle Crucis']
            if not any(t in venue for t in nc_towns):
                venue += ', NC'
            events.append({
                "id": create_event_id(title, event_date.isoformat(), "High Country Host"),
                "title": title,
                "date": event_date.isoformat(),
                "location": venue,
                "description": "",
                "source": "High Country Host",
                "url": url,
                "latitude": 36.2168,
                "longitude": -81.6746
            })
            i += 1
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ High Country Host error: {e}")
    return events


async def scrape_alleghany_chamber(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Alleghany Chamber (via Geekflare)...")
    events = []
    url = "https://www.alleghanycountychamber.com/events/communityeventscalendar"
    html = await fetch_with_geekflare(url, session)
    if not html:
        return events
    try:
        soup = BeautifulSoup(html, "html.parser")
        event_items = (
            soup.find_all("div", class_=lambda c: c and "event" in c.lower())
            or soup.find_all("li", class_=lambda c: c and "event" in c.lower())
            or soup.find_all("article", class_=lambda c: c and "event" in c.lower())
        )
        if event_items:
            for item in event_items:
                try:
                    title_el = (
                        item.find("h2") or item.find("h3") or item.find("h4")
                        or item.find(class_=lambda c: c and "title" in c.lower())
                    )
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    if not title:
                        continue
                    date_el = item.find("time") or item.find(class_=lambda c: c and "date" in c.lower())
                    date_str = ""
                    if date_el:
                        date_str = date_el.get("datetime", "") or date_el.get_text(strip=True)
                    event_date = None
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"]:
                        try:
                            event_date = datetime.strptime(date_str.strip(), fmt)
                            break
                        except ValueError:
                            continue
                    if not event_date:
                        dm = re.search(
                            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})',
                            item.get_text(), re.IGNORECASE
                        )
                        if dm:
                            event_date = parse_date_time(dm.group(1), dm.group(2))
                    if not event_date or event_date < datetime.now() - timedelta(hours=2):
                        continue
                    link_el = item.find("a", href=True)
                    link = link_el["href"] if link_el else url
                    if link and not link.startswith("http"):
                        link = f"https://www.alleghanycountychamber.com{link}"
                    loc_el = item.find(class_=lambda c: c and "location" in c.lower())
                    location = loc_el.get_text(strip=True) if loc_el else "Sparta, NC"
                    desc_el = item.find("p")
                    description = desc_el.get_text(strip=True) if desc_el else ""
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), "Alleghany Chamber"),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": location or "Sparta, NC",
                        "description": description,
                        "source": "Alleghany Chamber",
                        "url": link,
                        "latitude": 36.4905,
                        "longitude": -81.1701
                    })
                except Exception as e:
                    log_error(f"  ✗ Alleghany Chamber item parse error: {e}")
                    continue
        else:
            seen = set()
            for month, day in re.findall(
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})',
                html, re.IGNORECASE
            )[:15]:
                key = f"{month}{day}"
                if key in seen:
                    continue
                seen.add(key)
                event_date = parse_date_time(month, day)
                if event_date and event_date >= datetime.now() - timedelta(hours=2):
                    title = extract_title_from_html(html, html.find(f"{month} {day}"), "Community Event")
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), "Alleghany Chamber"),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": "Sparta, NC",
                        "description": "",
                        "source": "Alleghany Chamber",
                        "url": url,
                        "latitude": 36.4905,
                        "longitude": -81.1701
                    })
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Alleghany Chamber parse error: {e}")
    return events


async def scrape_alleghany_arts(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Alleghany Arts Council (via Geekflare)...")
    events = []
    url = "https://www.alleghanyartscouncil.org/calendar/"
    html = await fetch_with_geekflare(url, session)
    if not html:
        return events
    try:
        soup = BeautifulSoup(html, "html.parser")
        if 'BEGIN:VCALENDAR' in html:
            events = parse_ical_feed(html, "Alleghany Arts Council", "Sparta, NC", 36.4905, -81.1701, url)
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        event_items = (
            soup.find_all("article", class_=lambda c: c and "tribe_events" in c.lower())
            or soup.find_all("article", class_=lambda c: c and "event" in c.lower())
            or soup.find_all("div", class_=lambda c: c and "tribe-event" in c.lower())
            or soup.find_all("li", class_=lambda c: c and "tribe-event" in c.lower())
            or soup.find_all("div", class_=lambda c: c and "event" in c.lower())
        )
        if event_items:
            for item in event_items:
                try:
                    title_el = (
                        item.find(class_=lambda c: c and "tribe-event-url" in c.lower())
                        or item.find("h2") or item.find("h3") or item.find("h4")
                        or item.find(class_=lambda c: c and "title" in c.lower())
                    )
                    if not title_el:
                        continue
                    title = title_el.get_text(strip=True)
                    if not title:
                        continue
                    date_el = (
                        item.find("abbr", class_=lambda c: c and "tribe" in c.lower())
                        or item.find("time")
                        or item.find(class_=lambda c: c and "date" in c.lower())
                    )
                    date_str = ""
                    if date_el:
                        date_str = (date_el.get("title", "") or date_el.get("datetime", "") or date_el.get_text(strip=True))
                    event_date = None
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%B %d, %Y %I:%M %p", "%B %d, %Y", "%A, %B %d, %Y"]:
                        try:
                            event_date = datetime.strptime(date_str.strip(), fmt)
                            break
                        except ValueError:
                            continue
                    if not event_date:
                        dm = re.search(
                            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})',
                            item.get_text(), re.IGNORECASE
                        )
                        if dm:
                            event_date = parse_date_time(dm.group(1), dm.group(2))
                    if not event_date or event_date < datetime.now() - timedelta(hours=2):
                        continue
                    link_el = item.find("a", href=True)
                    link = link_el["href"] if link_el else url
                    if link and not link.startswith("http"):
                        link = f"https://www.alleghanyartscouncil.org{link}"
                    loc_el = item.find(class_=lambda c: c and ("venue" in c.lower() or "location" in c.lower()))
                    location = loc_el.get_text(strip=True) if loc_el else "Sparta, NC"
                    desc_el = item.find(class_=lambda c: c and "description" in c.lower()) or item.find("p")
                    description = desc_el.get_text(strip=True) if desc_el else ""
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), "Alleghany Arts Council"),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": location or "Sparta, NC",
                        "description": description,
                        "source": "Alleghany Arts Council",
                        "url": link,
                        "latitude": 36.4905,
                        "longitude": -81.1701
                    })
                except Exception as e:
                    log_error(f"  ✗ Alleghany Arts item parse error: {e}")
                    continue
        else:
            seen = set()
            for month, day in re.findall(
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})',
                html, re.IGNORECASE
            )[:15]:
                key = f"{month}{day}"
                if key in seen:
                    continue
                seen.add(key)
                event_date = parse_date_time(month, day)
                if event_date and event_date >= datetime.now() - timedelta(hours=2):
                    title = extract_title_from_html(html, html.find(f"{month} {day}"), "Arts & Culture Event")
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), "Alleghany Arts Council"),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": "Sparta, NC",
                        "description": "",
                        "source": "Alleghany Arts Council",
                        "url": url,
                        "latitude": 36.4905,
                        "longitude": -81.1701
                    })
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Alleghany Arts parse error: {e}")
    return events




async def scrape_ashe_chamber(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Ashe County Events...")
    events = []
    date_re_full = re.compile(
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
        r'(\d{1,2}),?\s*(\d{4})?', re.IGNORECASE
    )
    date_re_short = re.compile(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+(\d{1,2})', re.IGNORECASE)
    month_abbr = {
        'jan': 'January', 'feb': 'February', 'mar': 'March', 'apr': 'April',
        'may': 'May', 'jun': 'June', 'jul': 'July', 'aug': 'August',
        'sep': 'September', 'oct': 'October', 'nov': 'November', 'dec': 'December'
    }
    sources = [
        ("https://www.ashecountyarts.org/events/", "Ashe County Arts", 36.4332, -81.4990, "Ashe County, NC"),
        ("https://ashechamber.com/calendar.php?caltype=2", "Ashe Chamber", 36.4332, -81.4990, "Ashe County, NC"),
    ]
    for url, label, lat, lng, location in sources:
        html = await fetch_url(url, session)
        if not html or len(html) < 500:
            continue
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        found = []
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            link = heading.find('a')
            title_text = clean_text(link.get_text() if link else heading.get_text())
            if not title_text or len(title_text) < 4:
                continue
            event_url = link.get('href', '') if link else ''
            if event_url and not event_url.startswith('http'):
                event_url = f"https://www.ashecountyarts.org{event_url}"
            container = heading.parent
            search_text = container.get_text() if container else ''
            dm = date_re_full.search(search_text)
            if dm:
                month = dm.group(1).title()
                day = dm.group(2)
                year = int(dm.group(3)) if dm.group(3) else datetime.now().year
            else:
                dm2 = date_re_short.search(search_text)
                if not dm2:
                    continue
                month = month_abbr.get(dm2.group(1).lower()[:3], dm2.group(1).title())
                day = dm2.group(2)
                year = datetime.now().year
            event_date = parse_date_time(month, day, year=year, time_str="10am")
            if not event_date or event_date < datetime.now() - timedelta(hours=2):
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            found.append({
                "id": create_event_id(title_text, event_date.isoformat(), label),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": location,
                "description": "",
                "source": label,
                "url": event_url or url,
                "latitude": lat,
                "longitude": lng
            })
        if found:
            events.extend(found)
            log_info(f"  ✓ Found {len(found)} events from {label}")
            break
    if not events:
        log_warn("  ⚠ Ashe Chamber: all sources returned no events")
    return events


async def scrape_stay_blue_ridge(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Stay Blue Ridge...")
    events = []
    base_url = "https://www.stayblueridge.com"
    page_url = f"{base_url}/events-list"
    cutoff = datetime.now() - timedelta(hours=2)
    month_map = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12
    }
    try:
        html = await fetch_with_geekflare(page_url, session)
        if not html:
            return events
        soup = BeautifulSoup(html, "html.parser")
        # Events are in a <ul> after the "Local Mtn Events" heading
        heading = soup.find(lambda t: t.name in ["h1","h2","h3","h4"] and "event" in t.get_text(separator=" ").lower())
        event_list = heading.find_next("ul") if heading else soup.find("ul")
        if not event_list:
            log_warn("  ⚠ Stay Blue Ridge: could not find event list")
            return events
        for li in event_list.find_all("li", recursive=False):
            try:
                # Title from heading
                title_el = li.find(["h2", "h3", "h4"])
                if not title_el:
                    continue
                title = clean_text(title_el.get_text())
                if not title:
                    continue
                # Event URL
                link_el = li.find("a", href=True)
                event_url = base_url + link_el["href"] if link_el and link_el["href"].startswith("/") else (link_el["href"] if link_el else page_url)
                # Date: extract month abbreviations and day numbers from the date badge link
                date_link = li.find("a", href=lambda h: h and "/event/" in h)
                date_text = date_link.get_text(separator=" ").strip() if date_link else ""
                # Parse "MMM DD" or "MMM DD MMM DD" (date range)
                date_parts = re.findall(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{1,2})', date_text, re.IGNORECASE)
                if not date_parts:
                    continue
                mon_str, day_str = date_parts[0]
                mon_num = month_map.get(mon_str[:3].capitalize(), 0)
                if not mon_num:
                    continue
                year = datetime.now().year
                try:
                    event_date = datetime(year, mon_num, int(day_str), 19, 0)
                    if event_date < cutoff:
                        event_date = datetime(year + 1, mon_num, int(day_str), 19, 0)
                except ValueError:
                    continue
                # Location and time from the text block after title
                loc_time_el = title_el.find_next_sibling()
                loc_time = clean_text(loc_time_el.get_text()) if loc_time_el else ""
                # Parse start time if present
                time_m = re.search(r'Starts:\s*(\d{1,2}:\d{2}\s*[APap][Mm])', loc_time)
                if time_m:
                    try:
                        t = datetime.strptime(time_m.group(1).strip(), "%I:%M %p")
                        event_date = event_date.replace(hour=t.hour, minute=t.minute)
                    except Exception:
                        pass
                # Clean location (strip "| Starts:..." part)
                location = loc_time.split("|")[0].strip() or "West Jefferson, NC"
                # Description
                desc_parts = []
                for sibling in title_el.find_next_siblings():
                    text = clean_text(sibling.get_text())
                    if text and text != loc_time and "More Information" not in text:
                        desc_parts.append(text)
                description = " ".join(desc_parts)[:200]
                events.append({
                    "id": create_event_id(title, event_date.isoformat(), "Stay Blue Ridge"),
                    "title": title,
                    "date": event_date.isoformat(),
                    "location": location,
                    "description": description,
                    "source": "Stay Blue Ridge",
                    "url": event_url,
                    "latitude": 36.4458,
                    "longitude": -81.4264
                })
            except Exception:
                continue
        log_info(f"  ✓ Found {len(events)} Stay Blue Ridge events")
    except Exception as e:
        log_error(f"  ✗ Stay Blue Ridge error: {e}")
    return events


async def scrape_old_barn_winery(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Old Barn Winery...")
    events = []
    try:
        url = "https://oldbarnwinery.com/events-calendar"
        html = await fetch_url(url, session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        lines = [l.strip() for l in soup.get_text(separator='\n').split('\n') if l.strip()]
        date_pattern = re.compile(
            r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2})\s+FROM\s+(\d{1,2}(?::\d{2})?)\s*[-–]\s*(\d{1,2}(?::\d{2})?)\s*(AM|PM)',
            re.IGNORECASE
        )
        date_simple = re.compile(
            r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2})', re.IGNORECASE
        )
        seen = set()
        i = 0
        while i < len(lines):
            line = lines[i]
            date_match = date_pattern.search(line)
            simple_match = date_simple.search(line) if not date_match else None
            if date_match:
                month, day = date_match.group(2), date_match.group(3)
                time_str = f"{date_match.group(4)}{date_match.group(6)}"
            elif simple_match:
                month, day, time_str = simple_match.group(2), simple_match.group(3), "2pm"
            else:
                i += 1
                continue
            event_date = parse_date_time(month, day, time_str=time_str)
            key = f"{month}{day}"
            if event_date and key not in seen and event_date >= datetime.now() - timedelta(hours=2):
                seen.add(key)
                title = "Live Music at Old Barn Winery"
                if i + 1 < len(lines):
                    candidate = lines[i + 1]
                    if 3 < len(candidate) < 80 and not date_pattern.search(candidate) and 'FROM' not in candidate.upper():
                        title = candidate.title()
                description = ""
                if i + 2 < len(lines):
                    desc = lines[i + 2]
                    if len(desc) > 30 and not date_pattern.search(desc):
                        description = desc[:200]
                events.append({
                    "id": create_event_id(title, event_date.isoformat(), "Old Barn Winery"),
                    "title": title,
                    "date": event_date.isoformat(),
                    "location": "Old Barn Winery, Sparta NC",
                    "description": description,
                    "source": "Old Barn Winery",
                    "url": url,
                    "latitude": 36.5054,
                    "longitude": -81.1190
                })
            i += 1
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Old Barn Winery error: {e}")
    return events


async def scrape_blue_ridge_music(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Blue Ridge Music NC...")
    events = []
    try:
        url = "https://www.blueridgemusicnc.com/events/"
        html = await fetch_url(url, session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        date_pattern = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2})\s*@\s*(\d{1,2}:\d{2}\s*(?:am|pm))', re.IGNORECASE
        )
        date_pattern_year = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2}),?\s+(\d{4})', re.IGNORECASE
        )
        for h4 in soup.find_all('h4'):
            try:
                link = h4.find('a')
                if not link:
                    continue
                title = clean_text(link.get_text())
                if not title or len(title) < 4:
                    continue
                event_url = link.get('href', url)
                if event_url and not event_url.startswith('http'):
                    event_url = f"https://www.blueridgemusicnc.com{event_url}"
                event_date = None
                container = h4
                for _ in range(6):
                    container = container.parent
                    if container is None:
                        break
                    container_text = container.get_text()
                    dm = date_pattern.search(container_text)
                    if dm:
                        event_date = parse_date_time(dm.group(1), dm.group(2), time_str=dm.group(3))
                        break
                    dm2 = date_pattern_year.search(container_text)
                    if dm2:
                        event_date = parse_date_time(dm2.group(1), dm2.group(2), year=int(dm2.group(3)), time_str="7pm")
                        break
                if not event_date or event_date < datetime.now() - timedelta(hours=2):
                    continue
                key = f"{title.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title, event_date.isoformat(), "Blue Ridge Music NC"),
                    "title": title,
                    "date": event_date.isoformat(),
                    "location": "Blue Ridge, NC",
                    "description": "",
                    "source": "Blue Ridge Music NC",
                    "url": event_url,
                    "latitude": 36.2168,
                    "longitude": -81.6746
                })
            except Exception as e:
                log_error(f"  Blue Ridge Music item error: {e}")
                continue
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Blue Ridge Music error: {e}")
    return events


async def scrape_explore_boone(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping High Country Press...")
    events = []
    try:
        seen = set()
        sources = ["https://www.hcpress.com/events", "https://www.hcpress.com/"]
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2}),?\s*(\d{4})?', re.IGNORECASE
        )
        when_re = re.compile(
            r'When:\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2}),?\s*(\d{4})?', re.IGNORECASE
        )
        skip_titles = {
            'events', 'front page', 'sports', 'crime', 'letters', 'business',
            'obits', 'weather', 'politics', 'app state', 'about', 'read more',
            'obituaries', 'skip to content', 'high country press'
        }
        for url in sources:
            html = await fetch_url(url, session)
            if not html or len(html) < 1000:
                continue
            soup = BeautifulSoup(html, 'html.parser')
            for article in soup.find_all(['article', 'div'], class_=lambda c: c and ('post' in c or 'entry' in c or 'article' in c)):
                heading = article.find(['h2', 'h3', 'h4'])
                if not heading:
                    continue
                link = heading.find('a')
                if not link:
                    continue
                title_text = clean_text(link.get_text())
                if not title_text or len(title_text) < 5 or title_text.lower() in skip_titles:
                    continue
                event_url = link.get('href', url)
                article_text = article.get_text()
                dm = when_re.search(article_text) or date_re.search(article_text)
                if not dm:
                    continue
                month = dm.group(1).title()
                day = dm.group(2)
                year = int(dm.group(3)) if dm.group(3) else datetime.now().year
                event_date = parse_date_time(month, day, year=year, time_str="10am")
                if not event_date or event_date < datetime.now() - timedelta(hours=2):
                    continue
                key = f"{title_text.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), "High Country Press"),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": "Boone, NC",
                    "description": "",
                    "source": "High Country Press",
                    "url": event_url,
                    "latitude": 36.2168,
                    "longitude": -81.6746
                })
            if events:
                log_info(f"  ✓ Found {len(events)} events from {url}")
                break
        if not events:
            log_warn("  ⚠ High Country Press: no events found")
    except Exception as e:
        log_error(f"  ✗ High Country Press error: {e}")
    return events


async def scrape_appalachian_theatre(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Appalachian Theatre...")
    events = []
    source_name = "Appalachian Theatre"
    base_url = "https://www.apptheatre.org"
    try:
        ical_raw = await fetch_url(f"{base_url}/events-and-tickets?format=ical", session)
        if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
            events = parse_ical_feed(
                ical_raw, source_name,
                "Appalachian Theatre, 559 W King St, Boone, NC",
                36.2157, -81.6752, f"{base_url}/events-and-tickets"
            )
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/events-and-tickets", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        date_re = re.compile(
            r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+'
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2}),?\s+(\d{4})', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', re.IGNORECASE)
        for h1 in soup.find_all('h1'):
            link = h1.find('a')
            if not link:
                continue
            title_text = clean_text(link.get_text())
            if not title_text or len(title_text) < 3 or title_text.lower() in ('upcoming events', 'events', 'tickets'):
                continue
            event_slug = link.get('href', '')
            event_url = (base_url + event_slug) if event_slug.startswith('/') else event_slug or f"{base_url}/events-and-tickets"
            block = h1.parent
            dm = None
            for _ in range(3):
                if block and block.parent:
                    block = block.parent
                    dm = date_re.search(block.get_text())
                    if dm:
                        break
            if not dm:
                continue
            month, day, year = dm.group(1).title(), dm.group(2), int(dm.group(3))
            block_text = block.get_text() if block else ''
            tm = time_re.search(block_text)
            event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:00 PM")
            if not event_date or event_date < datetime.now() - timedelta(hours=2):
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": "Appalachian Theatre, 559 W King St, Boone, NC",
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 36.2157,
                "longitude": -81.6752
            })
        log_info(f"  ✓ Found {len(events)} events via HTML")
    except Exception as e:
        log_error(f"  ✗ Appalachian Theatre error: {e}")
    return events


async def scrape_ashe_county_arts(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Ashe County Arts Council...")
    events = []
    source_name = "Ashe County Arts"
    homepage_url = "https://ashecountyarts.org"
    try:
        ical_raw = await fetch_url(f"{homepage_url}/events/?ical=1", session)
        if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
            events = parse_ical_feed(
                ical_raw, source_name,
                "Ashe Arts Center, 303 School Ave, West Jefferson, NC",
                36.4013, -81.4874, f"{homepage_url}/events/"
            )
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{homepage_url}/events/", session) or await fetch_url(homepage_url + "/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2}),?\s+(\d{4})', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
        skip_titles = {'view all events', 'all events', 'upcoming events', 'contact us', 'home'}
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            link = heading.find('a')
            if not link:
                continue
            title_text = clean_text(link.get_text())
            if not title_text or len(title_text) < 4 or title_text.lower() in skip_titles:
                continue
            event_url = link.get('href', f"{homepage_url}/events/")
            if event_url and event_url.startswith('/'):
                event_url = homepage_url + event_url
            container = heading.parent
            container_text = container.get_text() if container else ''
            dm = date_re.search(container_text)
            if not dm and container and container.parent:
                dm = date_re.search(container.parent.get_text())
            if not dm:
                continue
            month, day, year = dm.group(1).title(), dm.group(2), int(dm.group(3))
            tm = time_re.search(container_text)
            event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "7:00 pm")
            if not event_date or event_date < datetime.now() - timedelta(hours=2):
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": "Ashe Arts Center, 303 School Ave, West Jefferson, NC",
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 36.4013,
                "longitude": -81.4874
            })
        log_info(f"  ✓ Found {len(events)} events via HTML")
    except Exception as e:
        log_error(f"  ✗ Ashe County Arts error: {e}")
    return events


async def scrape_downtown_boone(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Downtown Boone Events...")
    events = []
    source_name = "Downtown Boone"
    base_url = "https://downtownboonenc.com"
    try:
        ical_raw = await fetch_url(f"{base_url}/events/?ical=1", session)
        if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
            events = parse_ical_feed(
                ical_raw, source_name, "Downtown Boone, NC",
                36.2168, -81.6746, f"{base_url}/events/"
            )
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/events/list/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2})(?:,?\s+(\d{4}))?', re.IGNORECASE
        )
        at_date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2})\s*@\s*(\d{1,2}:\d{2}\s*(?:am|pm))', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}:\d{2}\s*(?:am|pm))', re.IGNORECASE)
        skip_titles = {'events', 'upcoming events', 'all events', 'list', 'month', 'week', 'day', 'today'}
        articles = soup.find_all(['article', 'li'], class_=lambda c: c and 'tribe' in (' '.join(c) if isinstance(c, list) else c)) or soup.find_all(['h4', 'h3'])
        for element in articles:
            heading = element.find(['h2', 'h3', 'h4']) if element.name in ('article', 'li') else element
            if not heading:
                continue
            link = heading.find('a')
            if not link:
                continue
            title_text = clean_text(link.get_text())
            if not title_text or len(title_text) < 4 or title_text.lower() in skip_titles:
                continue
            event_url = link.get('href', f"{base_url}/events/")
            block_text = element.get_text() if element.name in ('article', 'li') else (heading.parent.get_text() if heading.parent else heading.get_text())
            adm = at_date_re.search(block_text)
            if adm:
                month, day, time_str = adm.group(1).title(), adm.group(2), adm.group(3)
                year = datetime.now().year
                yr_m = re.search(r'\b(202\d)\b', block_text)
                if yr_m:
                    year = int(yr_m.group(1))
            else:
                dm = date_re.search(block_text)
                if not dm:
                    continue
                month, day = dm.group(1).title(), dm.group(2)
                year = int(dm.group(3)) if dm.group(3) else datetime.now().year
                tm = time_re.search(block_text)
                time_str = tm.group(1) if tm else "7:00 pm"
            event_date = parse_date_time(month, day, year=year, time_str=time_str)
            if not event_date or event_date < datetime.now() - timedelta(hours=2):
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": "Downtown Boone, NC",
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 36.2168,
                "longitude": -81.6746
            })
        log_info(f"  ✓ Found {len(events)} events via HTML")
    except Exception as e:
        log_error(f"  ✗ Downtown Boone error: {e}")
    return events


async def scrape_grandfather_mountain(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Grandfather Mountain Events...")
    events = []
    source_name = "Grandfather Mountain"
    base_url = "https://grandfather.com"
    try:
        ical_raw = await fetch_url(f"{base_url}/events/?ical=1", session)
        if ical_raw and 'BEGIN:VCALENDAR' in ical_raw:
            events = parse_ical_feed(
                ical_raw, source_name,
                "Grandfather Mountain, 2050 Blowing Rock Hwy, Linville, NC",
                36.0979, -81.8140, f"{base_url}/event-calendar/"
            )
            if events:
                log_info(f"  ✓ Found {len(events)} events via iCal")
                return events
        html = await fetch_url(f"{base_url}/event-calendar/", session)
        if not html:
            return events
        soup = BeautifulSoup(html, 'html.parser')
        seen = set()
        date_re = re.compile(
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+'
            r'(\d{1,2}),?\s+(\d{4})', re.IGNORECASE
        )
        time_re = re.compile(r'(\d{1,2}(?::\d{2})?\s*(?:am|pm))', re.IGNORECASE)
        skip_titles = {'event calendar', 'upcoming', 'all events', 'daily programs', 'buy tickets'}
        blocks = soup.find_all(['article', 'div', 'li'], class_=lambda c: c and 'tribe' in (' '.join(c) if isinstance(c, list) else c)) or soup.find_all(['h3', 'h4'])
        for element in blocks:
            heading = element.find(['h2', 'h3', 'h4']) if element.name in ('article', 'div', 'li') else element
            if not heading:
                continue
            link = heading.find('a')
            if not link:
                continue
            title_text = clean_text(link.get_text())
            if not title_text or len(title_text) < 4 or title_text.lower() in skip_titles:
                continue
            event_url = link.get('href', f"{base_url}/event-calendar/")
            if event_url.startswith('/'):
                event_url = base_url + event_url
            block_text = element.get_text() if element.name in ('article', 'div', 'li') else (heading.parent.get_text() if heading.parent else heading.get_text())
            dm = date_re.search(block_text)
            if not dm:
                continue
            month, day, year = dm.group(1).title(), dm.group(2), int(dm.group(3))
            tm = time_re.search(block_text)
            event_date = parse_date_time(month, day, year=year, time_str=tm.group(1) if tm else "10:00 am")
            if not event_date or event_date < datetime.now() - timedelta(hours=2):
                continue
            key = f"{title_text.lower()}_{event_date.date()}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "id": create_event_id(title_text, event_date.isoformat(), source_name),
                "title": title_text,
                "date": event_date.isoformat(),
                "location": "Grandfather Mountain, 2050 Blowing Rock Hwy, Linville, NC",
                "description": "",
                "source": source_name,
                "url": event_url,
                "latitude": 36.0979,
                "longitude": -81.8140
            })
        log_info(f"  ✓ Found {len(events)} events via HTML")
    except Exception as e:
        log_error(f"  ✗ Grandfather Mountain error: {e}")
    return events


async def scrape_boonerang(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Generating Boonerang Festival events (hardcoded 2026)...")
    events = []
    source_name = "Boonerang Festival"
    fest_url = "https://www.boonerangfest.com"
    boonerang_events = [
        {
            "title": "Boone's Got Talent — Boonerang 2026",
            "date": datetime(2026, 6, 18, 19, 0),
            "location": "Appalachian Theatre, 559 W King St, Boone, NC",
            "description": "Competition for a spot at Boonerang Music & Arts Festival, with cash prizes including $500 top prize.",
            "lat": 36.2157, "lon": -81.6752,
        },
        {
            "title": "Boonerang Music & Arts Festival — Free Shows & Silent Disco",
            "date": datetime(2026, 6, 19, 18, 0),
            "location": "Downtown Boone, NC",
            "description": "Free live music on multiple stages throughout downtown Boone. Evening silent disco.",
            "lat": 36.2168, "lon": -81.6746,
        },
        {
            "title": "Boonerang Music & Arts Festival — Main Street Festival",
            "date": datetime(2026, 6, 20, 11, 0),
            "location": "Downtown Boone, NC",
            "description": "All-day free street festival with music on multiple stages, local food and beer, vendor market, kids zone.",
            "lat": 36.2168, "lon": -81.6746,
        },
        {
            "title": "Boonerang International Parade of Nations",
            "date": datetime(2026, 6, 21, 12, 0),
            "location": "N Depot St & Queen St, Boone, NC",
            "description": "International Parade of Nations celebrating cultural diversity.",
            "lat": 36.2168, "lon": -81.6746,
        },
    ]
    now = datetime.now()
    for item in boonerang_events:
        if item["date"] < now - timedelta(hours=2):
            continue
        events.append({
            "id": create_event_id(item["title"], item["date"].isoformat(), source_name),
            "title": item["title"],
            "date": item["date"].isoformat(),
            "location": item["location"],
            "description": item["description"],
            "source": source_name,
            "url": fest_url,
            "latitude": item["lat"],
            "longitude": item["lon"]
        })
    log_info(f"  ✓ Generated {len(events)} Boonerang events")
    return events


async def scrape_nps_blueridge(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping NPS Blue Ridge Parkway events...")
    events = []
    source_name = "NPS Blue Ridge Pkwy"
    if not NPS_API_KEY:
        log_warn("  ⚠ NPS_API_KEY not set — skipping")
        return events
    try:
        api_url = f"https://developer.nps.gov/api/v1/events?parkCode=blri&limit=50&api_key={NPS_API_KEY}"
        resp = await session.get(api_url, timeout=aiohttp.ClientTimeout(total=20))
        if resp.status == 401:
            log_error("  ✗ NPS API: invalid key (401)")
            await resp.release()
            return events
        if resp.status != 200:
            log_warn(f"  ⚠ NPS API returned HTTP {resp.status}")
            await resp.release()
            return events
        data = await resp.json()
        await resp.release()
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        for item in data.get("data", []):
            title_text = clean_text(item.get("title", ""))
            if not title_text:
                continue
            dates_list = item.get("dates", [])
            times_list = item.get("times", [])
            if not dates_list:
                continue
            lat = float(item["latitude"]) if item.get("latitude") else 36.1430
            lon = float(item["longitude"]) if item.get("longitude") else -81.8576
            location_str = clean_text(item.get("location", "")) or "Blue Ridge Parkway, NC"
            description_str = clean_text(item.get("description", ""))[:200]
            info_url = item.get("infoURL", "https://www.nps.gov/blri/planyourvisit/events.htm")
            for i_date, date_str in enumerate(dates_list):
                try:
                    yr, mo, dy = date_str.split("-")
                    base_hour, base_min = 10, 0
                    if i_date < len(times_list):
                        ts = times_list[i_date].get("timestart", "10:00 AM")
                        t_m = re.match(r'(\d{1,2}):(\d{2})\s*(AM|PM)', ts.strip(), re.IGNORECASE)
                        if t_m:
                            base_hour = int(t_m.group(1))
                            base_min = int(t_m.group(2))
                            if t_m.group(3).upper() == 'PM' and base_hour != 12:
                                base_hour += 12
                            elif t_m.group(3).upper() == 'AM' and base_hour == 12:
                                base_hour = 0
                    event_date = datetime(int(yr), int(mo), int(dy), base_hour, base_min)
                except Exception:
                    continue
                if event_date < cutoff:
                    continue
                key = f"{title_text.lower()}_{event_date.date()}"
                if key in seen:
                    continue
                seen.add(key)
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), source_name),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": location_str,
                    "description": description_str,
                    "source": source_name,
                    "url": info_url or "https://www.nps.gov/blri/planyourvisit/events.htm",
                    "latitude": lat,
                    "longitude": lon
                })
        log_info(f"  ✓ Found {len(events)} NPS Blue Ridge Parkway events")
    except Exception as e:
        log_error(f"  ✗ NPS error: {e}")
    return events


async def scrape_eventbrite(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Eventbrite events near Boone, NC...")
    events = []
    source_name = "Eventbrite"
    urls = [
        "https://www.eventbrite.com/d/nc--boone/events/",
        "https://www.eventbrite.com/d/nc--west-jefferson/events/",
    ]
    try:
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        for url in urls:
            html = await fetch_url(url, session)
            if not html or len(html) < 1000 or any(x in html for x in ['Verify you are a human', 'cf-challenge', 'Just a moment']):
                html = await fetch_with_geekflare(url, session)
                if html:
                    _clear_fetch_failure()
            if not html or len(html) < 1000:
                continue
            jsonld_matches = re.findall(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                html, re.DOTALL | re.IGNORECASE
            )
            for jsonld_str in jsonld_matches:
                try:
                    data = json.loads(jsonld_str.strip())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict) or 'Event' not in item.get('@type', ''):
                            continue
                        title_text = clean_text(item.get('name', ''))
                        if not title_text or len(title_text) < 4:
                            continue
                        start_date = item.get('startDate', '')
                        if not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                            event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                        except Exception:
                            continue
                        if event_date < cutoff:
                            continue
                        location = "Boone, NC area"
                        loc_obj = item.get('location', {})
                        if isinstance(loc_obj, dict):
                            venue_name = clean_text(loc_obj.get('name', ''))
                            addr_obj = loc_obj.get('address', {})
                            if isinstance(addr_obj, dict):
                                parts = [p for p in [venue_name, addr_obj.get('streetAddress', ''), addr_obj.get('addressLocality', ''), addr_obj.get('addressRegion', '')] if p]
                                if parts:
                                    location = ', '.join(parts)[:100]
                            elif venue_name:
                                location = venue_name
                        key = f"{title_text.lower()}_{event_date.date()}"
                        if key in seen:
                            continue
                        seen.add(key)
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": location,
                            "description": clean_text(item.get('description', ''))[:200],
                            "source": source_name,
                            "url": item.get('url', url),
                            "latitude": 36.2168,
                            "longitude": -81.6746
                        })
                except Exception:
                    continue
            if events:
                log_info(f"  ✓ Found {len(events)} Eventbrite events")
                break
        if not events:
            log_warn("  ⚠ Eventbrite: no events found")
    except Exception as e:
        log_error(f"  ✗ Eventbrite error: {e}")
    return events


# ─────────────────────────────────────────────
# Eventbrite — Official API v3
# ─────────────────────────────────────────────
async def scrape_eventbrite_api(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Eventbrite (API)...")
    events = []
    if not EVENTBRITE_API_KEY:
        log_warn("  ⚠ EVENTBRITE_API_KEY not set — skipping")
        return events

    source_name = "Eventbrite"
    api_base = "https://www.eventbriteapi.com/v3/events/search/"
    headers = {"Authorization": f"Bearer {EVENTBRITE_API_KEY}"}
    cutoff = datetime.now()
    seen = set()

    # Search both Boone and Asheville areas
    searches = [
        {"location.latitude": "36.2168", "location.longitude": "-81.6746", "location.within": "30mi", "lat": 36.2168, "lon": -81.6746},
        {"location.latitude": "35.5951", "location.longitude": "-82.5515", "location.within": "20mi", "lat": 35.5951, "lon": -82.5515},
    ]

    for search in searches:
        params = {
            "location.latitude": search["location.latitude"],
            "location.longitude": search["location.longitude"],
            "location.within": search["location.within"],
            "start_date.range_start": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expand": "venue",
            "page_size": 50,
        }
        try:
            async with session.get(api_base, headers=headers, params=params,
                                   timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 401:
                    log_error("  ✗ Eventbrite API: invalid token (401)")
                    _record_fetch_failure("invalid API token")
                    return events
                if resp.status != 200:
                    log_warn(f"  ⚠ Eventbrite API returned {resp.status}")
                    continue
                data = await resp.json(content_type=None)

            for item in data.get("events", []):
                try:
                    title_text = clean_text(item.get("name", {}).get("text", ""))
                    if not title_text or len(title_text) < 4:
                        continue
                    start_raw = item.get("start", {}).get("local", "")
                    if not start_raw:
                        continue
                    event_date = datetime.strptime(start_raw[:16], "%Y-%m-%dT%H:%M")
                    if event_date < cutoff - timedelta(hours=2):
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)

                    venue = item.get("venue") or {}
                    addr = venue.get("address") or {}
                    venue_name = clean_text(venue.get("name", ""))
                    city = addr.get("city", "")
                    region = addr.get("region", "")
                    loc_parts = [p for p in [venue_name, city, region] if p]
                    location = ", ".join(loc_parts) if loc_parts else search["location.address"]
                    lat = float(venue.get("latitude") or search["lat"])
                    lon = float(venue.get("longitude") or search["lon"])

                    description = clean_text(item.get("description", {}).get("text", ""))[:200]
                    event_url = item.get("url", "")

                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": location,
                        "description": description,
                        "source": source_name,
                        "url": event_url,
                        "latitude": lat,
                        "longitude": lon,
                    })
                except Exception:
                    continue
        except Exception as e:
            log_warn(f"  ⚠ Eventbrite API search error for {search['location.latitude']},{search['location.longitude']}: {e}")

    log_info(f"  ✓ Found {len(events)} Eventbrite events")
    return events


# ─────────────────────────────────────────────
# Downtown Asheville — Squarespace JSON API
# ─────────────────────────────────────────────
async def scrape_downtown_asheville(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Downtown Asheville events...")
    events = []
    source_name = "Downtown Asheville"
    url = "https://www.ashevilledowntown.org/events?format=json"
    AVL_LAT, AVL_LON = 35.5951, -82.5515
    cutoff = datetime.now() - timedelta(hours=2)
    try:
        html = await fetch_url(url, session, extra_headers={"Accept": "application/json"})
        if not html:
            log_warn("  ⚠ Downtown Asheville: no response")
            return events
        data = json.loads(html)
        items = data.get("upcoming", data.get("items", []))
        for item in items:
            try:
                title_text = clean_text(item.get("title", ""))
                if not title_text or len(title_text) < 4:
                    continue
                ts = item.get("startDate")
                if not ts:
                    continue
                event_date = datetime.fromtimestamp(int(ts) / 1000)
                if event_date < cutoff:
                    continue
                loc_obj = item.get("location") or {}
                venue = clean_text(loc_obj.get("addressTitle", ""))
                addr1 = clean_text(loc_obj.get("addressLine1", ""))
                addr2 = clean_text(loc_obj.get("addressLine2", ""))
                lat = loc_obj.get("mapLat") or AVL_LAT
                lon = loc_obj.get("mapLng") or AVL_LON
                if abs(float(lat) - 40.72) < 0.1:
                    lat, lon = AVL_LAT, AVL_LON
                location_parts = [p for p in [venue, addr1, addr2] if p]
                location = ", ".join(location_parts)[:120] if location_parts else "Asheville, NC"
                description = clean_text(item.get("excerpt", ""))[:200]
                event_url = item.get("fullUrl", "")
                if event_url and not event_url.startswith("http"):
                    event_url = "https://www.ashevilledowntown.org" + event_url
                events.append({
                    "id": create_event_id(title_text, event_date.isoformat(), source_name),
                    "title": title_text,
                    "date": event_date.isoformat(),
                    "location": location,
                    "description": description,
                    "source": source_name,
                    "url": event_url,
                    "latitude": float(lat),
                    "longitude": float(lon),
                })
            except Exception:
                continue
        log_info(f"  ✓ Found {len(events)} Downtown Asheville events")
    except Exception as e:
        log_error(f"  ✗ Downtown Asheville error: {e}")
    return events


# ─────────────────────────────────────────────
# Mountain Xpress — Asheville alt-weekly community calendar
# ─────────────────────────────────────────────
async def scrape_mountain_xpress(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Mountain Xpress (Asheville)...")
    events = []
    source_name = "Mountain Xpress"
    AVL_LAT, AVL_LON = 35.5951, -82.5515
    cutoff = datetime.now() - timedelta(hours=2)

    def _parse_jsonld_events(html: str, fallback_url: str) -> List[Dict]:
        found = []
        seen = set()
        jsonld_matches = re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        )
        for jsonld_str in jsonld_matches:
            try:
                data = json.loads(jsonld_str.strip())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict) or 'Event' not in item.get('@type', ''):
                        continue
                    title_text = clean_text(item.get('name', ''))
                    if not title_text or len(title_text) < 4:
                        continue
                    start_date = item.get('startDate', '')
                    if not start_date:
                        continue
                    try:
                        dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                        event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                    except Exception:
                        continue
                    if event_date < cutoff:
                        continue
                    location = "Asheville, NC"
                    lat, lon = AVL_LAT, AVL_LON
                    loc_obj = item.get('location', {})
                    if isinstance(loc_obj, dict):
                        venue_name = clean_text(loc_obj.get('name', ''))
                        addr_obj = loc_obj.get('address', {})
                        if isinstance(addr_obj, dict):
                            parts = [p for p in [venue_name,
                                                  addr_obj.get('streetAddress', ''),
                                                  addr_obj.get('addressLocality', ''),
                                                  addr_obj.get('addressRegion', '')] if p]
                            if parts:
                                location = ', '.join(parts)[:120]
                        elif venue_name:
                            location = venue_name
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    found.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": location,
                        "description": clean_text(item.get('description', ''))[:200],
                        "source": source_name,
                        "url": item.get('url', fallback_url),
                        "latitude": lat,
                        "longitude": lon,
                    })
            except Exception:
                continue
        return found

    try:
        api_url = "https://mountainx.com/wp-json/tribe/events/v1/events"
        api_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        api_ok = False
        try:
            async with session.get(api_url, params={"page": 1, "per_page": 50,
                                                     "start_date": datetime.now().strftime("%Y-%m-%d"),
                                                     "status": "publish"},
                                   headers=api_headers,
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    page_events = data.get("events", [])
                    for item in page_events:
                        title_text = clean_text(item.get("title", ""))
                        start_raw = item.get("start_date", "")
                        if not title_text or not start_raw:
                            continue
                        try:
                            event_date = datetime.strptime(start_raw, "%Y-%m-%d %H:%M:%S")
                        except ValueError:
                            continue
                        if event_date < cutoff:
                            continue
                        venue = item.get("venue", {})
                        if isinstance(venue, dict) and venue.get("city"):
                            city = venue.get("city", "Asheville")
                            state = venue.get("stateprovince", "NC")
                            parts = [p for p in [clean_text(venue.get("venue", "")),
                                                  clean_text(venue.get("address", "")),
                                                  f"{city}, {state}"] if p]
                            location = ", ".join(parts)[:120]
                            lat = float(venue.get("lat") or AVL_LAT)
                            lon = float(venue.get("lng") or AVL_LON)
                        else:
                            location, lat, lon = "Asheville, NC", AVL_LAT, AVL_LON
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": location,
                            "description": clean_text(item.get("description", ""))[:200],
                            "source": source_name,
                            "url": item.get("url", api_url),
                            "latitude": lat,
                            "longitude": lon,
                        })
                    api_ok = bool(events)
        except Exception:
            pass

        if not api_ok:
            log_info("  → REST API blocked, trying Geekflare render...")
            page_url = "https://mountainx.com/events/list/"
            html = await fetch_with_geekflare(page_url, session)
            if html and len(html) > 1000:
                events = _parse_jsonld_events(html, page_url)
            else:
                log_warn("  ⚠ Mountain Xpress: Geekflare unavailable or no content")

        log_info(f"  ✓ Found {len(events)} Mountain Xpress events")
    except Exception as e:
        log_error(f"  ✗ Mountain Xpress error: {e}")
    return events


# ─────────────────────────────────────────────
# Eventbrite Asheville
# ─────────────────────────────────────────────
async def scrape_eventbrite_asheville(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Eventbrite events near Asheville, NC...")
    events = []
    source_name = "Eventbrite Asheville"
    AVL_LAT, AVL_LON = 35.5951, -82.5515
    urls = [
        "https://www.eventbrite.com/d/nc--asheville/events/",
        "https://www.eventbrite.com/d/nc--asheville/all-events/",
    ]
    try:
        cutoff = datetime.now() - timedelta(hours=2)
        seen = set()
        for url in urls:
            html = await fetch_url(url, session)
            if not html or len(html) < 1000 or any(x in html for x in ['Verify you are a human', 'cf-challenge', 'Just a moment']):
                html = await fetch_with_geekflare(url, session)
                if html:
                    _clear_fetch_failure()
            if not html or len(html) < 1000:
                continue
            jsonld_matches = re.findall(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                html, re.DOTALL | re.IGNORECASE
            )
            for jsonld_str in jsonld_matches:
                try:
                    data = json.loads(jsonld_str.strip())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict) or 'Event' not in item.get('@type', ''):
                            continue
                        title_text = clean_text(item.get('name', ''))
                        if not title_text or len(title_text) < 4:
                            continue
                        start_date = item.get('startDate', '')
                        if not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$', '', start_date)
                            event_date = datetime.strptime(dt_clean[:16], '%Y-%m-%dT%H:%M')
                        except Exception:
                            continue
                        if event_date < cutoff:
                            continue
                        location = "Asheville, NC"
                        loc_obj = item.get('location', {})
                        if isinstance(loc_obj, dict):
                            venue_name = clean_text(loc_obj.get('name', ''))
                            addr_obj = loc_obj.get('address', {})
                            if isinstance(addr_obj, dict):
                                parts = [p for p in [venue_name, addr_obj.get('streetAddress', ''), addr_obj.get('addressLocality', ''), addr_obj.get('addressRegion', '')] if p]
                                if parts:
                                    location = ', '.join(parts)[:100]
                            elif venue_name:
                                location = venue_name
                        key = f"{title_text.lower()}_{event_date.date()}"
                        if key in seen:
                            continue
                        seen.add(key)
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": location,
                            "description": clean_text(item.get('description', ''))[:200],
                            "source": source_name,
                            "url": item.get('url', url),
                            "latitude": AVL_LAT,
                            "longitude": AVL_LON,
                        })
                except Exception:
                    continue
            if events:
                log_info(f"  ✓ Found {len(events)} Eventbrite Asheville events")
                break
        if not events:
            log_warn("  ⚠ Eventbrite Asheville: no events found")
    except Exception as e:
        log_error(f"  ✗ Eventbrite Asheville error: {e}")
    return events


# ─────────────────────────────────────────────
# AVL Today (6AM City) — CitySpark calendar API
# ─────────────────────────────────────────────
async def scrape_avl_today(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping AVL Today (6AM City / CitySpark)...")
    events = []
    source_name = "AVL Today"
    AVL_LAT, AVL_LON = 35.5951, -82.5515
    cutoff = datetime.now() - timedelta(hours=2)
    api_url = "https://portal.cityspark.com/api/events/GetEvents/AVLT"
    today = datetime.now().strftime("%Y-%m-%d")
    end_date = (datetime.now() + timedelta(days=60)).strftime("%Y-%m-%d")
    payload = {
        "ppid": 9219, "start": today, "end": end_date,
        "lat": AVL_LAT, "lng": AVL_LON, "distance": 25, "labels": [], "skip": 0,
    }
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://avltoday.6amcity.com",
        "Referer": "https://avltoday.6amcity.com/events/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    try:
        seen = set()
        for page in range(3):
            payload["skip"] = page * 100
            try:
                async with session.post(api_url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        log_warn(f"  ⚠ AVL Today CitySpark API returned {resp.status}")
                        break
                    data = await resp.json(content_type=None)
            except Exception as e:
                log_warn(f"  ⚠ AVL Today request failed: {e}")
                break
            items = data if isinstance(data, list) else data.get("Events", data.get("events", []))
            if not items:
                break
            for item in items:
                try:
                    title_text = clean_text(item.get("Name") or item.get("name", ""))
                    if not title_text or len(title_text) < 4:
                        continue
                    start_raw = item.get("StartLocal") or item.get("startDate") or item.get("Start", "")
                    if not start_raw:
                        continue
                    try:
                        dt_clean = re.sub(r'[+-]\d{2}:\d{2}$|Z$', '', str(start_raw))
                        event_date = datetime.strptime(dt_clean[:16], "%Y-%m-%dT%H:%M")
                    except Exception:
                        continue
                    if event_date < cutoff:
                        continue
                    key = f"{title_text.lower()}_{event_date.date()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    venue = clean_text(item.get("Venue") or item.get("venue", ""))
                    city_state = clean_text(item.get("CityState") or item.get("cityState", "Asheville, NC"))
                    address = clean_text(item.get("Address") or item.get("address", ""))
                    loc_parts = [p for p in [venue, address, city_state] if p]
                    location = ", ".join(loc_parts)[:120] if loc_parts else "Asheville, NC"
                    lat = item.get("latitude") or item.get("Latitude") or AVL_LAT
                    lon = item.get("longitude") or item.get("Longitude") or AVL_LON
                    description = clean_text(item.get("Description") or item.get("description", ""))[:200]
                    event_url = item.get("TicketUrl") or item.get("ticketUrl") or item.get("Url") or item.get("url", "")
                    events.append({
                        "id": create_event_id(title_text, event_date.isoformat(), source_name),
                        "title": title_text,
                        "date": event_date.isoformat(),
                        "location": location,
                        "description": description,
                        "source": source_name,
                        "url": event_url,
                        "latitude": float(lat),
                        "longitude": float(lon),
                    })
                except Exception:
                    continue
            if len(items) < 100:
                break
        log_info(f"  ✓ Found {len(events)} AVL Today events")
    except Exception as e:
        log_error(f"  ✗ AVL Today error: {e}")
    return events


# ─────────────────────────────────────────────
# AllEvents.in Asheville — JSON-LD from paginated HTML
# ─────────────────────────────────────────────
async def scrape_allevents_asheville(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping AllEvents.in (Asheville)...")
    events = []
    source_name = "AllEvents Asheville"
    AVL_LAT, AVL_LON = 35.5951, -82.5515
    cutoff = datetime.now() - timedelta(hours=2)
    base_url = "https://allevents.in/asheville/all"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        seen = set()
        for page in range(1, 6):
            url = base_url if page == 1 else f"{base_url}?page={page}"
            try:
                html = await fetch_url(url, session, extra_headers=headers)
            except Exception as e:
                log_warn(f"  ⚠ AllEvents page {page} fetch error: {e}")
                break
            if not html or len(html) < 500:
                log_warn(f"  ⚠ AllEvents page {page}: empty response")
                break
            if any(x in html for x in ['cf-challenge', 'Just a moment', 'Verify you are human']):
                log_warn(f"  ⚠ AllEvents page {page}: Cloudflare challenge, trying Geekflare...")
                html = await fetch_with_geekflare(url, session)
                if not html or len(html) < 500:
                    break
            jsonld_matches = re.findall(
                r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                html, re.DOTALL | re.IGNORECASE
            )
            page_count = 0
            for jsonld_str in jsonld_matches:
                try:
                    data = json.loads(jsonld_str.strip())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        item_type = item.get("@type", "")
                        if isinstance(item_type, list):
                            if not any("Event" in t for t in item_type):
                                continue
                        elif "Event" not in item_type:
                            continue
                        title_text = clean_text(item.get("name", ""))
                        if not title_text or len(title_text) < 4:
                            continue
                        start_date = item.get("startDate", "")
                        if not start_date:
                            continue
                        try:
                            dt_clean = re.sub(r'[+-]\d{2}:\d{2}$|Z$', '', str(start_date))
                            event_date = datetime.strptime(dt_clean[:16], "%Y-%m-%dT%H:%M")
                        except Exception:
                            try:
                                event_date = datetime.strptime(str(start_date)[:10], "%Y-%m-%d")
                            except Exception:
                                continue
                        if event_date < cutoff:
                            continue
                        key = f"{title_text.lower()}_{event_date.date()}"
                        if key in seen:
                            continue
                        seen.add(key)
                        location = "Asheville, NC"
                        lat, lon = AVL_LAT, AVL_LON
                        loc_obj = item.get("location", {})
                        if isinstance(loc_obj, dict):
                            venue_name = clean_text(loc_obj.get("name", ""))
                            addr_obj = loc_obj.get("address", {})
                            if isinstance(addr_obj, dict):
                                street = clean_text(addr_obj.get("streetAddress", ""))
                                city = clean_text(addr_obj.get("addressLocality", ""))
                                region = clean_text(addr_obj.get("addressRegion", ""))
                                parts = [p for p in [venue_name, street, city, region] if p]
                                if parts:
                                    location = ", ".join(parts)[:120]
                            elif venue_name:
                                location = venue_name
                            geo = loc_obj.get("geo", {})
                            if isinstance(geo, dict):
                                lat = geo.get("latitude", AVL_LAT)
                                lon = geo.get("longitude", AVL_LON)
                        description = clean_text(item.get("description", ""))[:200]
                        event_url = item.get("url", url)
                        events.append({
                            "id": create_event_id(title_text, event_date.isoformat(), source_name),
                            "title": title_text,
                            "date": event_date.isoformat(),
                            "location": location,
                            "description": description,
                            "source": source_name,
                            "url": event_url,
                            "latitude": float(lat),
                            "longitude": float(lon),
                        })
                        page_count += 1
                except Exception:
                    continue
            if page_count == 0:
                log_info(f"  ✓ AllEvents: no more events on page {page}, stopping")
                break
        log_info(f"  ✓ Found {len(events)} AllEvents Asheville events")
    except Exception as e:
        log_error(f"  ✗ AllEvents Asheville error: {e}")
    return events


# ─────────────────────────────────────────────
# Manual Events — reads manual_events.json from repo root
# ─────────────────────────────────────────────
def load_manual_events() -> List[Dict]:
    try:
        with open(MANUAL_EVENTS_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else data.get("events", [])
    except FileNotFoundError:
        return []
    except Exception as e:
        log_error(f"  ✗ Could not load manual_events.json: {e}")
        return []


async def scrape_manual_events(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Loading manual events...")
    events = []
    try:
        raw_events = load_manual_events()
        cutoff = datetime.now() - timedelta(hours=2)
        date_formats = [
            "%Y-%m-%dT%H:%M:%S", "%B %d, %Y at %I:%M %p",
            "%B %d, %Y at %I %p", "%B %d, %Y", "%Y-%m-%d",
        ]
        for event in raw_events:
            if not event.get("title") or not event.get("date"):
                continue
            event_date = None
            for fmt in date_formats:
                try:
                    event_date = datetime.strptime(event["date"].strip(), fmt)
                    break
                except ValueError:
                    continue
            if not event_date or event_date < cutoff:
                continue
            event["date"] = event_date.isoformat()
            event.setdefault("id", create_event_id(event["title"], event["date"], "Manual"))
            event.setdefault("source", "Manual")
            event.setdefault("url", "")
            event.setdefault("description", "")
            event.setdefault("location", "High Country, NC")
            event.setdefault("latitude", 36.2168)
            event.setdefault("longitude", -81.6746)
            events.append(event)
        log_info(f"  ✓ Loaded {len(events)} manual events")
    except Exception as e:
        log_error(f"  ✗ Error loading manual events: {e}")
    return events


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
async def main():
    print("=" * 60)
    print("High Country Events Scraper Starting")
    print("=" * 60)

    all_events = []
    scrapers = [
        ("Manual Events",           scrape_manual_events),
        ("High Country Host",       scrape_high_country_host),
        ("Alleghany Chamber",       scrape_alleghany_chamber),
        ("Alleghany Arts Council",  scrape_alleghany_arts),
        ("Ashe Chamber",            scrape_ashe_chamber),
        ("Stay Blue Ridge",         scrape_stay_blue_ridge),
        ("Old Barn Winery",         scrape_old_barn_winery),
        ("Blue Ridge Music NC",     scrape_blue_ridge_music),
        ("High Country Press",      scrape_explore_boone),
        ("Appalachian Theatre",     scrape_appalachian_theatre),
        ("Ashe County Arts",        scrape_ashe_county_arts),
        ("Downtown Boone",          scrape_downtown_boone),
        ("Grandfather Mountain",    scrape_grandfather_mountain),
        ("Boonerang Festival",      scrape_boonerang),
        ("NPS Blue Ridge Pkwy",     scrape_nps_blueridge),
        # Asheville sources
        ("Downtown Asheville",      scrape_downtown_asheville),
        ("Mountain Xpress",         scrape_mountain_xpress),
        ("AVL Today",               scrape_avl_today),
        ("AllEvents Asheville",     scrape_allevents_asheville),
    ]

    global _current_source
    async with aiohttp.ClientSession() as session:
        for source_name, scraper_func in scrapers:
            _current_source = source_name
            try:
                events = await scraper_func(session)
                all_events.extend(events)
                print(f"  → {source_name}: {len(events)} events")
            except Exception as e:
                log_error(f"  ✗ {source_name} failed: {e}")
                _fetch_failures[source_name] = str(e)
    _current_source = None

    print("\nDeduplication...")
    original_count = len(all_events)
    all_events = deduplicate_events(all_events)
    removed = original_count - len(all_events)
    print(f"  ✓ Removed {removed} duplicates" if removed else "  ✓ No duplicates found")

    all_events.sort(key=lambda x: x["date"])
    now = datetime.now()
    cutoff = now + timedelta(days=365)
    all_events = [
        e for e in all_events
        if datetime.fromisoformat(e["date"]) >= now - timedelta(hours=2)
        and datetime.fromisoformat(e["date"]) <= cutoff
    ]
    print(f"  • Filtered to {len(all_events)} future events (next 365 days)")

    output = {
        "events": all_events,
        "last_updated": now.isoformat(),
        "total_events": len(all_events),
        "sources": [name for name, _ in scrapers]
    }

    os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_FILE)), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Saved {len(all_events)} events to high_country_events.json")
    print("=" * 60)

    if _fetch_failures:
        print("\n❌ SCRAPER FAILURES — these sources could not be fetched:")
        for source, reason in _fetch_failures.items():
            print(f"  • {source}: {reason}")
        print("\nCheck if URLs changed or sites are down.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
