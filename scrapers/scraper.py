"""
High Country Events Scraper
Scrapes multiple local event sites and writes high_country_events.json
to the repo root for GitHub Pages / mobile app consumption.

Required environment variables (GitHub Secrets):
  GEEKFLARE_API_KEY  — headless Chrome scraping API
  NPS_API_KEY        — NPS developer API key (free at nps.gov/subjects/developer)
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
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ High Country Host error: {e}")
    return events


async def scrape_alleghany_chamber(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping Alleghany Chamber (via Geekflare)...")
    events = []
    url = "https://alleghanycountychamber.memberclicks.net/index.php?option=com_mc_events&view=calendar"
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
                    log_error(f"  ✗ Alleghany Chamber item parse error: {e}")
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
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Alleghany Chamber parse error: {e}")
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
                log_info(f"  ✓ Found {len(events)} events via iCal")
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
                    log_error(f"  ✗ Alleghany Arts item parse error: {e}")
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
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ Alleghany Arts parse error: {e}")
    return events


def _parse_americantowns_html(soup, source_url, source_name, lat, lon, default_location) -> List[Dict]:
    events = []
    event_items = (
        soup.find_all("div", class_=lambda c: c and "event-item" in c.lower())
        or soup.find_all("div", class_=lambda c: c and "event-card" in c.lower())
        or soup.find_all("article", class_=lambda c: c and "event" in c.lower())
        or soup.find_all("li", class_=lambda c: c and "event" in c.lower())
        or soup.find_all("div", class_=lambda c: c and "event" in c.lower())
    )
    for item in event_items:
        try:
            title_el = (
                item.find("h2") or item.find("h3") or item.find("h4")
                or item.find(class_=lambda c: c and "title" in c.lower())
                or item.find(class_=lambda c: c and "name" in c.lower())
            )
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title:
                continue
            date_el = (
                item.find("time")
                or item.find(class_=lambda c: c and "date" in c.lower())
                or item.find(class_=lambda c: c and "when" in c.lower())
            )
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
            link = link_el["href"] if link_el else source_url
            if link and link.startswith("/"):
                link = f"https://www.americantowns.com{link}"
            loc_el = item.find(class_=lambda c: c and ("location" in c.lower() or "venue" in c.lower()))
            location = loc_el.get_text(strip=True) if loc_el else default_location
            desc_el = item.find("p")
            description = desc_el.get_text(strip=True) if desc_el else ""
            events.append({
                "id": create_event_id(title, event_date.isoformat(), source_name),
                "title": title,
                "date": event_date.isoformat(),
                "location": location or default_location,
                "description": description,
                "source": source_name,
                "url": link,
                "latitude": lat,
                "longitude": lon
            })
        except Exception as e:
            log_error(f"  ✗ AmericanTowns item parse error: {e}")
            continue
    return events


async def scrape_americantowns_alleghany(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping AmericanTowns Alleghany (via Geekflare)...")
    events = []
    url = "https://www.americantowns.com/alleghany-county-nc/events/"
    html = await fetch_with_geekflare(url, session)
    if not html:
        return events
    try:
        soup = BeautifulSoup(html, "html.parser")
        events = _parse_americantowns_html(soup, url, "AmericanTowns Alleghany", 36.4905, -81.1701, "Alleghany County, NC")
        if not events:
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
                    title = extract_title_from_html(html, html.find(f"{month} {day}"), "Local Event")
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), "AmericanTowns Alleghany"),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": "Alleghany County, NC",
                        "description": "",
                        "source": "AmericanTowns Alleghany",
                        "url": url,
                        "latitude": 36.4905,
                        "longitude": -81.1701
                    })
        log_info(f"  ✓ Found {len(events)} events")
    except Exception as e:
        log_error(f"  ✗ AmericanTowns Alleghany parse error: {e}")
    return events


async def scrape_americantowns_ashe(session: aiohttp.ClientSession) -> List[Dict]:
    log_info("Scraping AmericanTowns Ashe (via Geekflare)...")
    events = []
    url = "https://www.americantowns.com/ashe-county-nc/events/"
    html = await fetch_with_geekflare(url, session)
    if not html:
        return events
    try:
        soup = BeautifulSoup(html, "html.parser")
        events = _parse_americantowns_html(soup, url, "AmericanTowns Ashe", 36.4332, -81.4990, "Ashe County, NC")
        if not events:
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
                    title = extract_title_from_html(html, html.find(f"{month} {day}"), "Local Event")
                    events.append({
                        "id": create_event_id(title, event_date.isoformat(), "AmericanTowns Ashe"),
                        "title": title,
                        "date": event_date.isoformat(),
                        "location": "Ashe County, NC",
                        "description": ""
