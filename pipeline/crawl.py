"""
Marketing Pulse - Weekly Event Crawl Pipeline
"""

import os
import json
import time
import hashlib
import logging
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not FIRECRAWL_API_KEY:
    raise EnvironmentError("FIRECRAWL_API_KEY is not set.")
if not ANTHROPIC_API_KEY:
    raise EnvironmentError("ANTHROPIC_API_KEY is not set.")

from firecrawl import FirecrawlApp
import anthropic

SOURCES_FILE  = os.path.join(os.path.dirname(__file__), "sources.json")
KEYWORDS_FILE = os.path.join(os.path.dirname(__file__), "keywords.json")
OUTPUT_FILE   = os.path.join(os.path.dirname(__file__), "..", "data", "events.json")

CRAWL_DELAY     = 2
MAX_CONTENT_LEN = 15000
TODAY           = date.today().isoformat()

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("marketing-pulse")

firecrawl = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
claude    = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

with open(SOURCES_FILE)  as f: sources  = json.load(f)
with open(KEYWORDS_FILE) as f: keywords = json.load(f)

RELEVANCE_MIN = keywords.get("relevance_threshold", 7)
MAX_EVENTS    = keywords.get("max_events", 100)
T1  = ", ".join(keywords.get("tier_1_must_include", []))
T2  = ", ".join(keywords.get("tier_2_include_if_adjacent", []))
T3  = ", ".join(keywords.get("tier_3_exclude", []))
GEO = ", ".join(keywords.get("geography_include", []))

EXTRACTION_PROMPT = f"""
You are a marketing events curator for a professional marketing chapter.
Today's date is {TODAY}.

Extract ALL marketing-relevant events from the webpage content below.
Return a JSON array only — no preamble, no markdown fences, no explanation.

Each event object must have EXACTLY these fields:
  "title"           - Full event name (string)
  "date_start"      - Start date YYYY-MM-DD (string)
  "date_end"        - End date YYYY-MM-DD (string, same as date_start if single day)
  "location"        - City, Country or "Virtual" (string)
  "format"          - Exactly "In-Person" or "Virtual" (string)
  "cost"            - Exactly "Free" or "Paid" (string)
  "category"        - One of: Digital Marketing, B2B Marketing, Content Marketing,
                      Marketing Technology, Brand Strategy, Creativity, Marketing Innovation,
                      Inbound Marketing, Performance Marketing, Marketing Analytics,
                      Social Media, Email Marketing, Ecommerce, AI Marketing, Technology
  "description"     - 1-2 sentence summary (string)
  "url"             - Direct URL to event page (string)
  "relevance_score" - Integer 1-10 (int)

SCORING:
  Score 8-10: {T1}
  Score 7-8:  {T2}
  Exclude:    {T3}
  Geography:  {GEO}

RULES:
  - Only include events with date_start AFTER {TODAY}
  - Only include events with relevance_score >= {RELEVANCE_MIN}
  - If date is missing or unparseable, omit the event
  - If cost unclear, default to "Paid"
  - If format unclear but has physical location, use "In-Person"
  - Return [] if no qualifying events found
  - Return ONLY the raw JSON array, no other text
"""

REQUIRED_FIELDS = {"title", "date_start", "date_end", "location", "format",
                   "cost", "category", "description", "url", "relevance_score"}

def crawl_url(url: str, name: str):
    log.info(f"  Crawling: {name}")
    try:
        result  = firecrawl.scrape(url, formats=["markdown"])
content = result.markdown or ""
        if not content:
            log.warning(f"  Empty response for {name}")
            return None
        log.info(f"  Got {len(content):,} chars")
        return content
    except Exception as e:
        log.warning(f"  Crawl failed ({name}): {e}")
        return None

def extract_events(content: str, source_name: str):
    log.info(f"  Extracting with Claude...")
    try:
        message = claude.messages.create(
            model="claude-opus-4-5",
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": f"{EXTRACTION_PROMPT}\n\nSOURCE: {source_name}\n\n---\n\n{content[:MAX_CONTENT_LEN]}"
            }]
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:].strip()
        events = json.loads(raw)
        if not isinstance(events, list):
            return []
        log.info(f"  Found {len(events)} events")
        return events
    except json.JSONDecodeError as e:
        log.warning(f"  JSON parse error ({source_name}): {e}")
        return []
    except Exception as e:
        log.warning(f"  Extraction failed ({source_name}): {e}")
        return []

def validate_event(event: dict) -> bool:
    if not REQUIRED_FIELDS.issubset(event.keys()):
        return False
    if event.get("format") not in ("In-Person", "Virtual"):
        return False
    if event.get("cost") not in ("Free", "Paid"):
        return False
    try:
        datetime.strptime(event["date_start"], "%Y-%m-%d")
        datetime.strptime(event["date_end"], "%Y-%m-%d")
    except (ValueError, TypeError):
        return False
    return True

def normalise_event(event: dict, source_name: str) -> dict:
    event["source"]          = source_name
    event["relevance_score"] = int(event.get("relevance_score", 0))
    event["title"]           = str(event.get("title", "")).strip()
    event["description"]     = str(event.get("description", "")).strip()
    return event

def deduplicate(events: list) -> list:
    seen, unique = set(), []
    for e in events:
        key = hashlib.md5(
            f"{e.get('title','').lower().strip()}{e.get('date_start','')}".encode()
        ).hexdigest()
        if key not in seen:
            seen.add(key)
            unique.append(e)
    removed = len(events) - len(unique)
    if removed:
        log.info(f"  Removed {removed} duplicate(s)")
    return unique

def save_output(events: list) -> None:
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    payload = {
        "last_updated": datetime.now().isoformat(),
        "total_events": len(events),
        "events":       events,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log.info(f"  Saved {len(events)} events → {OUTPUT_FILE}")

def run() -> None:
    log.info("=" * 60)
    log.info("Marketing Pulse — Weekly Event Crawl")
    log.info(f"Date: {TODAY} | Max: {MAX_EVENTS} | Min relevance: {RELEVANCE_MIN}/10")
    log.info("=" * 60)

    all_sources = []
    for layer_key in ["layer_1_primary", "layer_2_aggregators", "layer_3_vendors", "layer_4_local"]:
        layer = sources.get("layers", {}).get(layer_key, [])
        all_sources.extend(layer)
        log.info(f"  {layer_key}: {len(layer)} sources")

    log.info(f"  Total: {len(all_sources)} sources\n")

    all_events     = []
    failed_sources = []

    for i, source in enumerate(all_sources, 1):
        name = source.get("name", "Unknown")
        url  = source.get("url", "")
        log.info(f"[{i}/{len(all_sources)}] {name}")

        content = crawl_url(url, name)
        if not content:
            failed_sources.append(name)
            time.sleep(CRAWL_DELAY)
            continue

        raw_events = extract_events(content, name)
        valid      = []
        for e in raw_events:
            if validate_event(e):
                valid.append(normalise_event(e, name))
        log.info(f"  {len(valid)} valid events")
        all_events.extend(valid)
        time.sleep(CRAWL_DELAY)

    log.info("\n" + "-" * 60)
    log.info(f"Raw collected:     {len(all_events)}")

    unique = deduplicate(all_events)
    log.info(f"After dedup:       {len(unique)}")

    final = sorted(unique, key=lambda e: e.get("date_start", "9999-12-31"))[:MAX_EVENTS]
    log.info(f"Final count:       {len(final)}")

    save_output(final)

    log.info("\n" + "=" * 60)
    log.info(f"DONE — {len(final)} events saved, {len(failed_sources)} sources failed")
    log.info("=" * 60)

if __name__ == "__main__":
    run()
