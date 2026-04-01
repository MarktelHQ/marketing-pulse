"""
Marketing Pulse - Weekly Event Crawl Pipeline
=============================================
Crawls 46 sources across 4 layers using Firecrawl,
extracts structured event data using Claude API,
deduplicates, filters by relevance, and saves to data/events.json.

Setup:
  pip install firecrawl-py anthropic python-dotenv
  cp pipeline/.env.example pipeline/.env
  # Add your API keys to .env
  python pipeline/crawl.py

Runs automatically every Monday at 7am UTC via GitHub Actions.
"""

import os
import json
import time
import hashlib
import logging
from datetime import date, datetime
from dotenv import load_dotenv

# ── Load environment ──────────────────────────────────────────────────────────
load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not FIRECRAWL_API_KEY:
    raise EnvironmentError("FIRECRAWL_API_KEY is not set. Check your .env file.")
if not ANTHROPIC_API_KEY:
    raise EnvironmentError("ANTHROPIC_API_KEY is not set. Check your .env file.")

from firecrawl import FirecrawlApp
import anthropic

# ── Config ────────────────────────────────────────────────────────────────────
SOURCES_FILE  = os.path.join(os.path.dirname(__file__), "sources.json")
KEYWORDS_FILE = os.path.join(os.path.dirname(__file__), "keywords.json")
OUTPUT_FILE   = os.path.join(os.path.dirname(__file__), "..", "data", "events.json")

CRAWL_DELAY     = 2      # seconds between requests (be polite to servers)
MAX_CONTENT_LEN = 15000  # chars sent to Claude per source (token budget)
TODAY           = date.today().isoformat()

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("marketing-pulse")

# ── Clients ───────────────────────────────────────────────────────────────────
firecrawl = FirecrawlApp(api_key=FIRECRAWL_API_KEY)
claude    = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

# ── Load config files ─────────────────────────────────────────────────────────
with open(SOURCES_FILE)  as f: sources  = json.load(f)
with open(KEYWORDS_FILE) as f: keywords = json.load(f)

RELEVANCE_MIN = keywords.get("relevance_threshold", 7)
MAX_EVENTS    = keywords.get("max_events", 100)
T1  = ", ".join(keywords.get("tier_1_must_include", []))
T2  = ", ".join(keywords.get("tier_2_include_if_adjacent", []))
T3  = ", ".join(keywords.get("tier_3_exclude", []))
GEO = ", ".join(keywords.get("geography_include", []))

# ── Extraction prompt ─────────────────────────────────────────────────────────
EXTRACTION_PROMPT = f"""
You are a marketing events curator for a professional marketing chapter at ABB.
Today's date is {TODAY}.

Extract ALL marketing-relevant events from the webpage content provided below.
Return a JSON array only — no preamble, no markdown fences, no explanation.

Each event object must have EXACTLY these fields:
  "title"          – Full event name (string)
  "date_start"     – Start date in YYYY-MM-DD format (string)
  "date_end"       – End date in YYYY-MM-DD format (string, same as date_start if single day)
  "location"       – City, Country — or "Virtual" (string)
  "format"         – Exactly "In-Person" or "Virtual" (string)
  "cost"           – Exactly "Free" or "Paid" (string)
  "category"       – Best-fit category from: Digital Marketing, B2B Marketing, Content Marketing,
                     Marketing Technology, Brand Strategy, Creativity, Marketing Innovation,
                     Inbound Marketing, Performance Marketing, Marketing Analytics,
                     Social Media, Email Marketing, Ecommerce, AI Marketing, Technology (string)
  "description"    – 1–2 sentence summary of what the event covers (string)
  "url"            – Direct URL to the event page (string)
  "relevance_score"– Integer 1–10 based on marketing relevance (int)

SCORING GUIDE:
  Score 8–10 (Tier 1 — core marketing):
    {T1}

  Score 7–8 (Tier 2 — adjacent / marketing-adjacent tech):
    {T2}

  Score below 7 — exclude these entirely:
    {T3}

GEOGRAPHY: Only include events located in or relevant to:
  {GEO}

RULES:
  - Only include events with date_start AFTER {TODAY}
  - Only include events with relevance_score >= {RELEVANCE_MIN}
  - If a date is missing or unparseable, omit the event
  - If cost is unclear, default to "Paid"
  - If format is unclear but has a physical location, use "In-Person"
  - Return [] if no qualifying events found
  - Return ONLY the raw JSON array — no other text
"""


# ── Step 1: Crawl a URL ───────────────────────────────────────────────────────
def crawl_url(url: str, name: str) -> str | None:
    """Fetch page content as markdown via Firecrawl."""
    log.info(f"  Crawling: {name}")
    try:
        result  = firecrawl.scrape_url(url, params={"formats": ["markdown"]})
        content = result.get("markdown", "")
        if not content:
            log.warning(f"  Empty response for {name}")
            return None
        log.info(f"  Got {len(content):,} chars")
        return content
    except Exception as e:
        log.warning(f"  Crawl failed ({name}): {e}")
        return None


# ── Step 2: Extract events with Claude ───────────────────────────────────────
def extract_events(content: str, source_name: str) -> list[dict]:
    """Send markdown content to Claude and parse returned JSON events."""
    log.info(f"  Extracting events with Claude...")
    truncated = content[:MAX_CONTENT_LEN]

    try:
        message = claude.messages.create(
            model="claude-opus-4-5",
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": f"{EXTRACTION_PROMPT}\n\nSOURCE: {source_name}\n\n---\n\n{truncated}"
            }]
        )
        raw = message.content[0].text.strip()

        # Strip accidental markdown fences if Claude adds them
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:].strip()

        events = json.loads(raw)

        if not isinstance(events, list):
            log.warning(f"  Claude returned non-list for {source_name}")
            return []

        log.info(f"  Found {len(events)} events")
        return events

    except json.JSONDecodeError as e:
        log.warning(f"  JSON parse error ({source_name}): {e}")
        return []
    except Exception as e:
        log.warning(f"  Claude extraction failed ({source_name}): {e}")
        return []


# ── Step 3: Validate & normalise events ──────────────────────────────────────
REQUIRED_FIELDS = {"title", "date_start", "date_end", "location", "format",
                   "cost", "category", "description", "url", "relevance_score"}

def validate_event(event: dict) -> bool:
    """Return True if event has all required fields and valid values."""
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
    """Attach source and ensure consistent field types."""
    event["source"]          = source_name
    event["relevance_score"] = int(event.get("relevance_score", 0))
    event["title"]           = str(event.get("title", "")).strip()
    event["description"]     = str(event.get("description", "")).strip()
    return event


# ── Step 4: Deduplicate ───────────────────────────────────────────────────────
def deduplicate(events: list[dict]) -> list[dict]:
    """Remove duplicates based on title + start date fingerprint."""
    seen   = set()
    unique = []
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


# ── Step 5: Write output ──────────────────────────────────────────────────────
def save_output(events: list[dict]) -> None:
    """Write final events to data/events.json."""
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    payload = {
        "last_updated":  datetime.now().isoformat(),
        "total_events":  len(events),
        "events":        events,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    log.info(f"  Saved {len(events)} events → {OUTPUT_FILE}")


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run() -> None:
    log.info("=" * 60)
    log.info("Marketing Pulse — Weekly Event Crawl")
    log.info(f"Date: {TODAY} | Max events: {MAX_EVENTS} | Min relevance: {RELEVANCE_MIN}/10")
    log.info("=" * 60)

    # Flatten all source layers into a single list
    all_sources: list[dict] = []
    for layer_key in ["layer_1_primary", "layer_2_aggregators",
                       "layer_3_vendors",  "layer_4_local"]:
        layer = sources.get("layers", {}).get(layer_key, [])
        all_sources.extend(layer)
        log.info(f"  {layer_key}: {len(layer)} sources")

    log.info(f"  Total: {len(all_sources)} sources to crawl\n")

    all_events: list[dict] = []
    failed_sources: list[str] = []

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

        valid_events = []
        for e in raw_events:
            if validate_event(e):
                valid_events.append(normalise_event(e, name))
            else:
                log.debug(f"  Skipped invalid event: {e.get('title','?')}")

        log.info(f"  {len(valid_events)} valid events from this source")
        all_events.extend(valid_events)

        time.sleep(CRAWL_DELAY)

    log.info("\n" + "─" * 60)
    log.info(f"Raw events collected: {len(all_events)}")

    # Deduplicate
    unique_events = deduplicate(all_events)
    log.info(f"After deduplication:  {len(unique_events)}")

    # Sort by start date, cap at MAX_EVENTS
    final_events = sorted(
        unique_events,
        key=lambda e: e.get("date_start", "9999-12-31")
    )[:MAX_EVENTS]
    log.info(f"Final event count:    {len(final_events)}")

    # Save
    save_output(final_events)

    # Summary
    log.info("\n" + "=" * 60)
    log.info("CRAWL COMPLETE")
    log.info(f"  Events saved:    {len(final_events)}")
    log.info(f"  Sources failed:  {len(failed_sources)}")
    if failed_sources:
        for s in failed_sources:
            log.info(f"    ✗ {s}")
    log.info("=" * 60)


if __name__ == "__main__":
    run()
