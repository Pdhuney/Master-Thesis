#!/usr/bin/env python3
"""
ETSI / 3GPP LISTSERV mailing-list scraper
=========================================

Scrapes the 3GPP mailing-list archives hosted on the ETSI LISTSERV server
(https://list.etsi.org/scripts/wa.exe) using `requests` + `BeautifulSoup`.

For every mailing list whose name starts with "3GPP", it walks the monthly /
weekly archive index, keeps only the periods in the configured date range, and
for every message extracts:

    list_name, subject, from, reply_to, date, content_type,
    mail_link, plain_url, plain_text

and writes ONE CSV per list.

Two-phase workflow (recommended):
  PHASE 1  RUN_PHASE="fetch"  -> anonymous, fast, gets everything incl. body
                                  (the 'from' e-mail stays masked). No ticket.
  PHASE 2  RUN_PHASE="emails" -> fills the REAL e-mail addresses into the CSVs
                                  (needs a LISTSERV ticket; resumable).
  RUN_PHASE="all"             -> body + real e-mail in one pass (needs ticket).

Extra-safe mode (ASK_PER_LIST): before scraping each list you are asked y/n;
lists whose CSV already exists are skipped automatically.
"""

from __future__ import annotations

import csv
import os
import re
import sys
import time
import json
import difflib
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup

# plain_text bodies can be huge -> lift CSV's default 131072-char field limit,
# otherwise reading the CSVs back (sender cache, resume) raises
# "field larger than field limit".
_csv_max = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_max)
        break
    except OverflowError:
        _csv_max = int(_csv_max / 10)

# =============================================================================
# CONFIGURATION  (edit this block)
# =============================================================================

BASE = "https://list.etsi.org/scripts/wa.exe"
SERVER = "https://list.etsi.org"

# --- what to scrape ----------------------------------------------------------
LIST_PREFIX = "3GPP"            # only lists whose name starts with this
START_YEAR, START_MONTH = 2020, 1   # keep periods >= this (January 2020)
END_YEAR, END_MONTH = 2026, 1       # keep periods <= this (nothing AFTER Jan 2026)

# --- two-phase run -----------------------------------------------------------
# "fetch"  = PHASE 1: scrape everything ANONYMOUSLY (subject, name, reply-to,
#            date, content-type, links AND the full body). No ticket needed.
# "senders"= PHASE 2 SMART: resolve each UNIQUE sender's e-mail ONCE into a
#            shared cache (skips senders already cached -> big time saver), then
#            fuzzy-match that cache onto the 'from' column of every CSV.
# "emails" = PHASE 2 simple: fetch a header page for EVERY masked message.
# "all"    = single-pass behaviour (body + real e-mail together, ticket).
RUN_PHASE = "fetch"

# --- smart sender cache (used by RUN_PHASE = "senders") ----------------------
SENDER_CACHE_FILE = "etsi_output/senders_cache.json"  # name -> real e-mail
FUZZY_THRESHOLD = 0.90         # 0..1: how close a name must match to reuse an
                               # e-mail (lower = more matches but more risk)

# --- test vs full run --------------------------------------------------------
TEST_MODE = False              # True  -> scrape ONE list only (see TEST_LIST_NAME)
                               # False -> scrape every 3GPP* list
TEST_LIST_NAME = "3GPP_TSG_SA_WG3_LI"   # the list used in TEST_MODE
TEST_MAX_MESSAGES = None        # cap messages in TEST_MODE (None = no cap)

MAX_MESSAGES_PER_LIST = None   # cap for full runs (None = no cap)
MAX_PLAIN_CHARS = None         # truncate plain_text to N chars (None = full body)

# --- output ------------------------------------------------------------------
OUTPUT_DIR = "etsi_output"     # one <LIST_NAME>.csv is written here per list
SKIP_EXISTING = True           # on re-runs, skip lists whose CSV already exists
                               # (lets you stop/resume a long full run safely)
ASK_PER_LIST = True            # extra-safe: ask y/n before each list. Lists whose
                               # CSV already exists are skipped without asking.
LOG_FILE = "etsi_output/scrape.log"   # timestamped progress log (also on screen)

# --- speed / politeness / robustness -----------------------------------------
# THE SERVER CAPS HOW MANY CONNECTIONS IT ACCEPTS FROM ONE IP. Going over that
# cap gives connect/read TIMEOUTS and resets that make the run SLOWER, not
# faster. If you see timeouts, LOWER MAX_WORKERS. Sweet spot is usually 6-12.
MAX_WORKERS = 13               # parallel fetches. THE main speed knob.
REQUEST_DELAY = 0.05           # small gap each worker waits after a request.
FETCH_PLAIN_TEXT = True        # set False to skip body downloads -> ~2x faster
TIMEOUT = (10, 30)             # (connect, read): fail a dead connection in 10s.
MAX_RETRIES = 10               # retries per request, with exponential backoff
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0 Safari/537.36")

# --- authentication (only needed for real e-mail addresses, PHASE 2 / "all") -
# The 'from' e-mail is unmasked only by an authenticated LISTSERV session.
# RECOMMENDED: the URL ticket. Log in at list.etsi.org, open any archive page,
# and copy the X=... value from the address bar into LISTSERV_TICKET (+ your
# login e-mail into LOGIN_EMAIL). Tickets expire after ~20-30 min; on expiry the
# script pauses and asks for a fresh one (see PROMPT_ON_EXPIRY).
LISTSERV_TICKET = ""
LOGIN_EMAIL = ""

# Alternative: paste the raw Cookie header from DevTools -> Network -> a wa.exe
# request -> Request Headers -> Cookie:. Also set USER_AGENT to navigator.userAgent.
COOKIE_HEADER = ""
BROWSER_COOKIE_DOMAIN = "list.etsi.org"
USE_BROWSER_COOKIES = False    # usually fails (httpOnly session cookie); use a
                               # ticket or COOKIE_HEADER instead.

PROMPT_ON_EXPIRY = True        # pause and ask for a fresh ticket on expiry
                               # instead of stopping.

CSV_COLUMNS = ["list_name", "subject", "from", "reply_to", "date",
               "content_type", "mail_link", "plain_url", "plain_text"]


# =============================================================================
# Logging  (timestamped, to screen + LOG_FILE)
# =============================================================================

def log(msg: str) -> None:
    line = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}  {msg}"
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
        with open(LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:                    # noqa: BLE001
        pass


# =============================================================================
# HTTP helpers
# =============================================================================

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT,
                      "Accept": "text/html,application/xhtml+xml,*/*"})
    pool = max(10, MAX_WORKERS * 2)
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    if USE_BROWSER_COOKIES:
        try:
            import browser_cookie3
            cj = None
            for loader in ("chrome", "edge", "brave", "firefox", "load"):
                try:
                    fn = getattr(browser_cookie3, loader)
                    cj = fn(domain_name=BROWSER_COOKIE_DOMAIN)
                    if cj and len(list(cj)) > 0:
                        log(f"[auth] loaded {len(list(cj))} cookies via "
                            f"browser_cookie3.{loader}")
                        break
                except Exception:                # noqa: BLE001
                    continue
            if cj:
                s.cookies.update(cj)
            else:
                log("[auth] WARNING: browser_cookie3 found no cookies "
                    "(common with new Chrome). Use COOKIE_HEADER instead.")
        except Exception as e:                   # noqa: BLE001
            log(f"[auth] could not load browser cookies ({e})")

    if COOKIE_HEADER.strip():
        n = 0
        raw = COOKIE_HEADER.strip()
        if raw.lower().startswith("cookie:"):
            raw = raw.split(":", 1)[1]
        for part in raw.split(";"):
            if "=" in part:
                k, v = part.strip().split("=", 1)
                for dom in (BROWSER_COOKIE_DOMAIN, "." + BROWSER_COOKIE_DOMAIN):
                    s.cookies.set(k.strip(), v.strip(), domain=dom)
                n += 1
        log(f"[auth] applied {n} cookies from COOKIE_HEADER")

    names = [c.name for c in s.cookies]
    log(f"[auth] session cookies present: {sorted(set(names)) or 'NONE'}")
    return s


# --- a known message whose From is a real address when authenticated ---------
AUTH_PROBE_URL = (BASE + "?A2=3GPP_TSG_SA_WG3_LI;2f08282c.2505C&S=")


def auth_check(session: requests.Session) -> bool:
    """Fetch one known message and report whether we are really logged in."""
    try:
        html = get(session, AUTH_PROBE_URL)
    except Exception as e:                        # noqa: BLE001
        log(f"[auth] probe request failed: {e}")
        return False
    low = html.lower()

    cf_markers = ("just a moment", "checking your browser",
                  "cf-challenge", "enable javascript and cookies",
                  "/cdn-cgi/challenge-platform")
    looks_like_message = ("nagaraja" in low or "sa3#li-97" in low
                          or "deferred documents" in low)
    if any(m in low for m in cf_markers) and not looks_like_message:
        log("[auth] >>> BLOCKED by Cloudflare (challenge page returned).")
        log("[auth] >>> USER_AGENT must EXACTLY match the browser you logged in "
            "with (DevTools console: navigator.userAgent), same computer/IP.")
        return False

    if "log in to unmask" in low:
        log("[auth] >>> NOT authenticated: e-mails will show "
            "'[log in to unmask]'.")
        log("[auth] >>> Grab a fresh X ticket from the browser address bar into "
            "LISTSERV_TICKET (or paste a Cookie header), then re-run.")
        return False

    if looks_like_message:
        log("[auth] OK: authenticated, e-mail addresses are unmasked.")
        return True

    log("[auth] ?? unexpected probe response; proceeding, check the first rows.")
    return True


def _auth_params() -> dict:
    """LISTSERV session ticket appended to every request when configured."""
    if LISTSERV_TICKET and LOGIN_EMAIL:
        return {"X": LISTSERV_TICKET, "Y": LOGIN_EMAIL}
    return {}


def _auth_enabled() -> bool:
    return bool((LISTSERV_TICKET and LOGIN_EMAIL)
                or USE_BROWSER_COOKIES or COOKIE_HEADER.strip())


class TicketExpired(RuntimeError):
    """Raised mid-run when the session stops authenticating, so we stop instead
    of silently writing '[log in to unmask]' rows."""


def get(session: requests.Session, url: str, **params) -> str:
    """GET with retries + polite delay. Returns response text.

    IMPORTANT: LISTSERV's wa.exe treats the FIRST query parameter as the command
    (A0/A1/A2/...). The auth ticket (X/Y) MUST come AFTER it, otherwise LISTSERV
    ignores the command and redirects to ?INDEX. So merge params first, ticket
    after.
    """
    merged = {**params, **_auth_params()}
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=merged or None, timeout=TIMEOUT)
            r.raise_for_status()
            if REQUEST_DELAY:
                time.sleep(REQUEST_DELAY)
            return r.text
        except Exception as e:           # noqa: BLE001
            last_err = e
            backoff = min(60, 2 ** attempt)   # 2,4,8,16,32s ... capped 60
            log(f"   ! GET failed (try {attempt}/{MAX_RETRIES}): {e} "
                f"-> waiting {backoff}s")
            time.sleep(backoff)
    raise RuntimeError(f"GET giving up: {url} :: {last_err}")


def post(session: requests.Session, url: str, data: dict) -> str:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(url, data=data, timeout=TIMEOUT)
            r.raise_for_status()
            if REQUEST_DELAY:
                time.sleep(REQUEST_DELAY)
            return r.text
        except Exception as e:           # noqa: BLE001
            last_err = e
            backoff = min(60, 2 ** attempt)
            log(f"   ! POST failed (try {attempt}/{MAX_RETRIES}): {e} "
                f"-> waiting {backoff}s")
            time.sleep(backoff)
    raise RuntimeError(f"POST giving up: {url} :: {last_err}")


# =============================================================================
# 1) Enumerate the 3GPP lists
# =============================================================================

def enumerate_lists(session: requests.Session) -> list[str]:
    names: set[str] = set()
    try:
        html = post(session, BASE, data={"INDEX": "", "lppts": "5000"})
        names |= _parse_list_names(html)
    except Exception as e:               # noqa: BLE001
        log(f"[lists] POST enumeration failed ({e}); trying GET")

    if not names:
        html = get(session, BASE, INDEX="")
        names |= _parse_list_names(html)

    three_gpp = sorted(n for n in names
                       if n.upper().startswith(LIST_PREFIX.upper()))
    log(f"[lists] found {len(names)} total lists, "
        f"{len(three_gpp)} starting with '{LIST_PREFIX}'")
    return three_gpp


def _parse_list_names(html: str) -> set[str]:
    return set(re.findall(r"[?&]A0=([A-Za-z0-9_.\-]+)", html))


# =============================================================================
# 2) Period (month/week) index for a list  ->  keep date range
# =============================================================================

_PERIOD_RE = re.compile(r"(?:ind|log)(\d{2,4})(\d{2})([A-Z])?$", re.I)


def parse_period_code(code: str) -> tuple[int, int] | None:
    """ind2505C -> (2025, 5). Returns None if it can't be parsed."""
    m = _PERIOD_RE.match(code.strip())
    if not m:
        return None
    yy, mm = m.group(1), int(m.group(2))
    year = 2000 + int(yy) if len(yy) == 2 else int(yy)
    if not (1 <= mm <= 12):
        return None
    return year, mm


def period_in_range(year: int, month: int) -> bool:
    return (START_YEAR, START_MONTH) <= (year, month) <= (END_YEAR, END_MONTH)


def get_periods(session: requests.Session, list_name: str) -> list[tuple[str, int, int]]:
    html = get(session, BASE, A0=list_name)
    soup = BeautifulSoup(html, "html.parser")

    seen, periods = set(), []
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]A1=([A-Za-z0-9]+)", a["href"])
        if not m:
            continue
        code = m.group(1)
        if code in seen:
            continue
        parsed = parse_period_code(code)
        if not parsed:
            continue
        year, month = parsed
        if period_in_range(year, month):
            seen.add(code)
            periods.append((code, year, month))

    periods.sort(key=lambda t: (t[1], t[2]), reverse=True)
    return periods


# =============================================================================
# 3) Message links inside a period
# =============================================================================

def get_message_links(session: requests.Session, list_name: str,
                      period_code: str) -> list[str]:
    html = get(session, BASE, A1=period_code, L=list_name)
    soup = BeautifulSoup(html, "html.parser")

    pat = re.compile(r"[?&]A2=" + re.escape(list_name) + r";", re.I)
    links, seen = [], set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if pat.search(href):
            url = urljoin(SERVER, href)
            if url not in seen:
                seen.add(url)
                links.append(url)
    return links


# =============================================================================
# 4) Parse a single message page (?A2=...)
# =============================================================================

def _field_after_label(soup: BeautifulSoup, label: str) -> str:
    """LISTSERV renders <tr><td><b>Label:</b></td><td>VALUE</td></tr>."""
    for b in soup.find_all("b"):
        if b.get_text(strip=True) == label:
            td = b.find_parent("td")
            if td is not None:
                val = td.find_next_sibling("td")
                if val is not None:
                    return val.get_text(" ", strip=True)
    return ""


def parse_message(session: requests.Session, mail_link: str) -> dict | None:
    html = get(session, mail_link)
    soup = BeautifulSoup(html, "html.parser")

    if _auth_enabled() and "log in to unmask" in html.lower():
        raise TicketExpired(
            "session expired: 'from' is masked again. Grab a fresh X ticket and "
            "re-run (finished lists are skipped).")

    subject = _field_after_label(soup, "Subject:")
    sender = _field_after_label(soup, "From:")
    reply_to = _field_after_label(soup, "Reply To:")
    date = _field_after_label(soup, "Date:")
    content_type = _field_after_label(soup, "Content-Type:")

    if not (subject or sender or date):
        return None

    plain_url = ""
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "A3=" in href and re.search(r"T=text(/|%2F)plain", href, re.I):
            plain_url = urljoin(SERVER, href)
            break

    plain_text = ""
    if plain_url and FETCH_PLAIN_TEXT:
        try:
            raw = get(session, plain_url)
            plain_text = BeautifulSoup(raw, "html.parser").get_text("\n").strip()
            if MAX_PLAIN_CHARS:
                plain_text = plain_text[:MAX_PLAIN_CHARS]
        except Exception as e:           # noqa: BLE001
            log(f"     ! plain-text fetch failed: {e}")

    return {
        "subject": subject,
        "from": sender,
        "reply_to": reply_to,
        "date": date,
        "content_type": content_type,
        "mail_link": _canonical_mail_link(mail_link),
        "plain_url": plain_url,
        "plain_text": plain_text,
    }


def _canonical_mail_link(url: str) -> str:
    m = re.search(r"(A2=[A-Za-z0-9_.\-]+;[0-9a-f.]+[A-Z]?)", url)
    if m:
        return f"{BASE}?{m.group(1)}&S="
    return url


# =============================================================================
# 5) Scrape one list -> one CSV (resumable)
# =============================================================================

def _load_done_links(path: str) -> set:
    done = set()
    if os.path.exists(path):
        try:
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    ml = row.get("mail_link")
                    if ml:
                        done.add(ml)
        except Exception:                    # noqa: BLE001
            pass
    return done


def scrape_list(session: requests.Session, list_name: str,
                max_messages: int | None) -> str:
    log(f"=== LIST: {list_name}  (range {START_YEAR}-{START_MONTH:02d} .. "
        f"{END_YEAR}-{END_MONTH:02d}) ===")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{list_name}.csv")
    tmp_path = out_path + ".part"

    done_links = _load_done_links(tmp_path)
    if done_links:
        log(f"  {list_name}: resuming, {len(done_links)} messages already saved")

    periods = get_periods(session, list_name)
    log(f"  {list_name}: {len(periods)} periods in range")

    # gather all message links across periods IN PARALLEL
    all_links = []

    def _links_for(period):
        code, year, month = period
        return code, year, month, get_message_links(session, list_name, code)

    if MAX_WORKERS > 1 and len(periods) > 1:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            gathered = list(ex.map(_links_for, periods))
    else:
        gathered = [_links_for(p) for p in periods]
    for code, year, month, links in gathered:
        log(f"  {list_name} {year}-{month:02d} ({code}): {len(links)} messages")
        all_links.extend(links)
    if max_messages is not None:
        all_links = all_links[:max_messages]

    todo = [l for l in all_links if _canonical_mail_link(l) not in done_links]
    log(f"  {list_name}: {len(todo)} to fetch "
        f"({len(done_links)} already done) with {MAX_WORKERS} workers")

    if not todo:
        if os.path.exists(tmp_path):
            os.replace(tmp_path, out_path)
        else:
            with open(out_path, "w", newline="", encoding="utf-8") as fh:
                csv.DictWriter(fh, fieldnames=CSV_COLUMNS).writeheader()
        log(f"  -> {list_name}: done ({len(done_links)} rows)")
        return out_path

    new_file = not (os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0)
    n = len(done_links)
    try:
        with open(tmp_path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            if new_file:
                writer.writeheader()

            def fetch(link):
                return parse_message(session, link)

            if MAX_WORKERS > 1 and len(todo) > 1:
                ex = ThreadPoolExecutor(max_workers=MAX_WORKERS)
                results = ex.map(fetch, todo)
            else:
                ex = None
                results = (fetch(l) for l in todo)

            try:
                for row in results:
                    if not row:
                        continue
                    row["list_name"] = list_name
                    writer.writerow({c: row.get(c, "") for c in CSV_COLUMNS})
                    n += 1
                    if n % 50 == 0:
                        fh.flush()
                        log(f"    {list_name}: {n}/{len(all_links)} rows ...")
            finally:
                if ex is not None:
                    ex.shutdown(wait=True)

        os.replace(tmp_path, out_path)        # atomic: only on full success
    except BaseException:
        raise                                 # keep .part for resume

    log(f"  -> {list_name}: wrote {n} rows to {out_path}")
    return out_path


# =============================================================================
# 6) PHASE 2 — fill the real e-mail addresses into existing CSVs
# =============================================================================

def _needs_email(from_value: str) -> bool:
    v = from_value or ""
    return ("log in to unmask" in v.lower()) or ("@" not in v)


def _fetch_from_field(session: requests.Session, mail_link: str) -> str:
    html = get(session, mail_link)
    if "log in to unmask" in html.lower():
        raise TicketExpired(
            "session expired during e-mail backfill: 'from' is masked. "
            "Paste a fresh X ticket to continue (already-filled rows are kept).")
    soup = BeautifulSoup(html, "html.parser")
    return _field_after_label(soup, "From:")


def _write_rows(path: str, rows: list) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in CSV_COLUMNS})
    os.replace(tmp, path)


def fill_emails_in_file(session: requests.Session, path: str) -> None:
    name = os.path.basename(path)
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    todo = [i for i, r in enumerate(rows) if _needs_email(r.get("from", ""))]
    if not todo:
        log(f"  {name}: all e-mails already filled, skipping")
        return
    log(f"  {name}: filling {len(todo)} e-mails with {MAX_WORKERS} workers")

    filled = 0

    def work(i):
        return i, _fetch_from_field(session, rows[i].get("mail_link", ""))

    try:
        if MAX_WORKERS > 1 and len(todo) > 1:
            ex = ThreadPoolExecutor(max_workers=MAX_WORKERS)
            results = ex.map(work, todo)
        else:
            ex = None
            results = (work(i) for i in todo)
        try:
            for i, newfrom in results:
                if newfrom and "@" in newfrom:
                    rows[i]["from"] = newfrom
                    filled += 1
        finally:
            if ex is not None:
                ex.shutdown(wait=True)
    finally:
        _write_rows(path, rows)            # always persist (partial = resumable)
    log(f"  -> {name}: filled {filled} e-mails")


def fill_emails(session: requests.Session, only_list: str | None = None) -> None:
    global LISTSERV_TICKET
    if not os.path.isdir(OUTPUT_DIR):
        log(f"No '{OUTPUT_DIR}' folder — run PHASE 1 (RUN_PHASE='fetch') first.")
        return
    files = sorted(os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR)
                   if f.endswith(".csv"))
    if only_list:
        files = [p for p in files if os.path.basename(p) == f"{only_list}.csv"]
    if not files:
        log("No CSVs to fill — run PHASE 1 first.")
        return
    log(f"PHASE 2: filling real e-mails in {len(files)} CSV file(s)")
    idx = 0
    while idx < len(files):
        path = files[idx]
        log(f"########## emails [{idx + 1}/{len(files)}] "
            f"{os.path.basename(path)} ##########")
        try:
            fill_emails_in_file(session, path)
        except TicketExpired as e:
            log(f"!! {e}")
            new = _prompt_new_ticket()
            if new:
                LISTSERV_TICKET = new
                log("[auth] fresh ticket applied; retrying this file")
                continue
            log("================ stopped (refresh ticket & re-run) ===========")
            return
        except Exception as e:           # noqa: BLE001
            log(f"  !! failed on {os.path.basename(path)}: {e}")
        idx += 1
    log("================ all e-mails done ================")


# =============================================================================
# 7) SMART sender cache — resolve each unique sender's e-mail once, then
#    fuzzy-match it onto every CSV's 'from' column.
# =============================================================================

def _sender_name(from_value: str) -> str:
    """Display name part of a From line: '"Alex Leadbeater" <...>' -> Alex
    Leadbeater."""
    name = (from_value or "").split("<", 1)[0]
    return name.strip().strip('"').strip()


def _norm_name(name: str) -> str:
    """Normalise a name for matching. Keeps word ORDER (no scrambling), drops
    '(Nokia)'/punctuation, and turns 'Lastname, Firstname' into 'Firstname
    Lastname' so both spellings match."""
    s = (name or "").strip().strip('"').strip()
    s = re.sub(r"\(.*?\)", " ", s)            # drop "(Nokia)" etc.
    # "Last, First"  ->  "First Last"  (only a single, simple comma)
    if s.count(",") == 1:
        a, b = s.split(",", 1)
        if a.strip() and b.strip() and len(b.split()) <= 3:
            s = f"{b.strip()} {a.strip()}"
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _extract_email(from_value: str) -> str:
    m = re.search(r"<([^<>]+@[^<>]+)>", from_value or "")
    if m:
        return m.group(1).strip()
    m = re.search(r"[\w.+-]+@[\w.-]+\.\w+", from_value or "")
    return m.group(0).strip() if m else ""


def _load_sender_cache() -> dict:
    if os.path.exists(SENDER_CACHE_FILE):
        try:
            with open(SENDER_CACHE_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception:                    # noqa: BLE001
            return {}
    return {}


def _save_sender_cache(cache: dict) -> None:
    os.makedirs(os.path.dirname(SENDER_CACHE_FILE) or ".", exist_ok=True)
    tmp = SENDER_CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=1)
    os.replace(tmp, SENDER_CACHE_FILE)


def _csv_files(only_list: str | None = None) -> list:
    if not os.path.isdir(OUTPUT_DIR):
        return []
    files = sorted(os.path.join(OUTPUT_DIR, f) for f in os.listdir(OUTPUT_DIR)
                   if f.endswith(".csv"))
    if only_list:
        files = [p for p in files if os.path.basename(p) == f"{only_list}.csv"]
    return files


def build_sender_cache(session: requests.Session,
                       only_list: str | None = None) -> dict:
    """Resolve each UNIQUE masked sender to a real e-mail, once, into a shared
    on-disk cache. Senders already in the cache are skipped (the time saver)."""
    global LISTSERV_TICKET
    cache = _load_sender_cache()
    log(f"[cache] {len(cache)} senders already cached")

    # one representative message link per unique masked sender name
    reps: dict[str, str] = {}
    for path in _csv_files(only_list):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                frm = row.get("from", "")
                nm = _norm_name(_sender_name(frm))
                if not nm:
                    continue
                if not _needs_email(frm):           # already unmasked -> free
                    em = _extract_email(frm)
                    if em and nm not in cache:
                        cache[nm] = em
                elif nm not in cache and nm not in reps:
                    ml = row.get("mail_link", "")
                    if ml:
                        reps[nm] = ml

    remaining = [(nm, ml) for nm, ml in reps.items() if nm not in cache]
    log(f"[cache] resolving {len(remaining)} new unique senders "
        f"(total messages don't matter — one fetch per sender)")

    def work(item):
        nm, ml = item
        return nm, _extract_email(_fetch_from_field(session, ml))

    while remaining:
        batch = remaining[:max(1, MAX_WORKERS) * 5]
        try:
            if MAX_WORKERS > 1 and len(batch) > 1:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                    for nm, em in ex.map(work, batch):
                        cache[nm] = em          # "" marks 'tried, none' (no loop)
            else:
                for item in batch:
                    nm, em = work(item)
                    cache[nm] = em
            _save_sender_cache(cache)
            remaining = [(nm, ml) for nm, ml in remaining if nm not in cache]
            log(f"[cache] {len(cache)} resolved, {len(remaining)} to go")
        except TicketExpired as e:
            _save_sender_cache(cache)
            log(f"!! {e}")
            new = _prompt_new_ticket()
            if new:
                LISTSERV_TICKET = new
                log("[auth] fresh ticket applied; continuing sender resolve")
                continue
            log("================ stopped (refresh ticket & re-run) ===========")
            return cache

    _save_sender_cache(cache)
    log(f"[cache] done: {len(cache)} senders cached in {SENDER_CACHE_FILE}")
    return cache


def apply_sender_cache(only_list: str | None = None) -> None:
    """Fill the real e-mail into every CSV's 'from' column using the cache,
    with fuzzy name matching for slight spelling differences."""
    cache = _load_sender_cache()
    if not cache:
        log("[apply] sender cache is empty — build it first.")
        return
    keys = [k for k, v in cache.items() if v]    # only names with an e-mail
    log(f"[apply] using {len(keys)} cached senders (fuzzy >= {FUZZY_THRESHOLD})")

    for path in _csv_files(only_list):
        name = os.path.basename(path)
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        changed = 0
        for r in rows:
            frm = r.get("from", "")
            if not _needs_email(frm):
                continue
            nm = _norm_name(_sender_name(frm))
            em = cache.get(nm, "")
            if not em:                            # fuzzy fallback
                hit = difflib.get_close_matches(nm, keys, n=1,
                                                cutoff=FUZZY_THRESHOLD)
                if hit:
                    em = cache.get(hit[0], "")
            if em:
                if "[log in to unmask]" in frm:
                    r["from"] = frm.replace("[log in to unmask]", em)
                elif "@" not in frm:
                    r["from"] = f"{frm} <{em}>"
                changed += 1
        if changed:
            _write_rows(path, rows)
        log(f"  {name}: filled {changed} of {len(rows)} rows")


# =============================================================================
# interactive helpers
# =============================================================================

def _ask_yes_no(question: str) -> bool:
    """y/n prompt. Non-interactive (e.g. nohup) -> default yes so background
    runs don't hang."""
    if not (sys.stdin and sys.stdin.isatty()):
        return True
    try:
        ans = input(f"{question} [y/n]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return False
    return ans in ("y", "yes", "j", "ja")


def _prompt_new_ticket() -> str:
    """Ask the user to paste a fresh X ticket when the session expires."""
    if not PROMPT_ON_EXPIRY:
        return ""
    try:
        if not (sys.stdin and sys.stdin.isatty()):
            return ""        # non-interactive -> just stop
        print("\n>>> In your browser open any list archive and copy the "
              "X=... value from the address bar.", flush=True)
        return input(">>> Paste fresh X ticket (or press Enter to stop): ").strip()
    except (EOFError, KeyboardInterrupt):
        return ""


# =============================================================================
# main
# =============================================================================

def main() -> None:
    global LISTSERV_TICKET, COOKIE_HEADER, USE_BROWSER_COOKIES
    phase = RUN_PHASE.lower()
    if phase == "fetch":
        # PHASE 1 is anonymous on purpose: no ticket = no expiry = max speed.
        LISTSERV_TICKET = ""
        COOKIE_HEADER = ""
        USE_BROWSER_COOKIES = False

    session = build_session()
    have_auth = bool((LISTSERV_TICKET and LOGIN_EMAIL)
                     or USE_BROWSER_COOKIES or COOKIE_HEADER.strip())
    auth = "AUTHENTICATED (real emails)" if have_auth \
        else "ANONYMOUS (from-email masked as [log in to unmask])"
    phase_desc = {"fetch": "PHASE 1 (anonymous: all data incl. body, fast)",
                  "senders": "PHASE 2 SMART (cache unique senders + fuzzy fill)",
                  "emails": "PHASE 2 (fill real e-mail addresses)",
                  "all": "single pass (body + real e-mail together)"}.get(phase, phase)
    log("================ ETSI 3GPP scraper starting ================")
    log(f"  phase       : {phase_desc}")
    log(f"  list filter : names starting with '{LIST_PREFIX}'")
    log(f"  date filter : {START_YEAR}-{START_MONTH:02d} .. "
        f"{END_YEAR}-{END_MONTH:02d} (inclusive)")
    log(f"  mode        : {'TEST (one list)' if TEST_MODE else 'FULL (all lists)'}")
    log(f"  ask per list: {ASK_PER_LIST}")
    log(f"  session     : {auth}")
    log(f"  output      : one CSV per list in '{OUTPUT_DIR}/'")

    # ---- PHASE 2 SMART: cache unique senders, then fuzzy-fill the CSVs ------
    if phase == "senders":
        only = TEST_LIST_NAME if TEST_MODE else None
        if not have_auth:
            log("PHASE 'senders' needs a ticket. Set LISTSERV_TICKET + LOGIN_EMAIL.")
            return
        auth_check(session)
        build_sender_cache(session, only_list=only)
        log("---- all unique senders resolved; fuzzy-filling CSVs ----")
        apply_sender_cache(only_list=only)
        log("================ senders done ================")
        return

    # ---- PHASE 2: just fill e-mails into the CSVs from phase 1 --------------
    if phase == "emails":
        if not have_auth:
            log("PHASE 2 needs a ticket. Set LISTSERV_TICKET + LOGIN_EMAIL.")
            return
        auth_check(session)
        fill_emails(session, only_list=TEST_LIST_NAME if TEST_MODE else None)
        return

    # ---- PHASE 1 / all: scrape ---------------------------------------------
    if have_auth:
        auth_check(session)

    if TEST_MODE:
        log(f"*** TEST MODE: only {TEST_LIST_NAME} (max {TEST_MAX_MESSAGES} msgs) ***")
        scrape_list(session, TEST_LIST_NAME, TEST_MAX_MESSAGES)
        log("================ done ================")
        return

    lists = enumerate_lists(session)
    if not lists:
        log("No 3GPP lists found — aborting.")
        sys.exit(1)
    log(f"  {len(lists)} lists to process: {', '.join(lists[:5])}"
        + (" ..." if len(lists) > 5 else ""))

    idx = 0
    while idx < len(lists):
        name = lists[idx]
        log(f"########## [{idx + 1}/{len(lists)}] {name} ##########")
        out_path = os.path.join(OUTPUT_DIR, f"{name}.csv")

        # 1) already scraped -> skip silently (no question)
        if SKIP_EXISTING and os.path.exists(out_path):
            log(f"  already done ({out_path}) -> skipping")
            idx += 1
            continue

        # 2) extra-safe: ask y/n before scraping this list
        if ASK_PER_LIST and not _ask_yes_no(f"Scrape {name}?"):
            log(f"  skipped {name} (user said no)")
            idx += 1
            continue

        try:
            scrape_list(session, name, MAX_MESSAGES_PER_LIST)
        except TicketExpired as e:
            log(f"!! {e}")
            new = _prompt_new_ticket()
            if new:
                LISTSERV_TICKET = new
                log("[auth] fresh ticket applied; retrying this list")
                continue                      # retry SAME list, don't advance
            log("================ stopped (refresh ticket & re-run) ===========")
            return
        except Exception as e:           # noqa: BLE001
            log(f"  !! failed on {name}: {e}")
        idx += 1

    log("================ all lists done ================")


if __name__ == "__main__":
    main()
