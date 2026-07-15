# ETSI / 3GPP LISTSERV Scraper (requests + BeautifulSoup)

Scrapes the 3GPP mailing-list archives on the ETSI LISTSERV server
(`https://list.etsi.org/scripts/wa.exe`) and writes **one CSV per list** with
the columns:

```
list_name, subject, from, reply_to, date, content_type, mail_link, plain_url, plain_text
```

No Selenium / ChromeDriver needed — pure `requests` + `beautifulsoup4`.

## Install & run

```bash
pip install requests beautifulsoup4
python etsi_3gpp_scraper.py      # runs in TEST_MODE by default
```

`TEST_MODE = True` scrapes just one list (`3GPP_TSG_SA_WG3_LI`, capped at 8
messages) into `etsi_output/3GPP_TSG_SA_WG3_LI.csv`. Once that looks right, set
`TEST_MODE = False` to scrape **every** list whose name starts with `3GPP`,
keeping only messages from **January 2020 onward**.

## How it works (verified against the live site)

1. **List enumeration** — POST `?INDEX` with a large `lppts` to get all 914
   lists on one page, then keep names starting with `3GPP`.
2. **Period index** — `?A0=<LIST>` lists weekly archives. Period codes are
   `ind<YY><MM><week>` (e.g. `ind2505C` = May 2025, week 3). Only periods
   `>= 2020-01` are kept.
3. **Message links** — `?A1=<period>&L=<LIST>` gives the message table; we
   collect the `?A2=<LIST>;<id>.<period>` message links.
4. **Message page** — `?A2=...` exposes Subject / From / Reply To / Date /
   Content-Type (each as `<td><b>Label:</b></td><td>value</td>`), plus the
   `?A3=...&T=text/plain` part link, which we fetch for `plain_text`.

## About login and the `from` e-mail address — please read

Everything above works **anonymously**: subject, sender *name*, reply-to, date,
content-type, links, and the full plain-text body all download without logging
in. (`plain_text` for the test message matches your sample CSV exactly.)

The **only** field that differs is the sender's real e-mail address. When not
logged in, LISTSERV masks it as `[log in to unmask]`. Your sample CSV shows the
real address (`nagaraja.rao@NOKIA.COM`), which is only visible to a logged-in
session.

**The ETSI login is protected by Cloudflare Turnstile (a CAPTCHA).** A plain
script cannot log in through it, and this script does **not** attempt to bypass
the CAPTCHA. To get the real e-mail addresses, reuse a session you logged into
manually:

1. Log in at `https://list.etsi.org` in your normal browser (you solve the
   Turnstile).
2. Give the script those cookies, either:
   - set `USE_BROWSER_COOKIES = True` (needs `pip install browser_cookie3`,
     reads cookies straight from Chrome), **or**
   - paste a cookie string into `COOKIE_HEADER`
     (DevTools → Network → any `wa.exe` request → Request Headers → `Cookie`).

With valid cookies the `from` column fills in with real addresses; everything
else is unchanged.

## Key settings (top of the script)

| Setting | Meaning |
|---|---|
| `TEST_MODE` / `TEST_LIST_NAME` / `TEST_MAX_MESSAGES` | one-list test run |
| `START_YEAR, START_MONTH` | date floor (default 2020-01) |
| `LIST_PREFIX` | which lists (default `3GPP`) |
| `MAX_MESSAGES_PER_LIST` | cap for full runs |
| `MAX_PLAIN_CHARS` | truncate body (`None` = full text) |
| `REQUEST_DELAY` | seconds between requests (be polite to ETSI) |
| `OUTPUT_DIR` | where the per-list CSVs go |

## Tests

`python test_scraper.py` runs 17 offline checks (period parsing, the 2020
filter, field extraction against the real DOM structure, plain-text-part
selection, CSV column order). All pass.

## Please note

Respect the ETSI Terms of Service and keep `REQUEST_DELAY` reasonable. A full
run over all 3GPP lists since 2020 is a large number of requests — start with
`TEST_MODE`, and consider running list-by-list.
