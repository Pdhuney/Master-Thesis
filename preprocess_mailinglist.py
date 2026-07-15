#!/usr/bin/env python3
"""
3GPP mailing-list preprocessing pipeline
=========================================
Master thesis: linking organisational mailing-list participation to formal
technical contribution (Change Requests) in 3GPP Release 18.

Produces analysis-ready tables:
  1. emails_processed3.csv          -- one row per email, enriched
  2. threads3.csv                   -- one row per reconstructed thread
  3. org_thread_response_times3.csv -- one row per (thread, organisation)
  4. reply_pairs3.csv               -- one row per matched reply
  5. org_participation3.csv         -- one row per organisation (participation index)
  6. org_network_edges3.csv         -- directed org->org reply edges
"""

import csv
import glob
import logging
import os
import re
import sys
import time
import unicodedata
from email.utils import parsedate_to_datetime

import numpy as np
import pandas as pd

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("preprocess.log", mode="w")],
)
log = logging.getLogger("mailinglist")

DEFAULT_DIR = "mailingListData"
DEFAULT_GLOB = "*_updated_updated.csv"
EXPECTED_COLS = ["list_name", "subject", "from", "reply_to", "date",
                 "content_type", "mail_link", "plain_url", "plain_text"]
DROP_FROM_EMAIL_CSV = ["reply_to_domain", "reply_to_email", "from_email",
                       "body_clean", "mail_link", "plain_url"]


def resolve_inputs(args):
    paths = []
    if not args:
        if os.path.isdir(DEFAULT_DIR):
            paths = sorted(glob.glob(os.path.join(DEFAULT_DIR, "*.csv")))
        else:
            paths = sorted(glob.glob(DEFAULT_GLOB))
    for a in args:
        if os.path.isdir(a):
            paths += sorted(glob.glob(os.path.join(a, "*.csv")))
        else:
            paths.append(a)
    seen, out = set(), []
    for p in paths:
        if p not in seen and os.path.exists(p):
            seen.add(p); out.append(p)
    return out


def load_inputs(paths):
    frames = []
    for p in paths:
        d = pd.read_csv(p, engine="python")
        missing = [c for c in EXPECTED_COLS if c not in d.columns]
        if missing:
            raise ValueError(f"{p} is missing columns: {missing}")
        if d["list_name"].isna().all():
            d["list_name"] = os.path.splitext(os.path.basename(p))[0]
        d["source_file"] = os.path.basename(p)
        frames.append(d)
        log.info("  loaded %5d rows from %s (lists: %s)",
                 len(d), os.path.basename(p),
                 ", ".join(map(str, d["list_name"].dropna().unique()[:3])))
    return pd.concat(frames, ignore_index=True)


def validate_data(df):
    n = len(df)
    rep = {
        "rows": n,
        "dup_subject+datetime": int(df.duplicated(subset=["subject", "datetime_utc"]).sum()),
        "dup_subject+datetime+from": int(df.duplicated(
            subset=["subject", "datetime_utc", "from"]).sum()),
        "dup_subject+date_string": int(df.duplicated(subset=["subject", "date"]).sum()),
        "missing_or_empty_subject": int(df["subject"].isna().sum()
                                        + (df["subject"].astype(str).str.strip() == "").sum()),
        "unparseable_date": int(df["datetime_utc"].isna().sum()),
        "missing_sender_email": int(df["from_email"].isna().sum()),
    }
    log.info("VALIDATION -- data-quality report:")
    log.info("  total rows                 : %d", rep["rows"])
    log.info("  DUPLICATES (subject+datetime): %d  <-- removed by de-dup step",
             rep["dup_subject+datetime"])
    log.info("  duplicates (subject+datetime+from): %d", rep["dup_subject+datetime+from"])
    log.info("  duplicates (subject+raw date string): %d", rep["dup_subject+date_string"])
    log.info("  rows w/ missing or empty subject: %d", rep["missing_or_empty_subject"])
    log.info("  rows w/ unparseable date   : %d", rep["unparseable_date"])
    log.info("  rows w/ no sender email    : %d", rep["missing_sender_email"])
    return rep


# ---------------------------------------------------------------------------
# 1. EMAIL / DOMAIN / ORGANISATION PARSING
# ---------------------------------------------------------------------------

MULTI_SUFFIXES = {
    "co.uk", "org.uk", "ac.uk", "gov.uk", "me.uk", "ltd.uk", "plc.uk", "net.uk",
    "co.kr", "or.kr", "ne.kr", "re.kr", "go.kr", "ac.kr",
    "com.cn", "net.cn", "org.cn", "gov.cn", "edu.cn", "ac.cn",
    "co.jp", "or.jp", "ne.jp", "ac.jp", "go.jp",
    "com.tw", "org.tw", "net.tw", "edu.tw",
    "com.au", "net.au", "org.au", "edu.au", "gov.au",
    "co.in", "net.in", "org.in", "gen.in", "firm.in", "ind.in",
    "com.br", "net.br", "org.br", "com.sg", "com.hk", "com.my",
    "co.il", "com.mx", "com.tr", "co.za", "com.es",
}

EMAIL_RE = re.compile(r"[<\(]?\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})\s*[>\)]?")


def extract_email(raw):
    if not raw or not isinstance(raw, str):
        return None
    m = EMAIL_RE.search(raw)
    return m.group(1).lower() if m else None


def domain_of(email):
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[1].lower().strip(".")


def org_domain_of(domain):
    if not domain:
        return None
    parts = domain.split(".")
    if len(parts) < 2:
        return domain
    last2 = ".".join(parts[-2:])
    last3 = ".".join(parts[-3:]) if len(parts) >= 3 else None
    if last3 in MULTI_SUFFIXES and len(parts) >= 4:
        return parts[-4]
    if last2 in MULTI_SUFFIXES and len(parts) >= 3:
        return parts[-3]
    return parts[-2]


# ---------------------------------------------------------------------------
# 2. BODY CLEANING
# ---------------------------------------------------------------------------

UNSUB_RE = re.compile(r"#{5,}.*?#{5,}", re.S)
ORIG_MSG_RE = re.compile(r"-{2,}\s*Original Message\s*-{2,}", re.I)
CAUTION_RE = re.compile(r"^\s*CAUTION:.*$", re.I | re.M)
URL_RE = re.compile(r"https?://\S+")


def clean_body(text):
    if not isinstance(text, str):
        return ""
    t = unicodedata.normalize("NFKC", text)
    t = UNSUB_RE.sub(" ", t)
    t = CAUTION_RE.sub(" ", t)
    cut = len(t)
    for pat in (ORIG_MSG_RE, re.compile(r"^\s*From:\s.+\bSent:", re.I | re.M),
                re.compile(r"^\s*From:\s.+@.+$", re.I | re.M),
                re.compile(r"^_{5,}$", re.M)):
        m = pat.search(t)
        if m:
            cut = min(cut, m.start())
    current = t[:cut]
    current = "\n".join(ln for ln in current.splitlines() if not ln.lstrip().startswith(">"))
    return re.sub(r"\s+", " ", current).strip()


# ---------------------------------------------------------------------------
# 3. SUBJECT NORMALISATION + THREADING
# ---------------------------------------------------------------------------

PREFIX_RE = re.compile(r"^\s*((re|fw|fwd|aw|sv|antw|wg|tr|rv)\s*(\[\d+\])?\s*:\s*)+", re.I)


def is_reply(subject):
    if not isinstance(subject, str):
        return False
    return bool(PREFIX_RE.match(subject.strip()))


def normalise_subject(subject):
    if not isinstance(subject, str):
        return ""
    s = subject.strip()
    prev = None
    while prev != s:
        prev = s
        s = PREFIX_RE.sub("", s).strip()
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).lower().strip()
    return s


# ---------------------------------------------------------------------------
# 4. RULE-BASED 4-CLASS LABELLING (graded by substantive weight)
# ---------------------------------------------------------------------------

CLASS_LABELS = {0: "Administrative / logistics",
                1: "Procedural coordination",
                2: "Substantive technical discussion",
                3: "Formal technical proposal"}
CLASS_WEIGHTS = {0: 0.0, 1: 0.5, 2: 1.0, 3: 1.5}
CLASS_PRIORITY = [3, 2, 1, 0]

CR_RE = re.compile(r"\bC[1-6]-\d{5,6}\b|\bS[1-6]-\d{5,6}\b")
SPEC_RE = re.compile(r"\b(?:23|24|29|33|38)\.\d{3}\b")
OOO_RE = re.compile(r"out of (the )?office|automatic reply|auto-?reply")

TDOC_RE = re.compile(r"(?<![A-Za-z0-9])[A-Za-z][A-Za-z0-9]-\d{4,10}(?![A-Za-z0-9])")
CR_ID_SEP = ", "


def extract_tdocs(text):
    if not isinstance(text, str):
        return []
    out = []
    for m in TDOC_RE.findall(text):
        u = m.upper()
        if u not in out:
            out.append(u)
    return out


PROPOSAL_KW = [
    "change request", " cr ", "cr,", "cr.", "draft cr", "pcr", "tdoc", "t-doc",
    "contribution", "attached contribution", "specification change", "spec change",
    "normative text", "revision", "revised", "revise", "approved", "rejected",
    "submitted", "i uploaded", "uploaded", "upload", "proposes", "proposes to modify",
    "modifies", "modify ts", "co-sign", "cosign", "co-signer", "cosigner", "endorse",
    "baseline", "cover sheet", "work item", "rel-18", "rel 18", "release 18", "merge",
    "merged", "for agreement", "for approval", "way forward", "comments and/or support",
    " ts ", " tr ", "ts2", "specification"]
TECH_KW = [
    "architecture", "requirement", "interface", "protocol", "security", "feature",
    " ue ", " ran", " sa2", " sa3", "core network", "mobility", "authentication",
    "session", "network function", "interworking", "clause", "subclause",
    "information element", " ie ", "nas", "5gmm", "5gsm", "pdu session", "ssc mode",
    "procedure", "parameter", "encoding", "stage 2", "stage 3", "rrc", "registration",
    "use case", "scenario", "handover", "qos", "signalling", "signaling"]
TECH_REASON_KW = [
    "does not support", "would not support", "we propose", "the issue is",
    "this implies", "because", "should ", "would ", "shall ", "in my view",
    "i think", "the problem is", "clarif"]
PROC_KW = [
    "we should discuss", "should this be discussed", "should be discussed",
    "should be handled", "handled in", "discuss this", "discuss in", "will you submit",
    "i will not submit", "not submit", "submit for next meeting", "for next meeting",
    "next meeting", "wait for feedback", "waiting for", "wait for", "please review",
    "review", "comment", "align", "alignment", "liaison", "working group",
    "no objection", "no comment", "ok for me", "fine for me", "fine with me",
    "agree", "agreed", "i support", "support this", "looks good", "offline",
    "let's", "let us", "i suggest", "proposal to discuss", "in sa2", "in sa3"]
ADMIN_KW = [
    "meeting time", "move the meeting", "the meeting", "meeting", "agenda", "room",
    "dial-in", "dial in", "minutes", "deadline", "registration", "register",
    "calendar", "schedule", "availability", "webex", "teams meeting", "ms teams",
    "zoom", "location", "reminder", "conference call", "concall", "time slot",
    "timeslot", "doodle", "online meeting", "logistics", "chairman", "secretary"]


def label_email(subject, body):
    text = f"{subject or ''} \n {body or ''}".lower()
    s = {0: 0.0, 1: 0.0, 2: 0.0, 3: 0.0}
    s[3] += 2.5 * len(CR_RE.findall(text))
    s[3] += sum(1.5 for k in PROPOSAL_KW if k in text)
    s[2] += 1.0 * len(set(SPEC_RE.findall(text)))
    s[2] += sum(1.0 for k in TECH_KW if k in text)
    s[2] += sum(0.5 for k in TECH_REASON_KW if k in text)
    s[1] += sum(1.0 for k in PROC_KW if k in text)
    s[0] += sum(1.0 for k in ADMIN_KW if k in text)
    if OOO_RE.search(text):
        s[0] += 6.0
    best_score = max(s.values())
    if best_score == 0.0:
        return 0, CLASS_LABELS[0], CLASS_WEIGHTS[0], 0.0, True, s
    cls = max(CLASS_PRIORITY, key=lambda c: (s[c], -CLASS_PRIORITY.index(c)))
    ordered = sorted(s.values(), reverse=True)
    second = ordered[1] if len(ordered) > 1 else 0.0
    ambiguous = (best_score - second) < 1.0
    conf = round((best_score - second) / best_score, 3)
    return cls, CLASS_LABELS[cls], CLASS_WEIGHTS[cls], conf, ambiguous, s


# ---------------------------------------------------------------------------
# 5. QUOTED-CHAIN PARENT DETECTION
# ---------------------------------------------------------------------------

QUOTED_FROM_RE = re.compile(r"From:\s*(.*?)\n.*?Sent:", re.S | re.I)
EMAIL_INLINE_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
OBO_RE = re.compile(r"On Behalf Of\s+(.*)", re.I)


def display_name(from_field):
    s = re.sub(r"<[^>]*>", "", str(from_field)).strip().strip('"').strip()
    return re.sub(r"\s+", " ", s).lower()


def parse_quoted_parent(body):
    if not isinstance(body, str):
        return None, None
    m = QUOTED_FROM_RE.search(body)
    if not m:
        return None, None
    frm = re.sub(r"\s+", " ", m.group(1))
    em = EMAIL_INLINE_RE.search(frm)
    em = em.group(0).lower() if em else None
    if em and em.endswith("list.etsi.org"):
        em = None
    obo = OBO_RE.search(frm)
    name = obo.group(1).strip().lower() if obo else None
    if name is None:
        nm = re.sub(r"<[^>]*>", "", frm).replace("[mailto:", "").strip()
        nm = re.sub(r"\s+", " ", nm).strip().lower()
        name = nm or None
    return em, name


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(inputs=None, since=None, until=None):
    t0 = time.time()
    paths = resolve_inputs(inputs if inputs is not None else sys.argv[1:])
    if not paths:
        raise SystemExit("No input CSVs found. Pass files/folder or create a "
                         f"'{DEFAULT_DIR}/' folder (or place '{DEFAULT_GLOB}' "
                         "files in the current directory).")
    log.info("STEP 1/9  Reading %d mailing-list file(s)", len(paths))
    df = load_inputs(paths)
    log.info("Loaded %d raw rows across %d source list(s)", len(df), df["list_name"].nunique())
    df = df.drop(columns=[c for c in ["source_file"] if c in df.columns])

    log.info("STEP 2/9  Parsing sender / reply-to / organisation domains")
    df["from_email"] = df["from"].apply(extract_email)
    df["from_domain"] = df["from_email"].apply(domain_of)
    df["from_org_domain"] = df["from_domain"].apply(org_domain_of)
    df["reply_to_email"] = df["reply_to"].apply(extract_email)
    df["reply_to_domain"] = df["reply_to_email"].apply(domain_of)
    df["reply_to_org_domain"] = df["reply_to_domain"].apply(org_domain_of)

    def to_dt(x):
        try:
            return parsedate_to_datetime(x)
        except Exception:
            return pd.NaT
    log.info("STEP 3/9  Parsing dates to UTC")
    df["datetime"] = df["date"].apply(to_dt)
    df["datetime_utc"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    n_bad = int(df["datetime_utc"].isna().sum())
    if n_bad:
        log.warning("  %d rows had unparseable dates (excluded from time stats)", n_bad)

    if since is not None:
        cut = pd.Timestamp(since, tz="UTC")
        before = len(df)
        df = df[df["datetime_utc"].notna() & (df["datetime_utc"] >= cut)].copy()
        log.info("  date filter: kept %d / %d emails on/after %s (dropped %d)",
                 len(df), before, since, before - len(df))
    if until is not None:
        cut = pd.Timestamp(until, tz="UTC")
        before = len(df)
        df = df[df["datetime_utc"] < cut].copy()
        log.info("  date filter: kept %d / %d emails before %s (dropped %d)",
                 len(df), before, until, before - len(df))

    df["year_month"] = df["datetime_utc"].dt.to_period("M").astype(str)
    df["hour_utc"] = df["datetime_utc"].dt.hour
    df["weekday"] = df["datetime_utc"].dt.day_name()

    log.info("STEP 4/9  Cleaning message bodies")
    df["body_clean"] = df["plain_text"].apply(clean_body)
    df["body_len"] = df["body_clean"].str.len()

    log.info("STEP 5/9  Reconstructing threads (subject-normalised)")
    df["subject_norm"] = df["subject"].apply(normalise_subject)
    df["is_reply"] = df["subject"].apply(is_reply)

    validate_data(df)

    before = len(df)
    df = df.drop_duplicates(subset=["subject", "datetime_utc"]).copy()
    log.info("  dropped %d duplicate rows (same subject + exact datetime); %d remain",
             before - len(df), len(df))

    THREAD_GAP_DAYS = 60
    df = df.sort_values(["subject_norm", "datetime_utc"], kind="stable").reset_index(drop=True)
    gap = df.groupby("subject_norm")["datetime_utc"].diff().dt.total_seconds() / 86400.0
    new_thread = (gap.isna()) | (gap > THREAD_GAP_DAYS)
    df["thread_id"] = new_thread.cumsum().astype(int)
    df = df.sort_values(["thread_id", "datetime_utc"], kind="stable").reset_index(drop=True)
    df["pos_in_thread"] = df.groupby("thread_id")["datetime_utc"].rank(method="first").astype(int)
    df["thread_size"] = df.groupby("thread_id")["thread_id"].transform("size")
    df["is_thread_starter"] = df["pos_in_thread"] == 1
    log.info("  %d threads after subject grouping (gap-split at %d days)",
             df["thread_id"].nunique(), THREAD_GAP_DAYS)

    df = df.sort_values(["thread_id", "datetime_utc"], kind="stable").reset_index(drop=True)
    df["from_name"] = df["from"].apply(display_name)

    log.info("STEP 6/9  Computing response times via quoted parent (%d rows)", len(df))
    n = len(df)
    tid_a = df["thread_id"].to_numpy()
    time_a = df["datetime_utc"].to_list()
    org_a = df["from_org_domain"].to_list()
    name_a = df["from_name"].to_list()
    email_a = [e.lower() if isinstance(e, str) else None for e in df["from_email"].to_list()]
    isreply_a = df["is_reply"].to_numpy()
    bodies = df["plain_text"].to_list()

    parent_org = [None] * n
    parent_person = [None] * n
    parent_times = [pd.NaT] * n
    resp_hours = [np.nan] * n
    match_method = ["root"] * n

    last_by_email, last_by_name, last_by_org = {}, {}, {}
    cur_tid = None
    for i in range(n):
        if i and i % 50000 == 0:
            log.info("  ...processed %d/%d rows", i, n)
        if tid_a[i] != cur_tid:
            cur_tid = tid_a[i]
            last_by_email.clear(); last_by_name.clear(); last_by_org.clear()
        p_em, p_name = parse_quoted_parent(bodies[i])
        j = None
        if p_em is not None and p_em in last_by_email:
            j = last_by_email[p_em]; match_method[i] = "quoted_email"
        elif p_name and p_name in last_by_name:
            j = last_by_name[p_name]; match_method[i] = "quoted_name"
        elif isreply_a[i]:
            best = -1
            for o, idx in last_by_org.items():
                if o != org_a[i] and idx > best:
                    best = idx
            if best >= 0:
                j = best; match_method[i] = "fallback_prev_diff_org"
        if j is not None:
            parent_org[i] = org_a[j]
            parent_person[i] = name_a[j]
            parent_times[i] = time_a[j]
            resp_hours[i] = (time_a[i] - time_a[j]).total_seconds() / 3600.0
        if email_a[i] is not None:
            last_by_email[email_a[i]] = i
        if name_a[i]:
            last_by_name[name_a[i]] = i
        if org_a[i] is not None and not (isinstance(org_a[i], float) and np.isnan(org_a[i])):
            last_by_org[org_a[i]] = i

    df["parent_org"] = parent_org
    df["parent_person"] = parent_person
    df["parent_time"] = parent_times
    df["response_time_hours"] = resp_hours
    df["response_match"] = match_method
    df["same_org_reply"] = (df["parent_org"] == df["from_org_domain"]) & df["parent_org"].notna()
    df["response_time_hours_xorg"] = df["response_time_hours"].where(~df["same_org_reply"])

    def weekday_hours(a, b):
        if pd.isna(a) or pd.isna(b) or b <= a:
            return np.nan
        sec, cur = 0.0, a
        while cur < b:
            nxt = min((cur + pd.Timedelta(days=1)).normalize(), b)
            if cur.weekday() < 5:
                sec += (nxt - cur).total_seconds()
            cur = nxt
        return sec / 3600.0

    df["response_time_business_hours"] = [
        weekday_hours(p, c) for p, c in zip(df["parent_time"], df["datetime_utc"])]
    df["response_time_business_hours_xorg"] = (
        df["response_time_business_hours"].where(~df["same_org_reply"]))

    log.info("STEP 7/9  Classifying emails into 4 weighted classes")
    lab = df.apply(lambda r: label_email(r["subject"], r["body_clean"]), axis=1)
    df["mail_class"] = [x[0] for x in lab]
    df["mail_class_label"] = [x[1] for x in lab]
    df["mail_class_weight"] = [x[2] for x in lab]
    df["class_confidence"] = [x[3] for x in lab]
    df["class_ambiguous"] = [x[4] for x in lab]
    df["is_substantive"] = df["mail_class"].isin([2, 3])

    log.info("STEP 7b/9 Extracting Tdoc/CR identifiers (per subject_norm union)")
    subj_ids = df["subject"].apply(extract_tdocs)
    body_ids = df["body_clean"].apply(extract_tdocs)
    own = [list(dict.fromkeys(s + b)) for s, b in zip(subj_ids, body_ids)]

    keys = [sn if isinstance(sn, str) and sn.strip() else f"__t{tid}"
            for sn, tid in zip(df["subject_norm"], df["thread_id"])]

    grp_union = {}
    for k, lst in zip(keys, own):
        u = grp_union.setdefault(k, [])
        for x in lst:
            if x not in u:
                u.append(x)

    cr_id, cr_src = [], []
    for s, b, k in zip(subj_ids, body_ids, keys):
        union = grp_union.get(k, [])
        cr_id.append(CR_ID_SEP.join(union))
        if not union:
            cr_src.append("")
        elif s:
            cr_src.append("subject")
        elif b:
            cr_src.append("body")
        else:
            cr_src.append("linked")
    df["cr_id"] = cr_id
    df["cr_id_source"] = cr_src
    log.info("  emails with a Tdoc/CR id: %d / %d (by %s)",
             int((df['cr_id'] != "").sum()), len(df),
             pd.Series(cr_src).value_counts().to_dict())

    log.info("STEP 8/9  Aggregating thread / org / network / index tables")

    def join_unique(s):
        seen = [x for x in s if pd.notna(x)]
        return ";".join(sorted(set(seen)))

    threads = df.groupby("thread_id").agg(
        subject_norm=("subject_norm", "first"),
        n_messages=("thread_id", "size"),
        n_replies=("is_reply", "sum"),
        start_time=("datetime_utc", "min"),
        end_time=("datetime_utc", "max"),
        starter_org=("from_org_domain", lambda s: s.iloc[0]),
        n_orgs=("from_org_domain", lambda s: s.nunique()),
        org_domains_involved=("from_org_domain", join_unique),
        avg_response_time_hours=("response_time_hours", "mean"),
        median_response_time_hours=("response_time_hours", "median"),
        avg_response_time_xorg_hours=("response_time_hours_xorg", "mean"),
        median_response_time_xorg_hours=("response_time_hours_xorg", "median"),
        avg_response_time_business_hours=("response_time_business_hours", "mean"),
        median_response_time_business_hours=("response_time_business_hours", "median"),
        n_substantive=("is_substantive", "sum"),
    ).reset_index()
    threads["duration_hours"] = (
        (threads["end_time"] - threads["start_time"]).dt.total_seconds() / 3600.0)
    threads["is_multi_message"] = threads["n_messages"] > 1

    org_thread = df.groupby(["thread_id", "from_org_domain"]).agg(
        subject_norm=("subject_norm", "first"),
        n_emails=("from_org_domain", "size"),
        n_replies=("is_reply", "sum"),
        started_thread=("is_thread_starter", "max"),
        n_responses=("response_time_hours", "count"),
        mean_response_time_hours=("response_time_hours", "mean"),
        median_response_time_hours=("response_time_hours", "median"),
        n_responses_xorg=("response_time_hours_xorg", "count"),
        mean_response_time_xorg_hours=("response_time_hours_xorg", "mean"),
        median_response_time_xorg_hours=("response_time_hours_xorg", "median"),
        mean_response_time_business_xorg_hours=("response_time_business_hours_xorg", "mean"),
        median_response_time_business_xorg_hours=("response_time_business_hours_xorg", "median"),
        min_response_time_hours=("response_time_hours", "min"),
        max_response_time_hours=("response_time_hours", "max"),
        first_email_time=("datetime_utc", "min"),
        last_email_time=("datetime_utc", "max"),
    ).reset_index()
    org_thread = org_thread.merge(
        threads[["thread_id", "n_messages", "n_orgs"]], on="thread_id", how="left")
    org_thread = org_thread.sort_values(
        ["thread_id", "mean_response_time_hours"]).reset_index(drop=True)

    reply_pairs = df[df["parent_org"].notna()][[
        "list_name", "thread_id", "subject_norm",
        "from_org_domain", "from_name", "datetime_utc",
        "parent_org", "parent_person", "response_time_hours",
        "response_time_business_hours",
        "same_org_reply", "response_match", "mail_class", "mail_class_label"]].copy()
    reply_pairs = reply_pairs.rename(columns={
        "from_org_domain": "responder_org", "from_name": "responder_person",
        "datetime_utc": "responder_time"})
    reply_pairs = reply_pairs.sort_values(["thread_id", "responder_time"])

    edges = reply_pairs[~reply_pairs["same_org_reply"]]
    edge_list = (edges.groupby(["responder_org", "parent_org"])
                 .size().reset_index(name="weight")
                 .rename(columns={"responder_org": "source_org",
                                  "parent_org": "target_org"})
                 .sort_values("weight", ascending=False))

    threads_started = (df[df["is_thread_starter"]]
                       .groupby("from_org_domain").size().rename("threads_started"))

    org = df.groupby("from_org_domain").agg(
        total_emails=("thread_id", "size"),
        n_admin=("mail_class", lambda s: int((s == 0).sum())),
        n_procedural=("mail_class", lambda s: int((s == 1).sum())),
        n_technical=("mail_class", lambda s: int((s == 2).sum())),
        n_proposal=("mail_class", lambda s: int((s == 3).sum())),
        substantive_participation_score=("mail_class_weight", "sum"),
        mean_class_weight=("mail_class_weight", "mean"),
        substantive_emails=("is_substantive", "sum"),
        replies_sent=("is_reply", "sum"),
        active_months=("year_month", "nunique"),
        n_threads_participated=("thread_id", "nunique"),
        mean_response_time_hours=("response_time_hours", "mean"),
        mean_response_time_xorg_hours=("response_time_hours_xorg", "mean"),
        median_response_time_xorg_hours=("response_time_hours_xorg", "median"),
        mean_response_time_business_xorg_hours=("response_time_business_hours_xorg", "mean"),
        median_response_time_business_xorg_hours=("response_time_business_hours_xorg", "median"),
    )
    org = org.join(threads_started).fillna({"threads_started": 0})
    org["threads_started"] = org["threads_started"].astype(int)

    out_deg = edge_list.groupby("source_org")["weight"].sum().rename("outgoing_replies")
    in_deg = edge_list.groupby("target_org")["weight"].sum().rename("incoming_replies")
    org = org.join(out_deg).join(in_deg).fillna(
        {"outgoing_replies": 0, "incoming_replies": 0})

    comp = ["substantive_participation_score", "replies_sent", "threads_started",
            "active_months"]
    weights = {"substantive_participation_score": 0.40, "replies_sent": 0.20,
               "threads_started": 0.20, "active_months": 0.20}
    z = org[comp].astype(float)
    z = (z - z.mean()) / z.std(ddof=0).replace(0, np.nan)
    z = z.fillna(0.0)
    org["weighted_participation_score"] = sum(z[c] * weights[c] for c in comp)
    org = org.sort_values("substantive_participation_score", ascending=False).reset_index()

    log.info("STEP 9/9  Writing output CSVs")
    email_cols = ["list_name", "thread_id", "pos_in_thread", "thread_size",
                  "is_thread_starter", "subject", "subject_norm", "is_reply",
                  "cr_id", "cr_id_source",
                  "from", "from_domain", "from_org_domain",
                  "reply_to", "reply_to_org_domain",
                  "date", "datetime_utc", "year_month", "hour_utc", "weekday",
                  "response_time_hours", "response_time_hours_xorg",
                  "response_time_business_hours", "response_time_business_hours_xorg",
                  "parent_org", "parent_person", "parent_time",
                  "response_match", "same_org_reply",
                  "mail_class", "mail_class_label", "mail_class_weight",
                  "class_confidence", "class_ambiguous", "is_substantive",
                  "body_len"]
    email_cols = [c for c in email_cols if c not in DROP_FROM_EMAIL_CSV]

    for name, frame in [("emails_processed.csv", df[email_cols]),
                        ("threads.csv", threads),
                        ("org_thread_response_times.csv", org_thread),
                        ("reply_pairs.csv", reply_pairs),
                        ("org_participation.csv", org),
                        ("org_network_edges.csv", edge_list)]:
        frame.to_csv(name, index=False)
        log.info("  wrote %-32s %6d rows x %2d cols", name, len(frame), frame.shape[1])

    rt = df["response_time_hours"].dropna()
    rtx = df["response_time_hours_xorg"].dropna()
    rtb = df["response_time_business_hours"].dropna()
    log.info("=" * 60)
    log.info("DONE in %.1fs  |  %d emails, %d threads, %d organisations",
             time.time() - t0, len(df), len(threads), org["from_org_domain"].nunique())
    log.info("  multi-message threads: %d | singletons: %d",
             int(threads["is_multi_message"].sum()), int((~threads["is_multi_message"]).sum()))
    log.info("  matched reply pairs  : %d (cross-org %d / same-org %d)",
             len(reply_pairs), int((~reply_pairs["same_org_reply"]).sum()),
             int(reply_pairs["same_org_reply"].sum()))
    log.info("  response time  raw=%.1fh (med %.1f) | business=%.1fh (med %.1f)",
             rt.mean(), rt.median(), rtb.mean(), rtb.median())
    log.info("  mail classes: %s", df["mail_class_label"].value_counts().to_dict())
    log.info("  ambiguous (review/LLM): %d", int(df["class_ambiguous"].sum()))
    top = org.head(5)[["from_org_domain", "substantive_participation_score"]]
    log.info("  top orgs by substantive score: %s",
             ", ".join(f"{r.from_org_domain}={r.substantive_participation_score:.0f}"
                       for r in top.itertuples()))
    log.info("Outputs: emails_processed / threads / org_thread_response_times / "
             "reply_pairs / org_participation / org_network_edges  (+ preprocess.log)")
    return dict(emails=df, threads=threads, org=org, reply_pairs=reply_pairs,
                org_thread=org_thread, edges=edge_list)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Preprocess pooled 3GPP mailing lists")
    ap.add_argument("inputs", nargs="*", help="CSV files or a folder (default: mailingListData/)")
    ap.add_argument("--since", default=None, help="keep emails on/after this date, e.g. 2019-07-01")
    ap.add_argument("--until", default=None, help="keep emails strictly before this date")
    args, _ = ap.parse_known_args()
    # If no inputs were given on the CLI, default to the etsi_output/ folder.
    inputs = args.inputs if args.inputs else ["etsi_output"]
    main(inputs, since=args.since, until=args.until)
