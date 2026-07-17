#!/usr/bin/env python3
"""
3GPP Change-Request (CR) preprocessing pipeline
===============================================
Master thesis: linking organisational mailing-list participation to formal
technical contribution (Change Requests) in 3GPP Release 18.

Cleans the raw CR export and produces analysis-ready CR tables:

  1. cr_rel18_clean.csv              -- one row per CR, cleaned + canonical sources
  2. cr_rel18_sources_long.csv       -- one row per (CR, canonical source, region)

Cleaning steps (in order):
  1. keep only Target Release == Rel-18
  2. drop category D (editorial)                     [configurable]
  3. keep only the selected WG statuses              [drops merged + noted]
  4. canonicalise the "SOURCES involved" names       (same vocabulary as the
     mailing-list preprocessing, via a learned map + generic rules + aliases)
  5. attach region per source and per CR
  6. add a working-group acceptance flag
"""

import os
import re
import sys
import logging
import unicodedata

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)-7s | %(message)s",
                    datefmt="%H:%M:%S",
                    handlers=[logging.StreamHandler(sys.stdout),
                              logging.FileHandler("preprocess_cr.log", mode="w")])
log = logging.getLogger("cr")

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
INPUT_CR        = "3gpp_rel18_with_tdoc_related_sources_canonical.csv"
NAME_MAP_FILE   = "cr_source_canonical_map.csv"   # learned raw_name -> canonical
ORG_REGION_FILE = "org_region_map.csv"            # canonical org -> region
OUT_MAIN        = "cr_rel18_clean.csv"
OUT_LONG        = "cr_rel18_sources_long.csv"

RAW_SRC_COL   = "SOURCES involved"
CANON_SRC_COL = "SOURCES involved (canonical)"
CAT_COL       = "CR Cat"
REL_COL       = "Target Release"
WG_STATUS_COL = "CR status at WG"
TDOC_COL      = "WG TDoc #"

TARGET_RELEASE   = "Rel-18"
DROP_CATEGORIES  = {"D"}          # editorial; keep A, B, C, E, F
# WG statuses kept (drops 'merged' and 'noted'); rows with no WG status are the
# "(missing)" bar and are kept as well.
KEEP_WG_STATUS   = {"agreed", "withdrawn", "postponed",
                    "not treated", "not pursued", "endorsed"}
KEEP_MISSING_WG  = True
# Working-group acceptance: accepted UNLESS the status is one of these.
NOT_ACCEPTED_WG  = {"withdrawn", "not pursued", "endorsed", "not treated"}
UNKNOWN_REGION   = "Other"

# Fallback aliases (compacted-token -> canonical), aligned with the mailing-list
# preprocessing so both sides share one vocabulary. The learned map above covers
# most names; these only catch anything not seen before.
ORG_ALIASES = {
    "deutschetelekom": "telekom",
    "magenta": "telekom",
    "nttdocomo": "nttdocomo",
    "docomo": "nttdocomo",
    "lgelectronics": "lge",
    "lg": "lge",
    "rohdeschwarz": "rohde-schwarz",
    "rohde&schwarz": "rohde-schwarz",
}

LEGAL_SUFFIX_RE = re.compile(
    r"\b(technolog(?:y|ies)|incorporated|inc|corporation|corp|company|co|"
    r"ltd|limited|gmbh|ag|llc|plc|oyj|oy|ab|kk|sarl|sas|s\.?a\.?|s\.?r\.?l\.?|"
    r"b\.?v\.?|n\.?v\.?|group|holding|holdings)\b", re.I)


# --------------------------------------------------------------------------- #
# CANONICALISATION
# --------------------------------------------------------------------------- #
def load_name_map():
    if not os.path.exists(NAME_MAP_FILE):
        log.warning("  name map %s not found -- using generic rules only", NAME_MAP_FILE)
        return {}
    m = pd.read_csv(NAME_MAP_FILE)
    d = {str(a).strip().lower(): str(b).strip().lower()
         for a, b in zip(m["raw_name"], m["canonical"]) if str(b).strip()}
    log.info("  loaded %d learned raw->canonical source mappings", len(d))
    return d


def generic_canon(name):
    x = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
    x = x.lower().strip()
    x = LEGAL_SUFFIX_RE.sub(" ", x)
    x = re.sub(r"[^a-z0-9&\- ]", " ", x)
    x = re.sub(r"\s+", " ", x).strip().replace(" ", "")
    if not x:
        return None
    return ORG_ALIASES.get(x, x)


def canonicalize_source(name, name_map):
    if name is None:
        return None
    key = str(name).strip().lower()
    if not key:
        return None
    if key in name_map:
        return name_map[key]
    return generic_canon(name)


def split_sources(raw):
    if not isinstance(raw, str):
        return []
    return [p.strip() for p in re.split(r"[;,]", raw) if p.strip()]


# --------------------------------------------------------------------------- #
# MAIN
# --------------------------------------------------------------------------- #
def main(input_path=INPUT_CR):
    log.info("STEP 1/6  Reading CR data: %s", input_path)
    df = pd.read_csv(input_path, low_memory=False)
    n0 = len(df)
    log.info("  %d raw CR rows, %d columns", n0, df.shape[1])

    log.info("STEP 2/6  Filtering to release %s", TARGET_RELEASE)
    df = df[df[REL_COL].astype(str).str.strip() == TARGET_RELEASE].copy()
    log.info("  kept %d / %d rows in %s", len(df), n0, TARGET_RELEASE)

    log.info("STEP 3/6  Dropping categories %s", sorted(DROP_CATEGORIES))
    cat = df[CAT_COL].astype(str).str.strip().str.upper()
    before = len(df)
    df = df[~cat.isin({c.upper() for c in DROP_CATEGORIES})].copy()
    log.info("  dropped %d rows; %d remain (categories: %s)",
             before - len(df), len(df),
             df[CAT_COL].value_counts(dropna=False).to_dict())

    log.info("STEP 4/6  Filtering WG status (keep %s; keep missing=%s)",
             sorted(KEEP_WG_STATUS), KEEP_MISSING_WG)
    st = df[WG_STATUS_COL].astype(str).str.strip().str.lower()
    is_missing = df[WG_STATUS_COL].isna() | (st == "nan") | (st == "")
    keep = st.isin(KEEP_WG_STATUS) | (is_missing if KEEP_MISSING_WG else False)
    before = len(df)
    df = df[keep].copy()
    log.info("  dropped %d rows (merged/noted/other); %d remain", before - len(df), len(df))

    log.info("STEP 5/6  Canonicalising sources + attaching region")
    name_map = load_name_map()
    reg = pd.read_csv(ORG_REGION_FILE)
    org2reg = {str(o).strip().lower(): str(r).strip()
               for o, r in zip(reg.iloc[:, 0], reg.iloc[:, 1])}

    canon_list, region_list = [], []
    for raw in df[RAW_SRC_COL]:
        srcs = []
        for nm in split_sources(raw):
            c = canonicalize_source(nm, name_map)
            if c and c not in srcs:
                srcs.append(c)
        canon_list.append(srcs)
        region_list.append(sorted({org2reg.get(s, UNKNOWN_REGION) for s in srcs}))
    df[CANON_SRC_COL] = [", ".join(s) for s in canon_list]
    df["SOURCES regions"] = ["; ".join(r) for r in region_list]
    df["n_sources"] = [len(s) for s in canon_list]

    # working-group acceptance flag (missing status counted as accepted)
    st2 = df[WG_STATUS_COL].astype(str).str.strip().str.lower()
    df["accepted_wg"] = (~st2.isin(NOT_ACCEPTED_WG)).astype(int)
    log.info("  canonical sources built; WG acceptance rate = %.1f%%",
             100 * df["accepted_wg"].mean())

    log.info("STEP 6/6  Writing outputs")
    df.to_csv(OUT_MAIN, index=False)
    log.info("  wrote %-28s %6d rows x %2d cols", OUT_MAIN, len(df), df.shape[1])

    # long format: one row per (CR, canonical source, region) -- for regional analysis
    long_rows = []
    keep_cols = [TDOC_COL, CAT_COL, WG_STATUS_COL, "accepted_wg"]
    keep_cols = [c for c in keep_cols if c in df.columns]
    for (_, row), srcs in zip(df.iterrows(), canon_list):
        base = {c: row[c] for c in keep_cols}
        for s in srcs:
            long_rows.append({**base, "source": s,
                              "region": org2reg.get(s, UNKNOWN_REGION)})
    long = pd.DataFrame(long_rows)
    long.to_csv(OUT_LONG, index=False)
    log.info("  wrote %-28s %6d rows x %2d cols", OUT_LONG, len(long), long.shape[1])

    # quick sanity summary
    log.info("=" * 60)
    log.info("SUMMARY: %d CRs kept (from %d)", len(df), n0)
    log.info("  categories : %s", df[CAT_COL].value_counts().to_dict())
    log.info("  WG status  : %s", df[WG_STATUS_COL].value_counts(dropna=False).to_dict())
    log.info("  by region (CR-involvement, accept%%):")
    for rg, g in long.groupby("region"):
        log.info("    %-12s n=%6d  acc=%.1f%%", rg, len(g), 100 * g["accepted_wg"].mean())
    return df, long


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else INPUT_CR
    main(path)
