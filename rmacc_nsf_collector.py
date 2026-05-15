#!/usr/bin/env python3
"""
RMACC NSF OAC Grant Collector
==============================
Queries the NSF Awards API for OAC-related grants at all RMACC member
institutions, stores results in a SQLite database, and exports JSON
for the interactive map visualization.

Usage:
    python3 rmacc_nsf_collector.py [--db rmacc_grants.db] [--export]

    --db       Path to SQLite database (default: rmacc_grants.db)
    --export   Export JSON for the visualization after collecting data

Requirements: Python 3.7+ (stdlib only, no pip installs needed)
"""

import json
import re
import sqlite3
import sys
import time
import urllib.request
import urllib.parse
import argparse
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────
# RMACC MEMBER INSTITUTIONS
# ─────────────────────────────────────────────────────────────────
RMACC_MEMBERS = [
    # (name, abbr, city, state, lat, lon, type)
    ("Arizona State University", "ASU", "Tempe", "AZ", 33.4242, -111.9281, "R1 University"),
    ("Arizona Western College", "AWC", "Yuma", "AZ", 32.6927, -114.6153, "Community College"),
    ("Boise State University", "BSU", "Boise", "ID", 43.6021, -116.2048, "R1 University"),
    ("Brigham Young University", "BYU", "Provo", "UT", 40.2518, -111.6493, "R1 University"),
    ("Colorado Mesa University", "CMU", "Grand Junction", "CO", 39.0839, -108.5507, "University"),
    ("Colorado School of Mines", "Mines", "Golden", "CO", 39.7514, -105.2225, "R1 University"),
    ("Colorado State University", "CSU", "Fort Collins", "CO", 40.5734, -105.0866, "R1 University"),
    ("Colorado State University Pueblo", "CSUP", "Pueblo", "CO", 38.2847, -104.6234, "University"),
    ("Columbia College", "CC", "Denver", "CO", 39.7400, -104.9870, "College"),
    ("Denver Botanic Gardens", "DBG", "Denver", "CO", 39.7320, -104.9594, "Research Org"),
    ("University of Denver", "DU", "Denver", "CO", 39.6765, -104.9619, "R1 University"),
    ("Grand Canyon University", "GCU", "Phoenix", "AZ", 33.5088, -112.1254, "University"),
    ("Idaho National Lab", "INL", "Idaho Falls", "ID", 43.5168, -112.0340, "Federal Lab"),
    ("Idaho State University", "ISU", "Pocatello", "ID", 42.8638, -112.4326, "University"),
    ("MSU Denver", "MSUD", "Denver", "CO", 39.7447, -105.0074, "University"),
    ("Montana State University", "MSU", "Bozeman", "MT", 45.6671, -111.0497, "R1 University"),
    ("Morgan Community College", "MCC", "Fort Morgan", "CO", 40.2503, -103.7998, "Community College"),
    ("NCAR", "NCAR", "Boulder", "CO", 39.9780, -105.2755, "Federal Lab"),
    ("National Jewish Health", "NJH", "Denver", "CO", 39.7392, -104.9394, "Research Org"),
    ("New Mexico State University", "NMSU", "Las Cruces", "NM", 32.2814, -106.7477, "R1 University"),
    ("New Mexico Tech", "NMT", "Socorro", "NM", 34.0654, -106.9060, "University"),
    ("NOAA", "NOAA", "Boulder", "CO", 40.0394, -105.2547, "Federal Agency"),
    ("Northern Arizona University", "NAU", "Flagstaff", "AZ", 35.1886, -111.6530, "University"),
    ("NREL", "NREL", "Golden", "CO", 39.7408, -105.1694, "Federal Lab"),
    ("Pima Community College", "PCC", "Tucson", "AZ", 32.2828, -110.9465, "Community College"),
    ("Regis University", "Regis", "Denver", "CO", 39.7879, -105.0239, "University"),
    ("University of Arizona", "UA", "Tucson", "AZ", 32.2319, -110.9501, "R1 University"),
    ("University of Colorado Anschutz", "CU Anschutz", "Aurora", "CO", 39.7464, -104.8378, "Medical Campus"),
    ("University of Colorado Boulder", "CU Boulder", "Boulder", "CO", 40.0076, -105.2659, "R1 University"),
    ("University of Colorado Colorado Springs", "UCCS", "Colorado Springs", "CO", 38.8940, -104.8014, "University"),
    ("University of Colorado Denver", "CU Denver", "Denver", "CO", 39.7457, -105.0072, "University"),
    ("University of Idaho", "UI", "Moscow", "ID", 46.7262, -117.0142, "R1 University"),
    ("University of Montana", "UMT", "Missoula", "MT", 46.8625, -113.9847, "R1 University"),
    ("University of Nevada, Las Vegas", "UNLV", "Las Vegas", "NV", 36.1080, -115.1403, "R1 University"),
    ("University of New Mexico", "UNM", "Albuquerque", "NM", 35.0844, -106.6189, "R1 University"),
    ("University of Northern Colorado", "UNCO", "Greeley", "CO", 40.4050, -104.6975, "University"),
    ("University of Utah", "UU", "Salt Lake City", "UT", 40.7649, -111.8421, "R1 University"),
    ("University of Wyoming", "UWyo", "Laramie", "WY", 41.3149, -105.5666, "R1 University"),
    ("USGS", "USGS", "Lakewood", "CO", 39.7163, -105.1130, "Federal Agency"),
    ("Washington State University", "WSU", "Pullman", "WA", 46.7298, -117.1817, "R1 University"),
    ("Western Colorado University", "Western", "Gunnison", "CO", 38.5449, -106.9253, "University"),
]

# NSF awardeeName variants → RMACC abbreviation
NAME_ALIASES = {
    "Arizona State University": "ASU",
    "Arizona Western College": "AWC",
    "Grand Canyon University": "GCU",
    "Northern Arizona University": "NAU",
    "Pima Community College": "PCC",
    "Pima County Community College District": "PCC",
    "University of Arizona": "UA",
    "Colorado Mesa University": "CMU",
    "Colorado School of Mines": "Mines",
    "Colorado State University": "CSU",
    "Colorado State University-Pueblo": "CSUP",
    "Colorado State University Pueblo": "CSUP",
    "Columbia College": "CC",
    "Denver Botanic Gardens": "DBG",
    "University of Denver": "DU",
    "Metropolitan State University of Denver": "MSUD",
    "Morgan Community College": "MCC",
    "University Corporation For Atmospheric Res": "NCAR",
    "University Corporation for Atmospheric Research": "NCAR",
    "UCAR": "NCAR",
    "National Jewish Health": "NJH",
    "National Oceanic & Atmospheric Administration": "NOAA",
    "National Oceanic and Atmospheric Administration": "NOAA",
    "National Renewable Energy Laboratory": "NREL",
    "Alliance for Sustainable Energy LLC": "NREL",
    "Alliance for Sustainable Energy": "NREL",
    "Regis University": "Regis",
    "University of Colorado at Boulder": "CU Boulder",
    "University of Colorado Boulder": "CU Boulder",
    "University of Colorado at Colorado Springs": "UCCS",
    "University of Colorado Colorado Springs": "UCCS",
    "University of Colorado at Denver": "CU Denver",
    "University of Colorado Denver": "CU Denver",
    "University of Colorado Anschutz Medical Campus": "CU Anschutz",
    "University of Northern Colorado": "UNCO",
    "U.S. Geological Survey": "USGS",
    "US Geological Survey": "USGS",
    "Western Colorado University": "Western",
    "Western State Colorado University": "Western",
    "Boise State University": "BSU",
    "Idaho National Laboratory": "INL",
    "Battelle Energy Alliance, LLC": "INL",
    "Battelle Energy Alliance": "INL",
    "Idaho State University": "ISU",
    "University of Idaho": "UI",
    "Montana State University": "MSU",
    "University of Montana": "UMT",
    "University of Nevada Las Vegas": "UNLV",
    "University of Nevada, Las Vegas": "UNLV",
    "New Mexico State University": "NMSU",
    "New Mexico Institute of Mining and Technology": "NMT",
    "University of New Mexico": "UNM",
    "Brigham Young University": "BYU",
    "University of Utah": "UU",
    "Washington State University": "WSU",
    "University of Wyoming": "UWyo",
}

# PI names guaranteed to be captured by a targeted PI-name search in Phase 3.
# Add any PI whose grants must not be missed (format: (last_name, first_name)).
PRIORITY_PIS = [
    ("Knuth", "Shelley"),
]

# Email domain → RMACC abbreviation (root domain only; subdomains are stripped before lookup)
DOMAIN_TO_ABBR = {
    # Arizona
    "asu.edu":          "ASU",
    "azwestern.edu":    "AWC",
    "nau.edu":          "NAU",
    "pima.edu":         "PCC",
    "arizona.edu":      "UA",
    "gcu.edu":          "GCU",
    # Colorado
    "colorado.edu":     "CU Boulder",
    "colostate.edu":    "CSU",
    "csupueblo.edu":    "CSUP",
    "ucdenver.edu":     "CU Denver",
    "cuanschutz.edu":   "CU Anschutz",
    "uccs.edu":         "UCCS",
    "coloradomesa.edu": "CMU",
    "mines.edu":        "Mines",
    "du.edu":           "DU",
    "msudenver.edu":    "MSUD",
    "morgancc.edu":     "MCC",
    "regis.edu":        "Regis",
    "western.edu":      "Western",
    "botanicgardens.org": "DBG",
    "nationaljewish.org": "NJH",
    # Idaho
    "boisestate.edu":   "BSU",
    "inl.gov":          "INL",
    "isu.edu":          "ISU",
    "uidaho.edu":       "UI",
    # Montana
    "montana.edu":      "MSU",
    "umt.edu":          "UMT",
    # Nevada
    "unlv.edu":         "UNLV",
    # New Mexico
    "nmsu.edu":         "NMSU",
    "nmt.edu":          "NMT",
    "unm.edu":          "UNM",
    # Utah
    "byu.edu":          "BYU",
    "utah.edu":         "UU",
    # Wyoming
    "uwyo.edu":         "UWyo",
    # Federal / research orgs
    "nrel.gov":         "NREL",
    "noaa.gov":         "NOAA",
    "usgs.gov":         "USGS",
    "ucar.edu":         "NCAR",
    # Washington
    "wsu.edu":          "WSU",
    # Northern Colorado
    "unco.edu":         "UNCO",
    # Columbia College Denver uses columbiasc.edu (parent org HQ domain)
    "columbiasc.edu":   "CC",
}

# Fields to request from the API
API_FIELDS = "id,title,awardeeName,piFirstName,piLastName,coPDPI,startDate,expDate,estimatedTotalAmt,fundProgramName,divAbbr,dirAbbr,awardeeCity,awardeeStateCode"


# ─────────────────────────────────────────────────────────────────
# DATABASE SCHEMA
# ─────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS institutions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    abbr        TEXT NOT NULL UNIQUE,
    city        TEXT,
    state       TEXT,
    lat         REAL,
    lon         REAL,
    inst_type   TEXT,
    is_rmacc    BOOLEAN DEFAULT 1,
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS grants (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    award_id        TEXT NOT NULL,
    title           TEXT,
    amount          INTEGER,
    start_date      TEXT,
    end_date        TEXT,
    pi_first_name   TEXT,
    pi_last_name    TEXT,
    co_pis          TEXT,
    agency          TEXT DEFAULT 'NSF',
    office          TEXT DEFAULT 'OAC',
    program         TEXT,
    abstract        TEXT,
    source_url      TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(award_id, agency)
);

CREATE TABLE IF NOT EXISTS grant_institutions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    grant_id        INTEGER NOT NULL REFERENCES grants(id),
    institution_id  INTEGER NOT NULL REFERENCES institutions(id),
    role            TEXT DEFAULT 'awardee',
    nsf_awardee_name TEXT,
    UNIQUE(grant_id, institution_id)
);

CREATE INDEX IF NOT EXISTS idx_grants_award_id ON grants(award_id);
CREATE INDEX IF NOT EXISTS idx_grants_agency ON grants(agency);
CREATE INDEX IF NOT EXISTS idx_grants_office ON grants(office);
CREATE INDEX IF NOT EXISTS idx_gi_grant ON grant_institutions(grant_id);
CREATE INDEX IF NOT EXISTS idx_gi_inst ON grant_institutions(institution_id);

DROP VIEW IF EXISTS v_cross_institutional;
CREATE VIEW v_cross_institutional AS
SELECT
    g.id as grant_id,
    g.award_id,
    g.title,
    g.amount,
    g.start_date,
    g.end_date,
    g.agency,
    g.office,
    g.program,
    COUNT(DISTINCT gi.institution_id) as num_institutions,
    GROUP_CONCAT(DISTINCT i.abbr) as institution_abbrs,
    GROUP_CONCAT(DISTINCT i.name) as institution_names,
    CASE
        WHEN MAX(CASE WHEN gi.role = 'collaborator' THEN 1 ELSE 0 END) = 1 THEN 'collaborative'
        ELSE 'copi'
    END as connection_type
FROM grants g
JOIN grant_institutions gi ON g.id = gi.grant_id
JOIN institutions i ON gi.institution_id = i.id
WHERE i.is_rmacc = 1
GROUP BY g.id
HAVING COUNT(DISTINCT gi.institution_id) >= 2;
"""


# ─────────────────────────────────────────────────────────────────
# NSF API CLIENT
# ─────────────────────────────────────────────────────────────────

API_BASE = "https://api.nsf.gov/services/v1/awards.json"
REQUEST_DELAY = 2.0   # seconds between paginated requests
SOCKET_TIMEOUT = 15   # seconds per recv() call — cuts off stalled connections faster than
                      # the old 30s, since the prior hang had ESTABLISHED but non-responsive socket


def _fetch_url(url):
    """Fetch a URL, returning the raw response bytes.

    SOCKET_TIMEOUT applies to each underlying recv() operation, so a server
    that stops sending data mid-response is cut off after 15 seconds of silence.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "RMACC-Grant-Collector/1.0"})
    with urllib.request.urlopen(req, timeout=SOCKET_TIMEOUT) as resp:
        return resp.read()


def query_nsf(params, max_pages=None, label=""):
    """Query the NSF Awards API with pagination and per-page progress output.

    Uses metadata.totalCount from the first response to determine the exact
    number of pages needed, so no manual max_pages cap is required.
    max_pages is kept as a safety override (None = no cap).
    label is printed as a prefix on each progress line.
    """
    import math
    all_awards = []
    offset = 1
    rpp = 25
    total_pages = max_pages  # may stay None until first response

    page = 0
    while True:
        if total_pages is not None and page >= total_pages:
            break

        params_copy = dict(params)
        params_copy["rpp"] = rpp
        params_copy["offset"] = offset
        params_copy["printFields"] = API_FIELDS

        url = f"{API_BASE}?{urllib.parse.urlencode(params_copy)}"
        try:
            raw = _fetch_url(url)
            data = json.loads(raw.decode("utf-8"))

            response = data.get("response", {})
            awards = response.get("award", [])
            all_awards.extend(awards)

            # On the first page, read totalCount to set an exact page target
            if page == 0 and total_pages is None:
                total_count = response.get("metadata", {}).get("totalCount")
                if total_count is not None:
                    total_pages = math.ceil(int(total_count) / rpp)

            page_str = f"{page+1}/{total_pages}" if total_pages else f"{page+1}/??"
            prefix = f"    {label} " if label else "    "
            print(f"{prefix}p{page_str} offset={offset} → {len(awards)} awards "
                  f"({len(all_awards)} total)", flush=True)

            if len(awards) < rpp:
                break
            offset += rpp
            page += 1
            time.sleep(REQUEST_DELAY)

        except Exception as e:
            print(f"    API error at offset {offset}: {e}", flush=True)
            break

    return all_awards


def resolve_institution(awardee_name):
    """Match an NSF awardeeName to an RMACC institution abbreviation."""
    if not awardee_name:
        return None

    if awardee_name in NAME_ALIASES:
        return NAME_ALIASES[awardee_name]

    lower = awardee_name.lower().strip()
    for alias, abbr in NAME_ALIASES.items():
        if alias.lower() == lower:
            return abbr

    for alias, abbr in NAME_ALIASES.items():
        if alias.lower() in lower or lower in alias.lower():
            return abbr

    return None


def resolve_copi_institutions(co_pis_str, awardee_abbr=None):
    """Extract RMACC institutions from co-PI email addresses.

    Parses 'Name email@domain.edu; Name2 email2@domain.edu' strings,
    strips subdomains (math.colostate.edu → colostate.edu), and maps
    root domains to RMACC abbreviations via DOMAIN_TO_ABBR.

    Returns a deduplicated list of RMACC abbreviations, excluding the
    awardee institution to prevent self-links.
    """
    if not co_pis_str:
        return []

    result = []
    seen = set()
    for domain in re.findall(r"@([\w.-]+)", co_pis_str):
        parts = domain.lower().split(".")
        if len(parts) >= 2:
            root = ".".join(parts[-2:])
            abbr = DOMAIN_TO_ABBR.get(root)
            if abbr and abbr != awardee_abbr and abbr not in seen:
                seen.add(abbr)
                result.append(abbr)
    return result


def _supplemental_search_names():
    """Build per-institution search terms from NAME_ALIASES for Phase 2 supplemental queries.

    For each RMACC institution, returns the minimal set of NSF awardee name strings
    needed to cover all known aliases via substring search. Names that are a prefix of
    another name for the same institution are dropped (the shorter one already covers them).

    Example: INL has both "Idaho National Laboratory" and "Battelle Energy Alliance"
    because NSF awards go to either legal entity. Both are included because neither is
    a prefix of the other.
    """
    raw = {}
    for nsf_name, abbr in NAME_ALIASES.items():
        raw.setdefault(abbr, []).append(nsf_name)

    result = {}
    for abbr, names in raw.items():
        sorted_names = sorted(set(names), key=len)
        minimal = []
        for name in sorted_names:
            if not any(name.lower().startswith(m.lower()) for m in minimal):
                minimal.append(name)
        result[abbr] = minimal
    return result


def _process_award(conn, cursor, inst_lookup, existing, award):
    """Process one NSF award dict: insert into DB if RMACC-relevant and not yet stored.

    Returns True if a new grant row was inserted, False otherwise.
    Does not commit — caller is responsible for conn.commit().
    """
    award_id = award.get("id", "")
    if not award_id:
        return False

    awardee_name = award.get("awardeeName", "")
    resolved = resolve_institution(awardee_name)
    if resolved is None:
        return False
    inst_id = inst_lookup.get(resolved)
    if inst_id is None:
        return False

    co_pis = award.get("coPDPI", "") or ""
    if isinstance(co_pis, list):
        co_pis = "; ".join(co_pis)

    added = False
    if award_id not in existing:
        try:
            cursor.execute("""
                INSERT INTO grants
                    (award_id, title, amount, start_date, end_date,
                     pi_first_name, pi_last_name, co_pis,
                     agency, office, program, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'NSF', 'OAC', ?,
                        'https://www.nsf.gov/awardsearch/showAward?AWD_ID=' || ?)
            """, (
                award_id,
                award.get("title", ""),
                int(award.get("estimatedTotalAmt", 0) or 0),
                award.get("startDate", ""),
                award.get("expDate", ""),
                award.get("piFirstName", ""),
                award.get("piLastName", ""),
                co_pis,
                award.get("fundProgramName", ""),
                award_id,
            ))
            existing.add(award_id)
            added = True
        except sqlite3.IntegrityError:
            pass

    grant_row = cursor.execute(
        "SELECT id FROM grants WHERE award_id = ? AND agency = 'NSF'",
        (award_id,)
    ).fetchone()

    if grant_row:
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO grant_institutions
                    (grant_id, institution_id, role, nsf_awardee_name)
                VALUES (?, ?, 'awardee', ?)
            """, (grant_row[0], inst_id, awardee_name))
        except sqlite3.IntegrityError:
            pass

        for copi_abbr in resolve_copi_institutions(co_pis, resolved):
            copi_inst_id = inst_lookup.get(copi_abbr)
            if copi_inst_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO grant_institutions "
                    "(grant_id, institution_id, role) VALUES (?, ?, 'copi_institution')",
                    (grant_row[0], copi_inst_id),
                )

    return added


# ─────────────────────────────────────────────────────────────────
# MAIN COLLECTOR
# ─────────────────────────────────────────────────────────────────

def init_db(db_path):
    """Create database and seed institutions."""
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)

    for name, abbr, city, state, lat, lon, itype in RMACC_MEMBERS:
        conn.execute("""
            INSERT OR IGNORE INTO institutions (name, abbr, city, state, lat, lon, inst_type, is_rmacc)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)
        """, (name, abbr, city, state, lat, lon, itype))

    conn.commit()
    return conn


def collect_grants(conn, window_start="05/01/2025", window_end="04/30/2026"):
    """Fetch NSF OAC grants active in [window_start, window_end] using three-phase strategy.

    window_start (MM/DD/YYYY) — maps to API expDateStart: only grants that haven't
                                expired before this date (i.e. still active by window open).
    window_end   (MM/DD/YYYY) — maps to API startDateEnd: only grants that started
                                by this date (i.e. already underway before window closes).

    No startDateStart restriction — catches long-running grants from any vintage that
    are still active in the window (e.g. 5-year awards that began before 2021).

    Phase 1 — global OAC query: efficient broad sweep; catches most grants but NSF
              pagination is non-deterministic at scale so some awards may slip through.
    Phase 2 — state-based supplemental: one query per RMACC state; small stable result
              sets catch anything Phase 1 missed.
    Phase 3 — priority PI name searches: guarantees specific individuals' grants are
              captured even if their institution's awardee name isn't recognized.

    INSERT OR IGNORE on grants + the `existing` set prevent duplicates across phases.
    """
    cursor = conn.cursor()

    cursor.execute("SELECT id, abbr FROM institutions")
    inst_lookup = {row[1]: row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT award_id FROM grants WHERE agency = 'NSF'")
    existing = {row[0] for row in cursor.fetchall()}
    print(f"  Database already has {len(existing)} NSF awards\n")

    BASE_PARAMS = {
        "org_code_div": "05090000",
        "expDateStart": window_start,   # exclude grants that expired before window opens
        "startDateEnd": window_end,     # exclude grants that haven't started by window closes
    }
    total_new = 0

    # ── Phase 1: Global OAC query ──────────────────────────────────
    print(f"  Phase 1: Global OAC query — active {window_start}–{window_end}...", flush=True)
    awards = query_nsf(BASE_PARAMS, label="[P1]")
    print(f"  Retrieved {len(awards)} OAC awards. Processing...", flush=True)
    phase1_new = 0
    for award in awards:
        if _process_award(conn, cursor, inst_lookup, existing, award):
            phase1_new += 1
            print(f"  [NEW] {award.get('awardeeName','')[:28]} — {award.get('title','')[:50]}",
                  flush=True)
    conn.commit()
    print(f"  Phase 1 complete: {phase1_new} new grants\n", flush=True)
    total_new += phase1_new

    # ── Phase 2: State-based supplemental ────────────────────────
    # awardeeName does word-level OR matching and returns ~1,272/1,407 results for any
    # "University" query — useless as a filter. awardeeStateCode is a real filter: each
    # RMACC state returns a manageable set of grants that paginates stably.
    rmacc_states = sorted({m[3] for m in RMACC_MEMBERS})
    print(f"  Phase 2: State-based supplemental ({len(rmacc_states)} RMACC states)...",
          flush=True)
    phase2_new = 0
    for idx, state in enumerate(rmacc_states, 1):
        print(f"  [{idx}/{len(rmacc_states)}] State: {state}", flush=True)
        state_awards = query_nsf({**BASE_PARAMS, "awardeeStateCode": state},
                                 label=f"[P2:{state}]")
        new_here = 0
        for award in state_awards:
            if _process_award(conn, cursor, inst_lookup, existing, award):
                new_here += 1
                phase2_new += 1
                print(f"    [NEW:{state}] {award.get('title','')[:65]}", flush=True)
        print(f"    → {len(state_awards)} awards returned, {new_here} new", flush=True)
        conn.commit()
    print(f"  Phase 2 complete: {phase2_new} additional grants\n", flush=True)
    total_new += phase2_new

    # ── Phase 3: Priority PI name searches ────────────────────────
    print(f"  Phase 3: Priority PI searches ({len(PRIORITY_PIS)} PIs)...", flush=True)
    phase3_new = 0
    for pi_last, pi_first in PRIORITY_PIS:
        print(f"  Searching PI: {pi_first} {pi_last}", flush=True)
        pi_awards = query_nsf({**BASE_PARAMS, "piLastName": pi_last, "piFirstName": pi_first},
                              label=f"[P3:{pi_last}]")
        new_here = 0
        for award in pi_awards:
            if _process_award(conn, cursor, inst_lookup, existing, award):
                new_here += 1
                phase3_new += 1
                print(f"    [NEW:{pi_first} {pi_last}] {award.get('title','')[:60]}", flush=True)
        print(f"    → {len(pi_awards)} awards returned, {new_here} new", flush=True)
    conn.commit()
    print(f"  Phase 3 complete: {phase3_new} additional grants\n", flush=True)
    total_new += phase3_new

    print(f"\n  Done! {total_new} total new RMACC OAC awards added.")

    cursor.execute("SELECT COUNT(*) FROM grants WHERE agency = 'NSF'")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM v_cross_institutional")
    cross = cursor.fetchone()[0]
    print(f"  Database totals: {total} grants, {cross} cross-institutional collaborations")


def identify_collaborations(conn):
    """Find collaborative grants shared across RMACC institutions."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT g.id, g.award_id, g.title, gi.institution_id
        FROM grants g
        JOIN grant_institutions gi ON g.id = gi.grant_id
        WHERE g.title LIKE 'Collaborative Research:%'
        ORDER BY g.title
    """)

    collab_rows = cursor.fetchall()

    from collections import defaultdict
    title_groups = defaultdict(list)
    for grant_id, award_id, title, inst_id in collab_rows:
        key = title.split(":", 1)[-1].strip().lower()[:80]
        title_groups[key].append((grant_id, award_id, inst_id))

    linked = 0
    for key, group in title_groups.items():
        if len(group) < 2:
            continue
        all_inst_ids = set(inst_id for _, _, inst_id in group)
        for grant_id, award_id, _ in group:
            for inst_id in all_inst_ids:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO grant_institutions
                            (grant_id, institution_id, role)
                        VALUES (?, ?, 'collaborator')
                    """, (grant_id, inst_id))
                    linked += 1
                except sqlite3.IntegrityError:
                    pass

    conn.commit()
    print(f"  Linked {linked} collaborative grant-institution pairs")


def export_json(conn, output_path, window_start=None, window_end=None):
    """Export database to JSON for the visualization."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, name, abbr, city, state, lat, lon, inst_type, is_rmacc
        FROM institutions WHERE is_rmacc = 1
    """)
    institutions = [
        {"id": r[0], "name": r[1], "abbr": r[2], "city": r[3], "state": r[4],
         "lat": r[5], "lon": r[6], "type": r[7], "is_rmacc": bool(r[8])}
        for r in cursor.fetchall()
    ]

    cursor.execute("""
        SELECT g.id, g.award_id, g.title, g.amount, g.start_date, g.end_date,
               g.pi_first_name, g.pi_last_name, g.co_pis,
               g.agency, g.office, g.program, g.source_url
        FROM grants g ORDER BY g.start_date DESC
    """)
    grants = []
    for row in cursor.fetchall():
        grant_id = row[0]
        cursor2 = conn.cursor()
        cursor2.execute("""
            SELECT i.abbr, i.name, gi.role
            FROM grant_institutions gi
            JOIN institutions i ON gi.institution_id = i.id
            WHERE gi.grant_id = ?
        """, (grant_id,))
        inst_links = [{"abbr": r[0], "name": r[1], "role": r[2]} for r in cursor2.fetchall()]

        grants.append({
            "award_id": row[1], "title": row[2], "amount": row[3],
            "start_date": row[4], "end_date": row[5],
            "pi": f"{row[6] or ''} {row[7] or ''}".strip(),
            "co_pis": row[8], "agency": row[9], "office": row[10],
            "program": row[11], "source_url": row[12],
            "institutions": inst_links,
        })

    cursor.execute("""
        SELECT award_id, title, amount, start_date, end_date,
               program, institution_abbrs, num_institutions, connection_type
        FROM v_cross_institutional ORDER BY amount DESC
    """)
    cross_institutional = [
        {"award_id": r[0], "title": r[1], "amount": r[2], "start_date": r[3],
         "end_date": r[4], "program": r[5],
         "institutions": r[6].split(",") if r[6] else [], "num_institutions": r[7],
         "connection_type": r[8]}
        for r in cursor.fetchall()
    ]

    output = {
        "generated_at": datetime.now().isoformat(),
        "institutions": institutions,
        "grants": grants,
        "cross_institutional": cross_institutional,
        "summary": {
            "total_institutions": len(institutions),
            "total_grants": len(grants),
            "cross_institutional_count": len(cross_institutional),
            "total_funding": sum(g["amount"] for g in grants if g["amount"]),
            "window_start": window_start,
            "window_end": window_end,
        }
    }

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n  Exported {len(grants)} grants and {len(cross_institutional)} "
          f"cross-institutional collaborations to {output_path}")
    return output


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Collect NSF OAC grants for RMACC institutions")
    parser.add_argument("--db", default="rmacc_grants.db", help="SQLite database path")
    parser.add_argument("--export", action="store_true", help="Export JSON after collection")
    parser.add_argument("--export-only", action="store_true", help="Skip collection, just export")
    parser.add_argument("--json", default="rmacc_grants.json", help="JSON export path")
    parser.add_argument("--window-start", default="05/01/2025", metavar="MM/DD/YYYY",
                        help="Window open date — only grants active on or after this date "
                             "(maps to API expDateStart; default: 05/01/2025)")
    parser.add_argument("--window-end", default="04/30/2026", metavar="MM/DD/YYYY",
                        help="Window close date — only grants that started by this date "
                             "(maps to API startDateEnd; default: 04/30/2026)")
    args = parser.parse_args()

    db_path = Path(args.db)
    json_path = Path(args.json)

    print("=" * 60)
    print("RMACC NSF OAC Grant Collector")
    print("=" * 60)
    print(f"  Window: {args.window_start} → {args.window_end}")

    print(f"\n1. Initializing database: {db_path}")
    conn = init_db(str(db_path))

    if not args.export_only:
        print(f"\n2. Collecting grants from NSF Awards API...")
        collect_grants(conn, window_start=args.window_start, window_end=args.window_end)

        print(f"\n3. Identifying collaborative grant networks...")
        identify_collaborations(conn)

    if args.export or args.export_only:
        print(f"\n4. Exporting to JSON: {json_path}")
        export_json(conn, str(json_path),
                    window_start=args.window_start, window_end=args.window_end)

    conn.close()
    print(f"\nDone! Database saved to: {db_path}")


if __name__ == "__main__":
    main()
