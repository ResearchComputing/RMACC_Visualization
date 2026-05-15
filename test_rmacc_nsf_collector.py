#!/usr/bin/env python3
"""
Tests for rmacc_nsf_collector.py
Run: python3 -m unittest test_rmacc_nsf_collector -v
"""

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from rmacc_nsf_collector import (
    NAME_ALIASES,
    PRIORITY_PIS,
    RMACC_MEMBERS,
    _process_award,
    _supplemental_search_names,
    export_json,
    identify_collaborations,
    init_db,
    collect_grants,
    resolve_institution,
    resolve_copi_institutions,
)


# ─────────────────────────────────────────────────────────────────
# resolve_institution
# ─────────────────────────────────────────────────────────────────

class TestResolveInstitution(unittest.TestCase):
    def test_exact_match(self):
        self.assertEqual(resolve_institution("Arizona State University"), "ASU")

    def test_exact_match_variant(self):
        self.assertEqual(resolve_institution("University of Colorado at Boulder"), "CU Boulder")

    def test_case_insensitive(self):
        self.assertEqual(resolve_institution("arizona state university"), "ASU")

    def test_nrel_primary_alias(self):
        self.assertEqual(resolve_institution("Alliance for Sustainable Energy"), "NREL")

    def test_nrel_llc_variant(self):
        # Partial match: "Alliance for Sustainable Energy" is in "Alliance for Sustainable Energy, LLC"
        self.assertEqual(resolve_institution("Alliance for Sustainable Energy, LLC"), "NREL")

    def test_inl_lab_name(self):
        self.assertEqual(resolve_institution("Idaho National Laboratory"), "INL")

    def test_inl_battelle_variant(self):
        self.assertEqual(resolve_institution("Battelle Energy Alliance, LLC"), "INL")

    def test_pcc_district_name(self):
        self.assertEqual(resolve_institution("Pima County Community College District"), "PCC")

    def test_unknown_returns_none(self):
        self.assertIsNone(resolve_institution("Saint Louis University"))

    def test_empty_string_returns_none(self):
        self.assertIsNone(resolve_institution(""))

    def test_none_input_returns_none(self):
        self.assertIsNone(resolve_institution(None))

    def test_ucar_alias(self):
        self.assertEqual(resolve_institution("UCAR"), "NCAR")


# ─────────────────────────────────────────────────────────────────
# init_db
# ─────────────────────────────────────────────────────────────────

class TestInitDb(unittest.TestCase):
    def setUp(self):
        self.conn = init_db(":memory:")

    def tearDown(self):
        self.conn.close()

    def _table_exists(self, name):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
        )
        return cur.fetchone() is not None

    def _view_exists(self, name):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name=?", (name,)
        )
        return cur.fetchone() is not None

    def test_institutions_table_exists(self):
        self.assertTrue(self._table_exists("institutions"))

    def test_grants_table_exists(self):
        self.assertTrue(self._table_exists("grants"))

    def test_grant_institutions_table_exists(self):
        self.assertTrue(self._table_exists("grant_institutions"))

    def test_cross_institutional_view_exists(self):
        self.assertTrue(self._view_exists("v_cross_institutional"))

    def test_all_rmacc_members_seeded(self):
        cur = self.conn.execute("SELECT COUNT(*) FROM institutions WHERE is_rmacc = 1")
        self.assertEqual(cur.fetchone()[0], len(RMACC_MEMBERS))

    def test_institution_abbr_unique_constraint(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                "INSERT INTO institutions (name, abbr) VALUES ('Duplicate', 'ASU')"
            )

    def test_columbia_college_is_denver_co(self):
        """Columbia College in the dataset is the Denver campus, not Missouri."""
        cur = self.conn.execute("SELECT state, city FROM institutions WHERE abbr = 'CC'")
        state, city = cur.fetchone()
        self.assertEqual(state, "CO")
        self.assertEqual(city, "Denver")

    def test_init_is_idempotent(self):
        """Calling init_db twice on the same DB should not error or duplicate rows."""
        init_db(":memory:")  # second init on fresh conn — schema uses IF NOT EXISTS
        cur = self.conn.execute("SELECT COUNT(*) FROM institutions")
        self.assertEqual(cur.fetchone()[0], len(RMACC_MEMBERS))


# ─────────────────────────────────────────────────────────────────
# collect_grants  (API mocked)
# ─────────────────────────────────────────────────────────────────

def _make_urlopen_mock(responses, total_count=None):
    """Return a side_effect function that yields successive API payloads.

    If total_count is given, the first response includes metadata.totalCount
    so query_nsf can compute the exact page count dynamically.
    If omitted, query_nsf falls back to the early-exit (len < rpp) condition.
    """
    call_iter = iter(responses)
    first_call = [True]

    def fake_urlopen(req, timeout=30):
        try:
            awards = next(call_iter)
        except StopIteration:
            awards = []
        response = {"award": awards}
        if first_call[0] and total_count is not None:
            response["metadata"] = {"totalCount": total_count}
        first_call[0] = False
        body = json.dumps({"response": response}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    return fake_urlopen


def _make_award(award_id, awardee_name, title="HPC Research Grant", div_abbr="OAC"):
    return {
        "id": award_id,
        "awardeeName": awardee_name,
        "title": title,
        "estimatedTotalAmt": "500000",
        "startDate": "09/01/2023",
        "expDate": "08/31/2026",
        "piFirstName": "Test",
        "piLastName": "PI",
        "coPDPI": "",
        "fundProgramName": "OAC Core",
        "divAbbr": div_abbr,
    }


# collect_grants makes one paginated query; without totalCount in mock, it exits
# via early-exit (len < rpp). Pad so the while loop always terminates cleanly.
_MAX_API_CALLS = 10


class TestCollectGrants(unittest.TestCase):
    def setUp(self):
        self.conn = init_db(":memory:")

    def tearDown(self):
        self.conn.close()

    def _run_collect(self, first_responses):
        """Patch urlopen; first_responses are returned in order, then empty pages."""
        padding = [[] for _ in range(_MAX_API_CALLS)]
        responses = list(first_responses) + padding
        with patch("urllib.request.urlopen", side_effect=_make_urlopen_mock(responses)):
            with patch("time.sleep"):
                collect_grants(self.conn)

    def _institution_abbrs_for(self, award_id):
        cur = self.conn.execute("""
            SELECT i.abbr
            FROM grant_institutions gi
            JOIN grants g ON gi.grant_id = g.id
            JOIN institutions i ON gi.institution_id = i.id
            WHERE g.award_id = ?
        """, (award_id,))
        return [r[0] for r in cur.fetchall()]

    # ── red test: exposes the fallback bug ──────────────────────────

    def test_non_rmacc_awardee_not_linked_to_searched_institution(self):
        """
        When the NSF API returns an award from a non-RMACC institution (e.g. Saint
        Louis University), that award must NOT be linked to whichever RMACC institution
        triggered the search.  The current fallback `inst_lookup.get(resolved, inst_id)`
        incorrectly links it to the searched institution when resolved is None.
        """
        award = _make_award("BAD001", "Saint Louis University")
        self._run_collect([[award]])

        abbrs = self._institution_abbrs_for("BAD001")
        self.assertEqual(abbrs, [], "Non-RMACC award should have zero institution links")

    # ── green tests ─────────────────────────────────────────────────

    def test_rmacc_award_linked_to_correct_institution(self):
        """Award whose awardee resolves to an RMACC member is linked to that member."""
        award = _make_award("GOOD001", "Arizona State University")
        self._run_collect([[award]])

        abbrs = self._institution_abbrs_for("GOOD001")
        self.assertIn("ASU", abbrs)

    def test_alias_awardee_name_resolves_correctly(self):
        """Award with a known NAME_ALIASES variant resolves to the right institution."""
        award = _make_award("GOOD002", "Alliance for Sustainable Energy")
        self._run_collect([[award]])

        abbrs = self._institution_abbrs_for("GOOD002")
        self.assertIn("NREL", abbrs)

    def test_duplicate_award_stored_only_once(self):
        """Same award_id appearing in multiple API responses is only inserted once."""
        award = _make_award("DUP001", "Colorado State University")
        self._run_collect([[award], [award]])  # returned on first two API calls

        cur = self.conn.execute("SELECT COUNT(*) FROM grants WHERE award_id = 'DUP001'")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_zero_amount_handled(self):
        """estimatedTotalAmt='' or missing does not crash the insert."""
        award = _make_award("AMT001", "University of Utah")
        award["estimatedTotalAmt"] = ""
        self._run_collect([[award]])

        cur = self.conn.execute("SELECT amount FROM grants WHERE award_id = 'AMT001'")
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 0)

    def test_copi_as_list_stored_as_string(self):
        """NSF returns coPDPI as a list when there are co-PIs; must be serialized before insert."""
        award = _make_award("CPI001", "Arizona State University")
        award["coPDPI"] = ["Jane Smith jane@asu.edu", "Bob Jones bob@asu.edu"]
        self._run_collect([[award]])

        cur = self.conn.execute("SELECT co_pis FROM grants WHERE award_id = 'CPI001'")
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertIsInstance(row[0], str)
        self.assertIn("Jane Smith", row[0])

    def test_oac_award_stored(self):
        """RMACC award from the OAC global query is stored and linked correctly."""
        award = _make_award("OAC001", "Arizona State University")
        self._run_collect([[award]])

        cur = self.conn.execute("SELECT COUNT(*) FROM grants WHERE award_id = 'OAC001'")
        self.assertEqual(cur.fetchone()[0], 1)


# ─────────────────────────────────────────────────────────────────
# identify_collaborations
# ─────────────────────────────────────────────────────────────────

class TestIdentifyCollaborations(unittest.TestCase):
    def setUp(self):
        self.conn = init_db(":memory:")
        cur = self.conn.cursor()
        cur.execute("SELECT id, abbr FROM institutions")
        self.inst_lookup = {row[1]: row[0] for row in cur.fetchall()}

    def tearDown(self):
        self.conn.close()

    def _insert_grant(self, award_id, title, abbr):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO grants (award_id, title, amount, agency) VALUES (?, ?, 0, 'NSF')",
            (award_id, title),
        )
        grant_id = cur.lastrowid
        cur.execute(
            "INSERT INTO grant_institutions (grant_id, institution_id, role) VALUES (?, ?, 'awardee')",
            (grant_id, self.inst_lookup[abbr]),
        )
        self.conn.commit()
        return grant_id

    def _inst_count_for(self, award_id):
        cur = self.conn.execute("""
            SELECT COUNT(DISTINCT gi.institution_id)
            FROM grant_institutions gi
            JOIN grants g ON gi.grant_id = g.id
            WHERE g.award_id = ?
        """, (award_id,))
        return cur.fetchone()[0]

    def test_two_way_collaborative_research_linked(self):
        self._insert_grant("C001", "Collaborative Research: Shared HPC Platform", "CU Boulder")
        self._insert_grant("C002", "Collaborative Research: Shared HPC Platform", "CSU")

        identify_collaborations(self.conn)

        self.assertEqual(self._inst_count_for("C001"), 2)
        self.assertEqual(self._inst_count_for("C002"), 2)

    def test_three_way_collaboration_fully_linked(self):
        self._insert_grant("D001", "Collaborative Research: Tri-Campus Grid", "CU Boulder")
        self._insert_grant("D002", "Collaborative Research: Tri-Campus Grid", "CSU")
        self._insert_grant("D003", "Collaborative Research: Tri-Campus Grid", "ASU")

        identify_collaborations(self.conn)

        for award_id in ("D001", "D002", "D003"):
            self.assertEqual(self._inst_count_for(award_id), 3,
                             f"{award_id} should link to all 3 institutions")

    def test_non_collaborative_title_not_cross_linked(self):
        """Matching titles without 'Collaborative Research:' prefix are not linked."""
        self._insert_grant("E001", "HPC Systems for Research", "CU Boulder")
        self._insert_grant("E002", "HPC Systems for Research", "CSU")

        identify_collaborations(self.conn)

        self.assertEqual(self._inst_count_for("E001"), 1)
        self.assertEqual(self._inst_count_for("E002"), 1)

    def test_lone_collaborative_grant_unchanged(self):
        """A single 'Collaborative Research:' grant with no partner stays as-is."""
        self._insert_grant("F001", "Collaborative Research: Solo Project", "ASU")

        identify_collaborations(self.conn)

        self.assertEqual(self._inst_count_for("F001"), 1)

    def test_cross_linking_is_symmetric(self):
        """If A is linked to B's grant, B is also linked to A's grant."""
        self._insert_grant("G001", "Collaborative Research: Symmetric Test", "UU")
        self._insert_grant("G002", "Collaborative Research: Symmetric Test", "BYU")

        identify_collaborations(self.conn)

        uu_abbrs = self._abbrs_for("G001")
        byu_abbrs = self._abbrs_for("G002")
        self.assertIn("BYU", uu_abbrs)
        self.assertIn("UU", byu_abbrs)

    def _abbrs_for(self, award_id):
        cur = self.conn.execute("""
            SELECT i.abbr FROM grant_institutions gi
            JOIN grants g ON gi.grant_id = g.id
            JOIN institutions i ON gi.institution_id = i.id
            WHERE g.award_id = ?
        """, (award_id,))
        return [r[0] for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────
# export_json
# ─────────────────────────────────────────────────────────────────

class TestExportJson(unittest.TestCase):
    def setUp(self):
        self.conn = init_db(":memory:")
        cur = self.conn.cursor()
        cur.execute("SELECT id, abbr FROM institutions")
        inst_lookup = {row[1]: row[0] for row in cur.fetchall()}

        # Grant 1: single institution
        cur.execute(
            "INSERT INTO grants (award_id, title, amount, start_date, end_date, agency) "
            "VALUES ('X001', 'Solo Grant', 100000, '2023-01-01', '2025-12-31', 'NSF')"
        )
        g1 = cur.lastrowid
        cur.execute(
            "INSERT INTO grant_institutions (grant_id, institution_id, role) VALUES (?, ?, 'awardee')",
            (g1, inst_lookup["ASU"]),
        )

        # Grant 2: two institutions (cross-institutional)
        cur.execute(
            "INSERT INTO grants (award_id, title, amount, start_date, end_date, agency) "
            "VALUES ('X002', 'Multi Grant', 500000, '2022-06-01', '2026-05-31', 'NSF')"
        )
        g2 = cur.lastrowid
        cur.execute(
            "INSERT INTO grant_institutions (grant_id, institution_id, role) VALUES (?, ?, 'awardee')",
            (g2, inst_lookup["CU Boulder"]),
        )
        cur.execute(
            "INSERT INTO grant_institutions (grant_id, institution_id, role) VALUES (?, ?, 'collaborator')",
            (g2, inst_lookup["CSU"]),
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _export(self):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        try:
            return export_json(self.conn, path)
        finally:
            os.unlink(path)

    def test_required_top_level_keys_present(self):
        out = self._export()
        for key in ("generated_at", "institutions", "grants", "cross_institutional", "summary"):
            self.assertIn(key, out)

    def test_summary_institution_count(self):
        out = self._export()
        self.assertEqual(out["summary"]["total_institutions"], len(RMACC_MEMBERS))

    def test_summary_grant_count(self):
        out = self._export()
        self.assertEqual(out["summary"]["total_grants"], 2)

    def test_summary_cross_institutional_count(self):
        out = self._export()
        self.assertEqual(out["summary"]["cross_institutional_count"], 1)

    def test_summary_total_funding(self):
        out = self._export()
        self.assertEqual(out["summary"]["total_funding"], 600000)

    def test_cross_institutional_excludes_solo_grant(self):
        out = self._export()
        award_ids = [g["award_id"] for g in out["cross_institutional"]]
        self.assertNotIn("X001", award_ids)

    def test_cross_institutional_includes_multi_grant(self):
        out = self._export()
        award_ids = [g["award_id"] for g in out["cross_institutional"]]
        self.assertIn("X002", award_ids)

    def test_grant_institutions_list_populated(self):
        out = self._export()
        multi = next(g for g in out["grants"] if g["award_id"] == "X002")
        abbrs = [i["abbr"] for i in multi["institutions"]]
        self.assertIn("CU Boulder", abbrs)
        self.assertIn("CSU", abbrs)

    def test_grant_pi_field_assembled(self):
        out = self._export()
        for grant in out["grants"]:
            self.assertIn("pi", grant)

    def test_cross_institutional_has_connection_type(self):
        """Each cross-institutional entry must carry a connection_type field."""
        out = self._export()
        entry = next(g for g in out["cross_institutional"] if g["award_id"] == "X002")
        self.assertIn("connection_type", entry)

    def test_collaborator_role_yields_collaborative_connection_type(self):
        """A grant linked via 'collaborator' role should be typed as 'collaborative'."""
        out = self._export()
        entry = next(g for g in out["cross_institutional"] if g["award_id"] == "X002")
        self.assertEqual(entry["connection_type"], "collaborative")

    def test_copi_role_yields_copi_connection_type(self):
        """A grant linked via 'copi_institution' role should be typed as 'copi'."""
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO grants (award_id, title, amount, agency) "
            "VALUES ('X003', 'Co-PI Grant', 200000, 'NSF')"
        )
        g3 = cur.lastrowid
        cur.execute("SELECT id FROM institutions WHERE abbr = 'UU'")
        uu_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM institutions WHERE abbr = 'BYU'")
        byu_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO grant_institutions (grant_id, institution_id, role) VALUES (?, ?, 'awardee')",
            (g3, uu_id),
        )
        cur.execute(
            "INSERT INTO grant_institutions (grant_id, institution_id, role) VALUES (?, ?, 'copi_institution')",
            (g3, byu_id),
        )
        self.conn.commit()

        out = self._export()
        entry = next((g for g in out["cross_institutional"] if g["award_id"] == "X003"), None)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["connection_type"], "copi")


# ─────────────────────────────────────────────────────────────────
# resolve_copi_institutions
# ─────────────────────────────────────────────────────────────────

class TestResolveCopiInstitutions(unittest.TestCase):
    def test_empty_string_returns_empty(self):
        self.assertEqual(resolve_copi_institutions(""), [])

    def test_none_returns_empty(self):
        self.assertEqual(resolve_copi_institutions(None), [])

    def test_non_rmacc_domain_ignored(self):
        self.assertEqual(resolve_copi_institutions("Joel S jsharbro@ucsb.edu"), [])

    def test_gmail_ignored(self):
        self.assertEqual(resolve_copi_institutions("Jarrod S jarrod@gmail.com"), [])

    def test_rmacc_domain_detected(self):
        result = resolve_copi_institutions("Someone s@colostate.edu")
        self.assertIn("CSU", result)

    def test_subdomain_stripped(self):
        """math.colostate.edu should resolve to CSU the same as colostate.edu."""
        result = resolve_copi_institutions("Someone s@math.colostate.edu")
        self.assertIn("CSU", result)

    def test_awardee_excluded(self):
        """Co-PI from the same institution as the awardee is not returned."""
        result = resolve_copi_institutions("Someone s@colostate.edu", awardee_abbr="CSU")
        self.assertNotIn("CSU", result)

    def test_multiple_copis_different_institutions(self):
        co_pis = "Jan M jan@ucdenver.edu; Mitchell M m@unco.edu; Joel S jsharbro@ucsb.edu"
        result = resolve_copi_institutions(co_pis)
        self.assertIn("CU Denver", result)
        self.assertIn("UNCO", result)
        self.assertNotIn("ucsb.edu", result)

    def test_no_duplicate_institutions(self):
        """Two co-PIs from the same institution yield only one entry."""
        co_pis = "A a@colostate.edu; B b@colostate.edu"
        result = resolve_copi_institutions(co_pis)
        self.assertEqual(result.count("CSU"), 1)

    def test_colorado_edu_resolves_to_cu_boulder(self):
        result = resolve_copi_institutions("Co-PI name@colorado.edu")
        self.assertIn("CU Boulder", result)


class TestCollectGrantsCopiDetection(unittest.TestCase):
    """Tests that co-PI institution links are created during collection."""

    def setUp(self):
        self.conn = init_db(":memory:")

    def tearDown(self):
        self.conn.close()

    def _run_collect(self, first_responses):
        padding = [[] for _ in range(_MAX_API_CALLS)]
        responses = list(first_responses) + padding
        with patch("urllib.request.urlopen", side_effect=_make_urlopen_mock(responses)):
            with patch("time.sleep"):
                collect_grants(self.conn)

    def _roles_for(self, award_id):
        cur = self.conn.execute("""
            SELECT i.abbr, gi.role
            FROM grant_institutions gi
            JOIN grants g ON gi.grant_id = g.id
            JOIN institutions i ON gi.institution_id = i.id
            WHERE g.award_id = ?
        """, (award_id,))
        return {r[0]: r[1] for r in cur.fetchall()}

    def test_copi_institution_linked_with_copi_role(self):
        """When a grant has a co-PI from another RMACC institution, that link uses role='copi_institution'."""
        award = _make_award("COPI001", "Colorado State University")
        award["coPDPI"] = "Shelley Knuth shelley.knuth@colorado.edu"
        self._run_collect([[award]])

        roles = self._roles_for("COPI001")
        self.assertIn("CSU", roles)
        self.assertEqual(roles["CSU"], "awardee")
        self.assertIn("CU Boulder", roles)
        self.assertEqual(roles["CU Boulder"], "copi_institution")

    def test_copi_link_makes_grant_cross_institutional(self):
        """A grant with an RMACC co-PI should appear in v_cross_institutional."""
        award = _make_award("COPI002", "Colorado State University")
        award["coPDPI"] = "Someone name@colorado.edu"
        self._run_collect([[award]])

        cur = self.conn.execute(
            "SELECT award_id FROM v_cross_institutional WHERE award_id = 'COPI002'"
        )
        self.assertIsNotNone(cur.fetchone())

    def test_non_rmacc_copi_domain_not_linked(self):
        """A co-PI from a non-RMACC institution (e.g. UCSB) creates no extra link."""
        award = _make_award("COPI003", "Arizona State University")
        award["coPDPI"] = "Joel S jsharbro@ucsb.edu"
        self._run_collect([[award]])

        roles = self._roles_for("COPI003")
        self.assertEqual(list(roles.keys()), ["ASU"])


# ─────────────────────────────────────────────────────────────────
# _supplemental_search_names
# ─────────────────────────────────────────────────────────────────

class TestSupplementalSearchNames(unittest.TestCase):
    def test_inl_has_both_search_terms(self):
        """INL must search both 'Idaho National Laboratory' and 'Battelle Energy Alliance'."""
        names = _supplemental_search_names()
        inl = names.get("INL", [])
        self.assertTrue(any("Idaho National" in n for n in inl), f"INL missing Idaho National: {inl}")
        self.assertTrue(any("Battelle" in n for n in inl), f"INL missing Battelle: {inl}")

    def test_cu_boulder_has_at_least_one_term(self):
        names = _supplemental_search_names()
        self.assertGreater(len(names.get("CU Boulder", [])), 0)

    def test_institutions_in_aliases_have_search_terms(self):
        """Every RMACC institution that appears in NAME_ALIASES gets at least one search term."""
        names = _supplemental_search_names()
        abbrs_in_aliases = set(NAME_ALIASES.values())
        for abbr in abbrs_in_aliases:
            self.assertGreater(len(names.get(abbr, [])), 0, f"{abbr} has no search terms")

    def test_no_prefix_duplicates(self):
        """No search term for an institution should be a prefix of another term for the same institution."""
        names = _supplemental_search_names()
        for abbr, terms in names.items():
            for i, t1 in enumerate(terms):
                for j, t2 in enumerate(terms):
                    if i != j:
                        self.assertFalse(
                            t2.lower().startswith(t1.lower()),
                            f"{abbr}: '{t1}' is a prefix of '{t2}'"
                        )


# ─────────────────────────────────────────────────────────────────
# _process_award
# ─────────────────────────────────────────────────────────────────

class TestProcessAward(unittest.TestCase):
    def setUp(self):
        self.conn = init_db(":memory:")
        cur = self.conn.cursor()
        cur.execute("SELECT id, abbr FROM institutions")
        self.inst_lookup = {row[1]: row[0] for row in cur.fetchall()}
        self.existing = set()
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def _roles_for(self, award_id):
        cur = self.conn.execute("""
            SELECT i.abbr, gi.role FROM grant_institutions gi
            JOIN grants g ON gi.grant_id = g.id
            JOIN institutions i ON gi.institution_id = i.id
            WHERE g.award_id = ?
        """, (award_id,))
        return {r[0]: r[1] for r in cur.fetchall()}

    def test_rmacc_award_returns_true(self):
        award = _make_award("PA001", "Arizona State University")
        added = _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.assertTrue(added)

    def test_non_rmacc_award_returns_false(self):
        award = _make_award("PA002", "Saint Louis University")
        added = _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.assertFalse(added)

    def test_non_rmacc_award_not_inserted(self):
        award = _make_award("PA002B", "Saint Louis University")
        _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.conn.commit()
        cur = self.conn.execute("SELECT COUNT(*) FROM grants WHERE award_id = 'PA002B'")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_duplicate_award_returns_false(self):
        award = _make_award("PA003", "Arizona State University")
        _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.conn.commit()
        added = _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.assertFalse(added)

    def test_rmacc_award_gets_awardee_link(self):
        award = _make_award("PA004", "Colorado State University")
        _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.conn.commit()
        roles = self._roles_for("PA004")
        self.assertIn("CSU", roles)
        self.assertEqual(roles["CSU"], "awardee")

    def test_copi_link_created_for_rmacc_copi(self):
        award = _make_award("PA005", "Colorado State University")
        award["coPDPI"] = "Shelley Knuth shelley.knuth@colorado.edu"
        _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.conn.commit()
        roles = self._roles_for("PA005")
        self.assertIn("CU Boulder", roles)
        self.assertEqual(roles["CU Boulder"], "copi_institution")

    def test_priority_pi_linked_as_copi_by_name(self):
        """When a priority PI's last name appears in co_pis, their institution is linked even if email doesn't resolve."""
        award = _make_award("PP001", "Colorado State University")
        award["coPDPI"] = "Shelley Knuth shelley.knuth@someunresolvable.org"
        _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.conn.commit()
        roles = self._roles_for("PP001")
        self.assertIn("CU Boulder", roles, "CU Boulder should be linked via Knuth name match")
        self.assertEqual(roles["CU Boulder"], "copi_institution")

    def test_priority_pi_name_match_skipped_when_awardee(self):
        """Name-based match should not self-link when priority PI's institution is the awardee."""
        award = _make_award("PP002", "University of Colorado Boulder")
        award["coPDPI"] = "Shelley Knuth shelley.knuth@someunresolvable.org"
        _process_award(self.conn, self.cursor, self.inst_lookup, self.existing, award)
        self.conn.commit()
        roles = self._roles_for("PP002")
        self.assertEqual(roles.get("CU Boulder"), "awardee", "CU Boulder should be awardee only, not also copi")
        cub_entries = [r for abbr, r in roles.items() if abbr == "CU Boulder"]
        self.assertEqual(len(cub_entries), 1, "CU Boulder should appear exactly once")


# ─────────────────────────────────────────────────────────────────
# Supplemental collection passes
# ─────────────────────────────────────────────────────────────────

def _smart_urlopen(phase1_awards=None, phase2_awards=None, phase3_awards=None):
    """Return a urlopen mock that routes by URL content to simulate three-phase collection.

    phase1_awards — returned for global OAC query (no awardeeStateCode, no piLastName)
    phase2_awards — dict of {state_code: [awards]} for per-state queries
    phase3_awards — returned for any piLastName query
    """
    phase1_awards = phase1_awards or []
    phase2_awards = phase2_awards or {}
    phase3_awards = phase3_awards or []

    def fake_urlopen(req, timeout=None):
        url = req.get_full_url()
        if "piLastName=" in url:
            awards = phase3_awards
        elif "awardeeStateCode=" in url:
            awards = []
            for state, val in phase2_awards.items():
                if f"awardeeStateCode={state}" in url:
                    awards = val
                    break
        else:
            awards = phase1_awards

        body = json.dumps({"response": {"award": awards}}).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    return fake_urlopen


class TestSupplementalCollection(unittest.TestCase):
    def setUp(self):
        self.conn = init_db(":memory:")

    def tearDown(self):
        self.conn.close()

    def _run(self, phase1=None, phase2=None, phase3=None):
        with patch("urllib.request.urlopen", side_effect=_smart_urlopen(phase1, phase2, phase3)):
            with patch("time.sleep"):
                collect_grants(self.conn)

    def test_supplemental_pass_catches_grant_missed_by_global(self):
        """Grant absent from Phase 1 global query is caught by Phase 2 per-state pass."""
        grant = _make_award("SUPP001", "Colorado State University")
        self._run(
            phase1=[],
            phase2={"CO": [grant]},
        )
        cur = self.conn.execute("SELECT COUNT(*) FROM grants WHERE award_id = 'SUPP001'")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_priority_pi_search_catches_missed_grant(self):
        """Grant missed by Phase 1 and Phase 2 is caught by Phase 3 PI name search."""
        grant = _make_award("PI001", "University of Colorado Boulder")
        grant["piFirstName"] = "Shelley"
        grant["piLastName"] = "Knuth"
        self._run(phase3=[grant])
        cur = self.conn.execute("SELECT COUNT(*) FROM grants WHERE award_id = 'PI001'")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_no_duplicate_when_grant_in_multiple_phases(self):
        """Same grant appearing in Phase 1 and Phase 2 is stored exactly once."""
        grant = _make_award("MULTI001", "Arizona State University")
        self._run(
            phase1=[grant],
            phase2={"AZ": [grant]},
        )
        cur = self.conn.execute("SELECT COUNT(*) FROM grants WHERE award_id = 'MULTI001'")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_priority_pis_includes_knuth(self):
        """PRIORITY_PIS must include Shelley Knuth with CU Boulder as home institution."""
        knuth_entries = [(l, f, inst) for l, f, inst in PRIORITY_PIS if l == "Knuth" and f == "Shelley"]
        self.assertGreater(len(knuth_entries), 0, "Shelley Knuth must be in PRIORITY_PIS")
        self.assertEqual(knuth_entries[0][2], "CU Boulder")

if __name__ == "__main__":
    unittest.main(verbosity=2)
