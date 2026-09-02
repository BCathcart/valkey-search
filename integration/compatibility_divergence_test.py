"""Replay the captured RediSearch answers for known FT.SEARCH divergences.

Each test below replays the commands captured by
compatibility_divergence/generate_divergence.py against valkey-search and
compares the replies *raw* -- element for element, order and arity included.

Every test is expected to fail, so each is marked xfail(strict=True): CI stays
green while the divergence exists, and turns red the moment one is fixed without
removing its marker. The marker's reason names the divergence.

Why not add these to compatibility_test.py: its comparator normalizes away
exactly the signals these bugs live in. unpack_result re-sorts *both* engines'
rows by the sort field before comparing, so an ordering divergence disappears;
unpack_search_result drops the sort-key element entirely, so WITHSORTKEYS values
are never compared; and its 2-element stride cannot parse a NOCONTENT reply at
all. Cases added there would pass against every divergence below.
"""

import gzip
import os
import pickle

import pytest
from compatibility_divergence import ANSWER_PATH, compute_generator_hash
from utils import IndexingTestHelper, wait_for_async_queries_drained
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


def load_answers():
    """Load the captured answers, checking they match the generator source.

    Set SKIP_DIVERGENCE_HASH_CHECK=1 to bypass the check while iterating on the
    generator locally.
    """
    with gzip.open(ANSWER_PATH, "rb") as f:
        payload = pickle.load(f)

    if os.getenv("SKIP_DIVERGENCE_HASH_CHECK") != "1":
        stored = payload.get("generator_hash")
        current = compute_generator_hash()
        if stored != current:
            pytest.fail(
                f"\n{os.path.basename(ANSWER_PATH)} is stale.\n"
                f"  Stored hash:  {stored}\n"
                f"  Current hash: {current}\n\n"
                f"generate_divergence.py has changed since the answers were\n"
                f"captured. Regenerate (Docker required):\n\n"
                f"  cd integration && python -m pytest "
                f"compatibility_divergence/generate_divergence.py\n\n"
                f"Then commit the updated answer file. To bypass this check,\n"
                f"set SKIP_DIVERGENCE_HASH_CHECK=1.\n",
                pytrace=False,
            )

    grouped = {}
    for answer in payload["answers"]:
        grouped.setdefault(answer["testname"], []).append(answer)
    return payload, grouped


PAYLOAD, ANSWERS = load_answers()

# Which RediSearch build the expectations came from, e.g. "search 21020".
REFERENCE = PAYLOAD.get("reference_module", "unknown")


def printable(value):
    """Render a reply for the assertion message, bytes decoded where possible."""
    if isinstance(value, list):
        return [printable(v) for v in value]
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return value
    return value


class TestFtSearchDivergence(ValkeySearchTestCaseBase):
    """One test per divergence. All are expected to xfail."""

    def replay(self, testname):
        """Run every command captured for testname; return (cmd, want, got).

        All commands run before any comparison so that the in-flight query
        counter can be drained first -- an assertion raised mid-flight would
        leave an async query alive into fixture teardown, which ASAN reports as
        a leak.
        """
        answers = ANSWERS[testname]
        assert answers, f"no captured answers for {testname}"

        client = self.client
        outcomes = []
        applied_setup = None

        for answer in answers:
            assert not answer["exception"], (
                f"RediSearch itself failed on {answer['cmd']}; the captured "
                f"answer is unusable"
            )

            if answer["setup"] != applied_setup:
                self.apply_setup(client, answer["setup"])
                applied_setup = answer["setup"]

            try:
                got = client.execute_command(*answer["cmd"])
            except Exception as exc:  # a divergence may also be an error reply
                got = exc
            outcomes.append((answer["cmd"], answer["result"], got))

        wait_for_async_queries_drained(self)
        return outcomes

    def apply_setup(self, client, setup):
        """Create the fixture's index and documents, then wait for indexing."""
        client.execute_command("FLUSHALL", "SYNC")
        for index in IndexingTestHelper.get_ft_list(client):
            client.execute_command("FT.DROPINDEX", index)

        index_name = setup[0][1]
        for cmd in setup:
            client.execute_command(*cmd)
        waiters.wait_for_true(
            lambda: IndexingTestHelper.is_indexing_complete_on_node(
                client, index_name
            )
        )

    def check(self, testname):
        """Assert every replayed reply matches RediSearch exactly."""
        failures = []
        for cmd, want, got in self.replay(testname):
            if want != got:
                failures.append(
                    f"\n  cmd:  {' '.join(cmd)}"
                    f"\n  {REFERENCE}: {printable(want)}"
                    f"\n  valkey-search: {printable(got)}"
                )
        assert not failures, "".join(failures)

    @pytest.mark.xfail(
        strict=True,
        reason="item 1: SORTBY is ignored under NOCONTENT, so results come "
        "back in insertion order and ASC == DESC (issue #1215, PR #1217)",
    )
    def test_sortby_ignored_under_nocontent(self):
        self.check("test_sortby_ignored_under_nocontent")

    @pytest.mark.xfail(
        strict=True,
        reason="item 2: WITHSORTKEYS under NOCONTENT omits the sort-key "
        "elements, so the reply has the wrong number of elements "
        "(issue #1215, PR #1217)",
    )
    def test_withsortkeys_dropped_under_nocontent(self):
        self.check("test_withsortkeys_dropped_under_nocontent")

    @pytest.mark.xfail(
        strict=True,
        reason="item 3: string SORTBY sorts by raw bytes; RediSearch collates "
        "SORTABLE strings case-insensitively",
    )
    def test_string_sortby_is_case_sensitive(self):
        self.check("test_string_sortby_is_case_sensitive")

    @pytest.mark.xfail(
        strict=True,
        reason="item 3: same defect on a TAG SORTABLE field",
    )
    def test_tag_sortby_is_case_sensitive(self):
        self.check("test_tag_sortby_is_case_sensitive")

    @pytest.mark.xfail(
        strict=True,
        reason="item 4: every sort key is prefixed '#'; RediSearch uses '$' "
        "for string sort keys and '#' only for numeric ones",
    )
    def test_string_sortkey_prefix(self):
        self.check("test_string_sortkey_prefix")

    @pytest.mark.xfail(
        strict=True,
        reason="item 5: WITHSORTKEYS without SORTBY emits the bare string '#'; "
        "RediSearch emits nil",
    )
    def test_sortkey_without_sortby_is_not_nil(self):
        self.check("test_sortkey_without_sortby_is_not_nil")

    @pytest.mark.xfail(
        strict=True,
        reason="item 5: a document missing the sort field emits '#'; "
        "RediSearch emits nil",
    )
    def test_sortkey_of_missing_field_is_not_nil(self):
        self.check("test_sortkey_of_missing_field_is_not_nil")

    @pytest.mark.xfail(
        strict=True,
        reason="item 6: numeric sort keys and RETURN values are echoed as the "
        "raw hash bytes; RediSearch normalizes them ('2.500' -> '2.5')",
    )
    def test_numeric_sortkey_not_normalized(self):
        self.check("test_numeric_sortkey_not_normalized")

    @pytest.mark.xfail(
        strict=True,
        reason="item 7: RETURN 0 keeps NOCONTENT set, so a later "
        "RETURN <field> is ignored; RediSearch lets the later clause win",
    )
    def test_return_zero_overridden_by_later_return(self):
        self.check("test_return_zero_overridden_by_later_return")

    @pytest.mark.xfail(
        strict=True,
        reason="item 8: documents with equal SORTBY values come back in an "
        "arbitrary order that varies between server instances given the same "
        "load sequence, and the LIMITed reply is not even a prefix of the "
        "unLIMITed one (the truncating path uses the unstable "
        "std::partial_sort). RediSearch returns ties in internal document id "
        "order, deterministically",
    )
    def test_sortby_ties_broken_by_doc_id(self):
        self.check("test_sortby_ties_broken_by_doc_id")
