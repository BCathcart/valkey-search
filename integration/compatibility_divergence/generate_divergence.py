"""Capture RediSearch answers for known FT.SEARCH compatibility divergences.

This is a compatibility generator in the same shape as
compatibility/generate.py -- it reuses BaseCompatibilityTest for the Docker
spin-up of redis/redis-stack-server, the answer accumulation, and the pickle
dump -- but it exists for a different purpose. Every command captured here is
one valkey-search answers differently from RediSearch today, so the replay side
(integration/compatibility_divergence_test.py) marks each test xfail.

Two things differ from the main generator:

  * Fixtures are defined in this file rather than in compatibility/data_sets.py.
    Each divergence needs a purpose-built corpus (mixed-case strings, equal sort
    values, unnormalized numerics), and the shared datasets have none of them.
    Every answer record therefore carries the setup commands that produced it,
    so the replay side needs no dataset registry.

  * Answers are compared raw on replay -- element for element, order and arity
    included. compatibility_test.py's comparator re-sorts both engines' rows by
    the sort field, drops the sort-key element when unpacking, and cannot parse
    a NOCONTENT reply, which is precisely why these divergences went unnoticed.

Regenerate (Docker required):

    cd integration && python -m pytest compatibility_divergence/generate_divergence.py
"""

import gzip
import os
import pickle
import time

from compatibility.generate import BaseCompatibilityTest

from . import ANSWER_PATH, compute_generator_hash

# Every command pins DIALECT 2. RediSearch defaults to dialect 1, which
# valkey-search rejects, so an un-suffixed command would compare two different
# dialects.
DIALECT = ["DIALECT", "2"]

# All fixtures carry a TAG field with the same value in every document, so the
# match predicate itself is trivial and cannot contribute a divergence of its
# own.
MATCH = "@m:{all}"

# Documents are loaded in an order that does not coincide with any sort order
# under test, so an unsorted reply is visibly unsorted.
FIXTURES = {
    # Numeric SORTABLE, three distinct values. Items 1, 2 and 7.
    "numeric": {
        "index": "div_num",
        "create": [
            "FT.CREATE", "div_num", "ON", "HASH", "PREFIX", "1", "n:",
            "SCHEMA", "m", "TAG", "price", "NUMERIC", "SORTABLE",
            "title", "TEXT",
        ],
        "docs": [
            ["HSET", "n:1", "m", "all", "price", "30", "title", "hello world"],
            ["HSET", "n:2", "m", "all", "price", "10", "title", "hello there"],
            ["HSET", "n:3", "m", "all", "price", "20", "title", "hello again"],
        ],
    },
    # Mixed-case TEXT SORTABLE. Item 3. Single-case fixtures cannot detect a
    # collation difference, which is why the existing tests do not.
    "mixed_case_text": {
        "index": "div_str",
        "create": [
            "FT.CREATE", "div_str", "ON", "HASH", "PREFIX", "1", "c:",
            "SCHEMA", "m", "TAG", "s", "TEXT", "SORTABLE",
        ],
        "docs": [
            ["HSET", "c:1", "m", "all", "s", "Banana"],
            ["HSET", "c:2", "m", "all", "s", "apple"],
            ["HSET", "c:3", "m", "all", "s", "Cherry"],
            ["HSET", "c:4", "m", "all", "s", "date"],
        ],
    },
    # Mixed-case TAG SORTABLE. Item 3, second shape.
    "mixed_case_tag": {
        "index": "div_tag",
        "create": [
            "FT.CREATE", "div_tag", "ON", "HASH", "PREFIX", "1", "g:",
            "SCHEMA", "m", "TAG", "g", "TAG", "SORTABLE",
        ],
        "docs": [
            ["HSET", "g:1", "m", "all", "g", "Beta"],
            ["HSET", "g:2", "m", "all", "g", "alpha"],
            ["HSET", "g:3", "m", "all", "g", "Gamma"],
            ["HSET", "g:4", "m", "all", "g", "delta"],
        ],
    },
    # Single-case strings. Item 4: both engines agree on the order here, so the
    # sort-key prefix is the only thing left to differ.
    "lower_case_text": {
        "index": "div_low",
        "create": [
            "FT.CREATE", "div_low", "ON", "HASH", "PREFIX", "1", "l:",
            "SCHEMA", "m", "TAG", "z", "TEXT", "SORTABLE",
        ],
        "docs": [
            ["HSET", "l:1", "m", "all", "z", "zebra"],
            ["HSET", "l:2", "m", "all", "z", "apple"],
            ["HSET", "l:3", "m", "all", "z", "mango"],
        ],
    },
    # One document, so nothing about ordering can enter the comparison.
    # Items 5a and 7.
    "single": {
        "index": "div_one",
        "create": [
            "FT.CREATE", "div_one", "ON", "HASH", "PREFIX", "1", "o:",
            "SCHEMA", "m", "TAG", "p", "NUMERIC", "SORTABLE", "title", "TEXT",
        ],
        "docs": [
            ["HSET", "o:1", "m", "all", "p", "10", "title", "hello world"],
        ],
    },
    # One document lacks the sort field. Item 5b. RETURN pins the shared tag
    # rather than the sort field, so the attrs are identical for every document
    # and the sort key is the only element that can differ.
    "sparse": {
        "index": "div_sparse",
        "create": [
            "FT.CREATE", "div_sparse", "ON", "HASH", "PREFIX", "1", "s:",
            "SCHEMA", "m", "TAG", "p", "NUMERIC", "SORTABLE",
        ],
        "docs": [
            ["HSET", "s:1", "m", "all", "p", "20"],
            ["HSET", "s:2", "m", "all", "p", "10"],
            ["HSET", "s:3", "m", "all"],
        ],
    },
    # Numerics whose stored representation is not their normalized form.
    # Item 6. Both engines order these correctly; only the emitted text differs.
    "unnormalized_numbers": {
        "index": "div_raw",
        "create": [
            "FT.CREATE", "div_raw", "ON", "HASH", "PREFIX", "1", "r:",
            "SCHEMA", "m", "TAG", "p", "NUMERIC", "SORTABLE",
        ],
        "docs": [
            ["HSET", "r:1", "m", "all", "p", "2.500"],
            ["HSET", "r:2", "m", "all", "p", "1e3"],
            ["HSET", "r:3", "m", "all", "p", "10"],
        ],
    },
    # Every document has the same sort value, so only the tie-break shows.
    # Item 8. Ten documents, loaded in neither key order nor sorted order:
    # RediSearch returns tied documents in internal document id order, which is
    # load order, so the expected reply is this exact sequence. Five documents
    # is not enough -- with a set that small both engines happen to come back in
    # key order.
    "ties": {
        "index": "div_ties",
        "create": [
            "FT.CREATE", "div_ties", "ON", "HASH", "PREFIX", "1", "i:",
            "SCHEMA", "m", "TAG", "p", "NUMERIC", "SORTABLE",
        ],
        "docs": [
            ["HSET", f"i:{i:03d}", "m", "all", "p", "100"]
            for i in [7, 3, 10, 1, 8, 5, 2, 9, 4, 6]
        ],
    },
}


class TestFtSearchDivergence(BaseCompatibilityTest):
    """Captures the RediSearch side of each divergence."""

    ANSWER_FILE_NAME = ANSWER_PATH

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.reference_module = cls._reference_module_version()

    @classmethod
    def _reference_module_version(cls):
        """The reference search module's name and version, for the record."""
        for module in cls.client.execute_command("MODULE LIST"):
            if module.get(b"name") == b"search":
                return f"search {module.get(b'ver')}"
        return "unknown"

    @classmethod
    def teardown_class(cls):
        # Same shape as the base class, with our own metadata: the generator
        # hash is scoped to this file (see compatibility_divergence/__init__.py)
        # and the reference module version records which RediSearch build the
        # expectations came from.
        print("Stopping Generate-search server")
        os.system("docker stop Generate-search")
        print("Dumping", len(cls.answers), "answers from", cls.reference_module)
        payload = {
            "generator_hash": compute_generator_hash(),
            "reference_module": cls.reference_module,
            "answers": cls.answers,
        }
        with gzip.open(cls.ANSWER_FILE_NAME, "wb") as answer_file:
            pickle.dump(payload, answer_file)

    def load_fixture(self, name):
        """Create the index and documents for a fixture on the reference server.

        The commands are remembered so every answer captured afterwards carries
        the setup that produced it.
        """
        fixture = FIXTURES[name]
        self.setup_cmds = [fixture["create"]] + fixture["docs"]
        for cmd in self.setup_cmds:
            self.client.execute_command(*cmd)
        time.sleep(1)

    def check(self, *cmd):
        """Run one command and record its reply."""
        cmd = [str(c) for c in cmd]
        answer = {
            "setup": self.setup_cmds,
            "cmd": cmd,
            "testname": os.environ["PYTEST_CURRENT_TEST"]
            .split("::")[-1]
            .split(" ")[0],
        }
        try:
            print("Cmd:", *cmd)
            answer["result"] = self.client.execute_command(*cmd)
            answer["exception"] = False
            print("replied:", answer["result"])
        except Exception as exc:
            print(f"Got exception '{exc}' for cmd {cmd}")
            answer["result"] = {}
            answer["exception"] = True
        self.answers.append(answer)

    # ---- Item 1: SORTBY is ignored under NOCONTENT (issue #1215, PR #1217) ---

    def test_sortby_ignored_under_nocontent(self):
        self.load_fixture("numeric")
        for direction in ["ASC", "DESC"]:
            self.check("FT.SEARCH", "div_num", MATCH, "SORTBY", "price",
                       direction, "NOCONTENT", *DIALECT)

    # ---- Item 2: WITHSORTKEYS elements are dropped under NOCONTENT ----------

    def test_withsortkeys_dropped_under_nocontent(self):
        self.load_fixture("numeric")
        self.check("FT.SEARCH", "div_num", MATCH, "SORTBY", "price", "ASC",
                   "WITHSORTKEYS", "NOCONTENT", *DIALECT)

    # ---- Item 3: string SORTBY is case-sensitive ---------------------------

    def test_string_sortby_is_case_sensitive(self):
        self.load_fixture("mixed_case_text")
        for direction in ["ASC", "DESC"]:
            self.check("FT.SEARCH", "div_str", MATCH, "SORTBY", "s", direction,
                       "RETURN", "1", "s", *DIALECT)

    def test_tag_sortby_is_case_sensitive(self):
        self.load_fixture("mixed_case_tag")
        for direction in ["ASC", "DESC"]:
            self.check("FT.SEARCH", "div_tag", MATCH, "SORTBY", "g", direction,
                       "RETURN", "1", "g", *DIALECT)

    # ---- Item 4: string sort keys are prefixed '#' instead of '$' ----------

    def test_string_sortkey_prefix(self):
        self.load_fixture("lower_case_text")
        self.check("FT.SEARCH", "div_low", MATCH, "SORTBY", "z", "ASC",
                   "WITHSORTKEYS", "RETURN", "1", "z", *DIALECT)

    # ---- Item 5: an absent sort key is '#' instead of nil ------------------

    def test_sortkey_without_sortby_is_not_nil(self):
        self.load_fixture("single")
        self.check("FT.SEARCH", "div_one", MATCH, "WITHSORTKEYS",
                   "RETURN", "1", "title", *DIALECT)

    def test_sortkey_of_missing_field_is_not_nil(self):
        self.load_fixture("sparse")
        self.check("FT.SEARCH", "div_sparse", MATCH, "SORTBY", "p", "ASC",
                   "WITHSORTKEYS", "RETURN", "1", "m", *DIALECT)

    # ---- Item 6: numeric sort keys and RETURN values are not normalized ----

    def test_numeric_sortkey_not_normalized(self):
        self.load_fixture("unnormalized_numbers")
        self.check("FT.SEARCH", "div_raw", MATCH, "SORTBY", "p", "ASC",
                   "WITHSORTKEYS", "RETURN", "1", "p", *DIALECT)

    # ---- Item 7: RETURN 0 is not overridden by a later RETURN --------------

    def test_return_zero_overridden_by_later_return(self):
        self.load_fixture("single")
        self.check("FT.SEARCH", "div_one", MATCH, "RETURN", "0",
                   "RETURN", "1", "title", *DIALECT)

    # ---- Item 8: SORTBY ties are not broken by document id -----------------

    def test_sortby_ties_broken_by_doc_id(self):
        self.load_fixture("ties")
        # LIMIT 0 10 takes the full sort; LIMIT 0 5 takes the truncating
        # std::partial_sort path, whose result is not even a prefix of the
        # untruncated one.
        for limit in ["10", "5"]:
            self.check("FT.SEARCH", "div_ties", MATCH, "SORTBY", "p", "ASC",
                       "LIMIT", "0", limit, "RETURN", "1", "p", *DIALECT)
