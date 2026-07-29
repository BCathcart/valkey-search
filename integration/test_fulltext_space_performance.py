import pytest
import time
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import threading

"""
This file contains large-scale tests for full text search indexing performance.
These tests create large volumes of documents to test indexing scalability.
"""

@pytest.mark.skip(reason="Only used for manual testing currently")
class TestFullTextSpacePerformance(ValkeySearchTestCaseBase):
    # Class variables to store memory usage across tests
    test1_memory_bytes = None
    test2_memory_bytes = None

    def test_single_document_million_tokens(self):
        """Test 1: Create a document with 1 million 'b' tokens"""
        print("\n" + "="*80)
        print("TEST 1: Single document with 1 million 'b' tokens")
        print("="*80)
        
        self.client.execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")
        
        # Create a document with 1 million 'b' tokens
        print("Creating document with 1 million tokens...")
        million_bs = " ".join(["b"] * 1000000)
        self.client.execute_command("HSET", "product:1", "desc", million_bs)
        
        print("Ingestion complete. Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = self.client.execute_command("INFO", "SEARCH")
        
        # Parse search_used_memory_human (handle both dict and string formats)
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            # Store memory for next test and calculate per-position memory
            if memory_bytes != 'N/A' and memory_bytes > 0:
                TestFullTextSpacePerformance.test1_memory_bytes = memory_bytes
                per_position_memory = memory_bytes / 1_000_000
                print(f"📐 Per Position memory: {per_position_memory:.2f} bytes")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify the document was indexed
        result = self.client.execute_command("FT.SEARCH", "products", "b")
        assert result[0] == 1  # Should find 1 document
        assert result[1] == b"product:1"
        print("✅ Test passed: Document indexed successfully")

    def test_million_documents_single_token(self):
        """Test 2: Create 1 million documents, each with a single 'b' token using multi-client"""
        print("\n" + "="*80)
        print("TEST 2: 1 million documents with single 'b' token (multi-client)")
        print("="*80)
        
        # Create multiple clients for concurrent insertion
        num_clients = 10
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create index using first client
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")
        
        # Insert 1 million documents, each with just "b"
        num_docs = 1000000
        docs_per_client = num_docs // num_clients
        
        print(f"Inserting {num_docs:,} documents using {num_clients} concurrent clients...")
        print(f"Each client will insert {docs_per_client:,} documents")
        
        def insert_docs(client_id, start_id, count):
            client = clients[client_id]
            for i in range(start_id, start_id + count):
                client.execute_command("HSET", f"product:{i}", "desc", "b")
        
        threads = []
        for client_id in range(num_clients):
            start_id = client_id * docs_per_client
            thread = threading.Thread(target=insert_docs, args=(client_id, start_id, docs_per_client))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        print("Ingestion complete. Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = clients[0].execute_command("INFO", "SEARCH")
        
        # Parse search_used_memory_human (handle both dict and string formats)
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            # Store memory and calculate per-key memory (difference from test 1)
            if memory_bytes != 'N/A' and memory_bytes > 0:
                TestFullTextSpacePerformance.test2_memory_bytes = memory_bytes
                if TestFullTextSpacePerformance.test1_memory_bytes is not None:
                    per_key_memory = (memory_bytes - TestFullTextSpacePerformance.test1_memory_bytes) / 1_000_000
                    print(f"🔑 Per Key memory: {per_key_memory:.2f} bytes (excluding position memory)")
                else:
                    per_key_memory = memory_bytes / 1_000_000
                    print(f"🔑 Per Key memory: {per_key_memory:.2f} bytes (test 1 not run, showing total)")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify all documents were indexed
        result = clients[0].execute_command("FT.SEARCH", "products", "b", "LIMIT", "0", "0")
        assert result[0] == num_docs  # Should find all 1 million documents
        print(f"✅ Test passed: All {num_docs:,} documents indexed successfully")

    def test_million_documents_unique_tokens(self):
        """Test 3: Create 1 million documents, each with a unique token using multi-client"""
        print("\n" + "="*80)
        print("TEST 3: 1 million documents with unique tokens (multi-client)")
        print("="*80)
        
        # Create multiple clients for concurrent insertion
        num_clients = 10
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create index using first client
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH", "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")
        
        def generate_unique_token(n):
            """Generate unique token string for index n (a, b, c, ..., z, aa, ab, ...)"""
            result = ""
            n += 1  # Start from 1 to match a=1, b=2, etc.
            while n > 0:
                n -= 1
                result = chr(ord('a') + (n % 26)) + result
                n //= 26
            return result
        
        # Insert 1 million documents, each with a unique token
        num_docs = 1000000
        docs_per_client = num_docs // num_clients
        
        print(f"Inserting {num_docs:,} documents with unique tokens using {num_clients} concurrent clients...")
        print(f"Each client will insert {docs_per_client:,} documents")
        print("Token examples: 'a', 'b', 'c', ..., 'z', 'aa', 'ab', ...")
        
        def insert_unique_docs(client_id, start_id, count):
            client = clients[client_id]
            for i in range(start_id, start_id + count):
                unique_token = generate_unique_token(i)
                client.execute_command("HSET", f"product:{i}", "desc", unique_token)
        
        threads = []
        for client_id in range(num_clients):
            start_id = client_id * docs_per_client
            thread = threading.Thread(target=insert_unique_docs, args=(client_id, start_id, docs_per_client))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        print("Ingestion complete. Fetching memory usage...")
        
        # Get memory usage from INFO SEARCH
        info_result = clients[0].execute_command("INFO", "SEARCH")
        
        # Parse search_used_memory_human (handle both dict and string formats)
        if isinstance(info_result, dict):
            memory_used = info_result.get('search_used_memory_human', 'N/A')
            memory_bytes = info_result.get('search_used_memory_bytes', 0)
            print(f"\n📊 Memory Usage After Ingestion: {memory_used} ({memory_bytes} bytes)")
            
            # Calculate per-posting memory (difference from test 2)
            if memory_bytes != 'N/A' and memory_bytes > 0:
                if TestFullTextSpacePerformance.test2_memory_bytes is not None:
                    per_posting_memory = (memory_bytes - TestFullTextSpacePerformance.test2_memory_bytes) / 1_000_000
                    print(f"📮 Per Posting memory: {per_posting_memory:.2f} bytes (excluding key memory)")
                else:
                    per_posting_memory = memory_bytes / 1_000_000
                    print(f"📮 Per Posting memory: {per_posting_memory:.2f} bytes (test 2 not run, showing total)")
        else:
            info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
            for line in info_str.split('\n'):
                if 'search_used_memory_human' in line:
                    memory_used = line.split(':')[1].strip()
                    print(f"\n📊 Memory Usage After Ingestion: {memory_used}")
                    break
        
        # Verify some random unique tokens can be found
        print("\nVerifying random unique tokens can be found...")
        test_tokens = [generate_unique_token(1), generate_unique_token(2), generate_unique_token(3)]
        for token in test_tokens:
            result = clients[0].execute_command("FT.SEARCH", "products", token)
            assert result[0] == 1  # Each unique token should find exactly 1 document
        
        print(f"✅ Test passed: All {num_docs:,} documents with unique tokens indexed successfully")


# Common English words for realistic document generation
ENGLISH_WORDS = [
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
    "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
    "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
    "or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
    "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
    "when", "make", "can", "like", "time", "no", "just", "him", "know", "take",
    "people", "into", "year", "your", "good", "some", "could", "them", "see",
    "other", "than", "then", "now", "look", "only", "come", "its", "over",
    "think", "also", "back", "after", "use", "two", "how", "our", "work",
    "first", "well", "way", "even", "new", "want", "because", "any", "these",
    "give", "day", "most", "us", "great", "between", "need", "large", "often",
    "american", "important", "national", "world", "develop", "children", "system",
    "social", "government", "number", "right", "political", "general", "water",
    "history", "program", "market", "service", "information", "company", "change",
    "education", "university", "different", "family", "country", "student",
    "school", "research", "community", "business", "technology", "problem",
    "experience", "management", "development", "economic", "environment",
    "international", "president", "industry", "production", "organization",
    "individual", "investment", "relationship", "security", "computer",
    "performance", "traditional", "population", "professional", "authority",
    "financial", "significant", "construction", "communication", "analysis",
    "particular", "conference", "department", "application", "evaluation",
    "distribution", "independent", "commission", "administration", "agreement",
    "understanding", "responsibility", "association", "competition", "manufacturing",
    "temperature", "beautiful", "dangerous", "wonderful", "impossible",
    "interesting", "comfortable", "necessary", "reasonable", "available",
    "acceptable", "successful", "responsible", "fundamental", "professional",
    "traditional", "alternative", "appropriate", "considerable", "substantial",
    "running", "walking", "sleeping", "eating", "drinking", "thinking",
    "writing", "reading", "playing", "working", "singing", "dancing",
    "mountain", "building", "morning", "evening", "kitchen", "garden",
    "window", "village", "library", "hospital", "station", "museum",
    "bridge", "island", "forest", "desert", "ocean", "river", "valley",
    "quickly", "slowly", "carefully", "quietly", "suddenly", "finally",
    "usually", "certainly", "probably", "naturally", "obviously", "simply",
    "actually", "recently", "currently", "previously", "approximately",
    "orange", "purple", "yellow", "silver", "golden", "wooden", "metal",
    "plastic", "leather", "cotton", "ancient", "modern", "digital", "global",
    "coffee", "butter", "cheese", "chicken", "potato", "tomato", "pepper",
    "sugar", "bread", "cream", "fruit", "juice", "honey", "chocolate",
    "camera", "piano", "guitar", "violin", "trumpet", "painting", "sculpture",
    "theater", "concert", "festival", "holiday", "journey", "adventure",
    "discover", "explore", "imagine", "believe", "achieve", "succeed",
    "consider", "continue", "describe", "establish", "identify", "indicate",
    "maintain", "recognize", "recommend", "represent", "determine", "introduce",
    "strength", "courage", "patience", "freedom", "justice", "wisdom",
    "knowledge", "creative", "powerful", "peaceful", "grateful", "generous",
    "elephant", "giraffe", "penguin", "dolphin", "butterfly", "kangaroo",
    "crocodile", "squirrel", "cheetah", "octopus", "flamingo", "hedgehog",
]


# @pytest.mark.skip(reason="Manual benchmark only — run with TEST_PATTERN=test_benchmark_full_suite")
class TestRaxBenchmark(ValkeySearchTestCaseBase):
    """
    Memory benchmark to compare Rax vs RadixTree per-key tree overhead.

    Run at two commits:
      BEFORE (RadixTree): git checkout e73481f
      AFTER  (Rax):       git checkout fe6c99b

    Usage:
      TEST_PATTERN="test_benchmark_full_suite" ./integration/run.sh --capture
    """

    def _get_search_memory_bytes(self, client):
        """Extract search_used_memory_bytes from INFO SEARCH"""
        info_result = client.execute_command("INFO", "SEARCH")
        if isinstance(info_result, dict):
            return info_result.get('search_used_memory_bytes', 0)
        info_str = info_result.decode('utf-8') if isinstance(info_result, bytes) else info_result
        for line in info_str.split('\n'):
            if 'search_used_memory_bytes' in line:
                return int(line.split(':')[1].strip())
        return 0

    def _get_server_memory(self, client):
        """Get used_memory_rss from INFO memory"""
        info = client.execute_command("INFO", "memory")
        if isinstance(info, dict):
            return info.get('used_memory_rss', 0)
        info_str = info.decode('utf-8') if isinstance(info, bytes) else info
        for line in info_str.split('\n'):
            if line.startswith('used_memory_rss:'):
                return int(line.split(':')[1].strip())
        return 0

    def _ingest_documents(self, clients, num_docs, text_content):
        """Insert documents using multiple clients"""
        num_clients = len(clients)
        docs_per_client = num_docs // num_clients

        def insert_batch(client_id, start_id, count):
            client = clients[client_id]
            # Pipelining version (commented out due to potential hang):
            # pipe = client.pipeline(transaction=False)
            # for i in range(start_id, start_id + count):
            #     pipe.execute_command("HSET", f"product:{i}", "desc", text_content)
            #     if (i - start_id) % 1000 == 999:
            #         pipe.execute()
            #         pipe = client.pipeline(transaction=False)
            # pipe.execute()
            for i in range(start_id, start_id + count):
                client.execute_command("HSET", f"product:{i}", "desc", text_content)

        threads = []
        for cid in range(num_clients):
            start = cid * docs_per_client
            t = threading.Thread(target=insert_batch, args=(cid, start, docs_per_client))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def _run_scenario(self, text_content, scenario_label):
        """Run a single benchmark scenario and print results"""
        num_docs = 1_000_000
        num_clients = 10
        clients = [self.server.get_new_client() for _ in range(num_clients)]

        # Baseline
        baseline_rss = self._get_server_memory(clients[0])

        # Create index
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH",
                                   "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")

        # Measure search memory after index creation but before ingestion
        baseline_search_bytes = self._get_search_memory_bytes(clients[0])

        print(f"\n  Ingesting {num_docs:,} documents (text: '{text_content}')...")
        start_time = time.time()
        self._ingest_documents(clients, num_docs, text_content)
        elapsed = time.time() - start_time
        print(f"  Ingestion complete in {elapsed:.1f}s")

        # Measurements
        search_bytes = self._get_search_memory_bytes(clients[0])
        final_rss = self._get_server_memory(clients[0])
        rss_delta = final_rss - baseline_rss

        search_delta = search_bytes - baseline_search_bytes
        search_mb = search_delta / (1024 * 1024)
        rss_delta_mb = rss_delta / (1024 * 1024)
        per_key = search_delta / num_docs

        print(f"\n  ┌────────────────────────────────────────────────────────────┐")
        print(f"  │ {scenario_label:<58} │")
        print(f"  ├────────────────────────────────────────────────────────────┤")
        print(f"  │ Search memory:  {search_mb:>8.1f} MB  ({search_bytes:>12,} bytes)      │")
        print(f"  │ RSS delta:      {rss_delta_mb:>8.1f} MB                               │")
        print(f"  │ Per-key:        {per_key:>8.1f} bytes/key                         │")
        print(f"  └────────────────────────────────────────────────────────────┘")

        return {
            'scenario': scenario_label,
            'text': text_content,
            'search_mb': search_mb,
            'rss_delta_mb': rss_delta_mb,
            'per_key_bytes': per_key,
        }

    def test_benchmark_single_token(self):
        """1M docs x 1 token 'b' — simplest per-key tree (compressed path)"""
        print("\n" + "="*70)
        print("  SCENARIO 1: 1M documents x 1 token 'b'")
        print("  Tree shape: root -> compressed 'b' -> leaf")
        print("="*70)
        self._run_scenario("b", "1M x 1 token 'b'")

    def test_benchmark_three_tokens(self):
        """1M docs x 3 tokens 'a b c' — BEST CASE (branching node)"""
        print("\n" + "="*70)
        print("  SCENARIO 2: 1M documents x 3 tokens 'a b c'  * BEST CASE *")
        print("  Tree shape: root -> branch{a,b,c} -> 3 leaves")
        print("  This maximizes the std::map overhead in RadixTree")
        print("="*70)
        self._run_scenario("a b c", "1M x 3 tokens 'a b c'")

    def test_benchmark_ten_tokens(self):
        """1M docs x 10 tokens — large branching node"""
        print("\n" + "="*70)
        print("  SCENARIO 3: 1M documents x 10 tokens")
        print("  Tree shape: root -> branch{a..j} -> 10 leaves")
        print("="*70)
        self._run_scenario("a b c d e f g h i j", "1M x 10 tokens")

    def test_benchmark_multichar_tokens(self):
        """1M docs x 3 multi-char tokens — branch + compressed paths"""
        print("\n" + "="*70)
        print("  SCENARIO 4: 1M documents x 3 multi-char tokens")
        print("  Tree shape: root -> branch{h,w,t} -> compressed paths -> leaves")
        print("="*70)
        self._run_scenario("hello world test", "1M x 3 multi-char tokens")

    def _ingest_random_documents(self, clients, num_docs, vocab, min_words=20, max_words=80, seed=42):
        """Insert documents with random English text (varying length) using multiple clients"""
        import random as _random
        num_clients = len(clients)
        docs_per_client = num_docs // num_clients

        def insert_batch(client_id, start_id, count):
            client = clients[client_id]
            rng = _random.Random(seed + client_id)
            for i in range(start_id, start_id + count):
                doc_len = rng.randint(min_words, max_words)
                text = " ".join(rng.choices(vocab, k=doc_len))
                client.execute_command("HSET", f"product:{i}", "desc", text)

        threads = []
        for cid in range(num_clients):
            start = cid * docs_per_client
            t = threading.Thread(target=insert_batch, args=(cid, start, docs_per_client))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    def _run_realistic_scenario(self, num_docs, vocab, min_words, max_words, scenario_label):
        """Run a realistic benchmark scenario with random documents"""
        num_clients = 10
        clients = [self.server.get_new_client() for _ in range(num_clients)]

        # Baseline
        baseline_rss = self._get_server_memory(clients[0])

        # Create index
        clients[0].execute_command("FT.CREATE", "products", "ON", "HASH",
                                   "PREFIX", "1", "product:", "SCHEMA", "desc", "TEXT")

        # Measure search memory after index creation but before ingestion
        baseline_search_bytes = self._get_search_memory_bytes(clients[0])

        print(f"\n  Ingesting {num_docs:,} documents ({min_words}-{max_words} words each, vocab={len(vocab)})...")
        start_time = time.time()
        self._ingest_random_documents(clients, num_docs, vocab, min_words, max_words)
        elapsed = time.time() - start_time
        print(f"  Ingestion complete in {elapsed:.1f}s")

        # Measurements
        search_bytes = self._get_search_memory_bytes(clients[0])
        final_rss = self._get_server_memory(clients[0])
        rss_delta = final_rss - baseline_rss

        search_delta = search_bytes - baseline_search_bytes
        search_mb = search_delta / (1024 * 1024)
        rss_delta_mb = rss_delta / (1024 * 1024)
        per_key = search_delta / num_docs

        print(f"\n  ┌────────────────────────────────────────────────────────────┐")
        print(f"  │ {scenario_label:<58} │")
        print(f"  ├────────────────────────────────────────────────────────────┤")
        print(f"  │ Search memory:  {search_mb:>8.1f} MB  ({search_bytes:>12,} bytes)      │")
        print(f"  │ RSS delta:      {rss_delta_mb:>8.1f} MB                               │")
        print(f"  │ Per-key:        {per_key:>8.1f} bytes/key                         │")
        print(f"  └────────────────────────────────────────────────────────────┘")

        return {
            'scenario': scenario_label,
            'text': f"random {min_words}-{max_words} words",
            'search_mb': search_mb,
            'rss_delta_mb': rss_delta_mb,
            'per_key_bytes': per_key,
        }

    def test_benchmark_realistic_docs(self):
        """1M docs with realistic random English text (20-80 words per doc, 300-word vocab)"""
        print("\n" + "="*70)
        print("  SCENARIO 5: 1M documents x random English text")
        print("  Each doc: 20-80 words randomly sampled from 300-word vocabulary")
        print("  Simulates real-world product descriptions / article snippets")
        print("="*70)
        self._run_realistic_scenario(1_000_000, ENGLISH_WORDS, 20, 80, "1M x realistic text")

    def test_benchmark_full_suite(self):
        """
        Runs all 4 scenarios sequentially and prints a summary table.
        Run once at each commit, then compare side-by-side.

        Usage:
          TEST_PATTERN="test_benchmark_full_suite" ./integration/run.sh --capture
        """
        print("\n")
        print("=" * 66)
        print("  RAX vs RADIXTREE — PER-KEY TREE MEMORY BENCHMARK")
        print("=" * 66)
        print("  Run this at TWO commits:")
        print("    BEFORE: git checkout e73481f  (RadixTree)")
        print("    AFTER:  git checkout fe6c99b  (Rax)")
        print("=" * 66)

        scenarios = [
            ("b", "1 token 'b'"),
            ("a b c", "3 tokens 'a b c'"),
            ("a b c d e f g h i j", "10 tokens"),
            ("hello world test", "3 multi-char"),
        ]

        results = []
        for text, label in scenarios:
            self.client.execute_command("FLUSHALL")
            time.sleep(1)
            r = self._run_scenario(text, label)
            results.append(r)

        # Summary Table
        print("\n")
        print("=" * 66)
        print("  SUMMARY TABLE")
        print("=" * 66)
        print(f"  {'Scenario':<22} {'Search(MB)':<12} {'Per-Key(B)':<12} {'RSS delta(MB)':<14}")
        print(f"  {'-'*22} {'-'*12} {'-'*12} {'-'*14}")
        for r in results:
            print(f"  {r['scenario']:<22} {r['search_mb']:<12.1f} {r['per_key_bytes']:<12.1f} {r['rss_delta_mb']:<14.1f}")
        print("=" * 66)
        print("  BUILD: __________ (write 'RadixTree' or 'Rax')")
        print("=" * 66)
