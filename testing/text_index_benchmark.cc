/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Text index micro-benchmark.
 *
 * A pure C++ replacement for `integration/test_fulltext_space_performance.py::
 * TestRaxBenchmark`. Exercises the same 5 scenarios the Python test uses, but
 * bypasses Valkey, the RESP protocol, sockets, threads, and the Python
 * runtime, so the measured throughput reflects the C++ indexing path only.
 *
 * Design (matches user spec):
 *   - Single writer thread, then single reader thread (phased C).
 *   - Hardcoded 5 scenarios (1M docs each by default).
 *   - Depends only on stable public API — TextIndexSchema, Text, AddRecord,
 *     CommitKeyData, Rax::WordIterator. Meant to be stashed and popped onto
 *     older commits (e.g. e73481f RadixTree vs fe6c99b Rax) unchanged.
 *   - Memory reported via vmsdk::GetUsedMemoryCnt() — the same counter that
 *     feeds INFO SEARCH's `search_used_memory_bytes`, so numbers are
 *     directly comparable to the Python results.
 *
 * Build & run (from repo root):
 *   ./build.sh
 *   ./.build-release/tests/text_index_benchmark
 */


#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "gtest/gtest.h"
#include "src/index_schema.pb.h"
#include "src/indexes/text.h"
#include "src/indexes/text/text_index.h"
#include "src/utils/string_interning.h"
#include "vmsdk/src/memory_allocation.h"
#include "vmsdk/src/testing_infra/module.h"
#include "vmsdk/src/testing_infra/utils.h"


namespace {

using ::valkey_search::InternedStringPtr;
using ::valkey_search::StringInternStore;
using ::valkey_search::indexes::Text;
using ::valkey_search::indexes::text::TextIndexSchema;

// ---------------------------------------------------------------------------
// Scenarios — mirror TestRaxBenchmark exactly.
// ---------------------------------------------------------------------------

struct Scenario {
  const char* label;
  const char* text;   // fixed doc content ("" ⇒ generate random English)
  const char* shape_note;
};

constexpr Scenario kScenarios[] = {
    {"1M x 1 token 'b'", "b",
     "root -> compressed 'b' -> leaf"},
    {"1M x 3 tokens 'a b c'", "a b c",
     "root -> branch{a,b,c} -> 3 leaves   * BEST CASE (branching) *"},
    {"1M x 10 tokens", "a b c d e f g h i j",
     "root -> branch{a..j} -> 10 leaves"},
    {"1M x 3 multi-char tokens", "hello world test",
     "root -> branch{h,w,t} -> compressed paths -> leaves"},
    {"1M x realistic text", "",
     "20-80 words per doc, ~300-word English vocab (like the Python test)"},
};

// Realistic English vocabulary — copied verbatim from
// integration/test_fulltext_space_performance.py so results are directly
// comparable.
static const char* const kEnglishWords[] = {
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
    "american", "important", "national", "world", "develop", "children",
    "system", "social", "government", "number", "right", "political", "general",
    "water", "history", "program", "market", "service", "information",
    "company", "change", "education", "university", "different", "family",
    "country", "student", "school", "research", "community", "business",
    "technology", "problem", "experience", "management", "development",
    "economic", "environment", "international", "president", "industry",
    "production", "organization", "individual", "investment", "relationship",
    "security", "computer", "performance", "traditional", "population",
    "professional", "authority", "financial", "significant", "construction",
    "communication", "analysis", "particular", "conference", "department",
    "application", "evaluation", "distribution", "independent", "commission",
    "administration", "agreement", "understanding", "responsibility",
    "association", "competition", "manufacturing", "temperature", "beautiful",
    "dangerous", "wonderful", "impossible", "interesting", "comfortable",
    "necessary", "reasonable", "available", "acceptable", "successful",
    "responsible", "fundamental", "alternative", "appropriate", "considerable",
    "substantial", "running", "walking", "sleeping", "eating", "drinking",
    "thinking", "writing", "reading", "playing", "working", "singing",
    "dancing", "mountain", "building", "morning", "evening", "kitchen",
    "garden", "window", "village", "library", "hospital", "station", "museum",
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
};
constexpr size_t kEnglishWordCount =
    sizeof(kEnglishWords) / sizeof(kEnglishWords[0]);

// ---------------------------------------------------------------------------
// Configuration knobs (compile-time — matches Python test defaults).
// ---------------------------------------------------------------------------

constexpr size_t kNumDocs = 1'000'000;
constexpr size_t kNumReadLookups = 5'000'000;
constexpr int kRealisticMinWords = 20;
constexpr int kRealisticMaxWords = 80;
constexpr uint32_t kMinStemSize = 4;

// Default English punctuation — matches TextIndexSchemaTest and Python test.
static const char* const kDefaultPunctuation =
    " \t\n\r!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

using Clock = std::chrono::steady_clock;
using Nanos = std::chrono::nanoseconds;

double SecondsSince(Clock::time_point t0) {
  return std::chrono::duration<double>(Clock::now() - t0).count();
}

std::shared_ptr<TextIndexSchema> MakeSchema() {
  std::vector<std::string> empty_stop_words;
  return std::make_shared<TextIndexSchema>(
      valkey_search::data_model::LANGUAGE_ENGLISH, kDefaultPunctuation,
      /*with_offsets=*/false, empty_stop_words, kMinStemSize);
}

std::unique_ptr<Text> MakeTextIndex(std::shared_ptr<TextIndexSchema> schema) {
  valkey_search::data_model::TextIndex proto;
  proto.set_no_stem(true);  // Disable stemming — matches the Python test setup
                            // (no explicit stemming enabled in FT.CREATE).
  return std::make_unique<Text>(proto, std::move(schema));
}

// Generate keys once, upfront, so the timed loop only measures indexing.
std::vector<InternedStringPtr> GenerateKeys(size_t n) {
  std::vector<InternedStringPtr> keys;
  keys.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    keys.push_back(
        StringInternStore::Intern("product:" + std::to_string(i)));
  }
  return keys;
}

// For scenarios with a fixed doc string, all docs share the same content — no
// need to materialize N copies, just pass one string_view. For the realistic
// scenario we pre-generate all doc contents.
std::vector<std::string> GenerateRealisticDocs(size_t n, uint32_t seed = 42) {
  std::vector<std::string> docs;
  docs.reserve(n);
  std::mt19937 rng(seed);
  std::uniform_int_distribution<int> len_dist(kRealisticMinWords,
                                              kRealisticMaxWords);
  std::uniform_int_distribution<size_t> word_dist(0, kEnglishWordCount - 1);
  for (size_t i = 0; i < n; ++i) {
    int doc_len = len_dist(rng);
    std::string doc;
    doc.reserve(doc_len * 8);
    for (int w = 0; w < doc_len; ++w) {
      if (w != 0) doc.push_back(' ');
      doc.append(kEnglishWords[word_dist(rng)]);
    }
    docs.push_back(std::move(doc));
  }
  return docs;
}

// Tokens to hit during the reader phase. For fixed-content scenarios we split
// the doc text on whitespace. For the realistic scenario we use the vocab.
std::vector<std::string> BuildReaderVocabulary(const Scenario& sc) {
  std::vector<std::string> vocab;
  if (sc.text[0] != '\0') {
    for (absl::string_view tok :
         absl::StrSplit(absl::string_view(sc.text), ' ', absl::SkipEmpty())) {
      vocab.emplace_back(tok);
    }
  } else {
    vocab.reserve(kEnglishWordCount);
    for (size_t i = 0; i < kEnglishWordCount; ++i) {
      vocab.emplace_back(kEnglishWords[i]);
    }
  }
  return vocab;
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

struct Result {
  std::string scenario;
  double write_seconds = 0;
  double read_seconds = 0;
  size_t docs = 0;
  size_t reads = 0;
  size_t reads_hit = 0;
  int64_t mem_delta_bytes = 0;
};

void PrintHeader(const Scenario& sc) {
  std::printf("\n");
  std::printf("======================================================================\n");
  std::printf("  %s\n", sc.label);
  std::printf("  Tree shape: %s\n", sc.shape_note);
  std::printf("======================================================================\n");
}

void PrintResult(const Result& r) {
  const double docs_per_sec = r.docs / r.write_seconds;
  const double reads_per_sec = r.read_seconds > 0
                                    ? (r.reads / r.read_seconds)
                                    : 0.0;
  const double mem_mb = r.mem_delta_bytes / (1024.0 * 1024.0);
  const double per_key = static_cast<double>(r.mem_delta_bytes) / r.docs;

  std::printf("\n");
  std::printf("  +----------------------------------------------------------+\n");
  std::printf("  | %-56s |\n", r.scenario.c_str());
  std::printf("  +----------------------------------------------------------+\n");
  std::printf("  | Ingest time    : %8.2f s (%.1f docs/s)          |\n",
              r.write_seconds, docs_per_sec);
  std::printf("  | Read  time     : %8.2f s (%.1f lookups/s, hits=%zu) |\n",
              r.read_seconds, reads_per_sec, r.reads_hit);
  std::printf("  | Search memory  : %8.1f MB  (%12lld bytes)  |\n", mem_mb,
              static_cast<long long>(r.mem_delta_bytes));
  std::printf("  | Per-key        : %8.1f bytes/key                    |\n",
              per_key);
  std::printf("  +----------------------------------------------------------+\n");
}

Result RunScenario(const Scenario& sc,
                   const std::vector<InternedStringPtr>& keys) {
  PrintHeader(sc);

  const bool is_realistic = (sc.text[0] == '\0');
  std::vector<std::string> docs_owned;  // only populated for realistic
  if (is_realistic) {
    std::printf("  Generating %zu random English docs...\n", keys.size());
    docs_owned = GenerateRealisticDocs(keys.size());
  }
  absl::string_view fixed_doc(sc.text);

  // Fresh schema per scenario so the memory delta only counts this scenario's
  // structures.
  auto schema = MakeSchema();
  auto text_index = MakeTextIndex(schema);

  // Baseline memory (after schema/index construction — subtract the shell
  // overhead so the reported number is purely indexed data).
  const int64_t baseline = static_cast<int64_t>(vmsdk::GetUsedMemoryCnt());

  // -------- WRITER PHASE (single thread) --------
  std::printf("  Ingesting %zu documents...\n", keys.size());
  const auto write_start = Clock::now();
  for (size_t i = 0; i < keys.size(); ++i) {
    absl::string_view doc = is_realistic ? absl::string_view(docs_owned[i])
                                          : fixed_doc;
    auto s = text_index->AddRecord(keys[i], doc);
    if (!s.ok() || !s.value()) {
      std::fprintf(stderr, "AddRecord failed at i=%zu: %s\n", i,
                   s.ok() ? "returned false" : s.status().ToString().c_str());
      std::abort();
    }
    schema->CommitKeyData(keys[i]);
  }
  const double write_seconds = SecondsSince(write_start);

  // Memory sample right after ingest, before reader phase.
  const int64_t after_write = static_cast<int64_t>(vmsdk::GetUsedMemoryCnt());
  const int64_t mem_delta = after_write - baseline;

  // Free the realistic-doc storage — it's no longer needed and would inflate
  // the reader-phase working set otherwise.
  std::vector<std::string>().swap(docs_owned);

  // -------- READER PHASE (single thread) --------
  std::vector<std::string> reader_vocab = BuildReaderVocabulary(sc);
  std::mt19937 rng(1337);
  std::uniform_int_distribution<size_t> word_pick(0, reader_vocab.size() - 1);
  size_t hits = 0;
  std::printf("  Doing %zu random lookups...\n", kNumReadLookups);
  const auto read_start = Clock::now();
  for (size_t i = 0; i < kNumReadLookups; ++i) {
    const std::string& tok = reader_vocab[word_pick(rng)];
    auto it =
        schema->GetTextIndex()->GetPrefix().GetWordIterator(tok);
    // Cross-commit-portable check: on Rax GetTarget() returns void*, on
    // RadixTree it returns const Target&. We only need to know the term was
    // found — Done() + GetWord() matching is all the API surface we can rely
    // on across both.
    if (!it.Done() && it.GetWord() == tok) {
      ++hits;
      // Force the compiler to keep the iterator work — assemble a byte from
      // the matched word.
      asm volatile("" ::"r"(it.GetWord().data()) : "memory");
    }
  }

  const double read_seconds = SecondsSince(read_start);

  Result r;
  r.scenario = sc.label;
  r.write_seconds = write_seconds;
  r.read_seconds = read_seconds;
  r.docs = keys.size();
  r.reads = kNumReadLookups;
  r.reads_hit = hits;
  r.mem_delta_bytes = mem_delta;

  PrintResult(r);
  return r;
}

}  // namespace

int main(int /*argc*/, char** /*argv*/) {
  // Wire the mock ValkeyModule symbol table so vmsdk allocation tracking and
  // any module-API calls made during indexing (e.g. MallocUsableSize used by
  // InternedString accounting) resolve to test stubs.
  TestValkeyModule_Init();
  // The mock module is intentionally leaked as a global — tell gmock so it
  // doesn't complain at program exit (which would make us exit with code 1).
  if (kMockValkeyModule) {
    testing::Mock::AllowLeak(kMockValkeyModule);
  }



  std::printf(
      "\n"
      "==========================================================\n"
      "  TEXT INDEX MICRO-BENCHMARK (C++ replacement for\n"
      "  TestRaxBenchmark in test_fulltext_space_performance.py)\n"
      "==========================================================\n"
      "  Docs per scenario  : %zu\n"
      "  Read lookups       : %zu\n"
      "  Threading          : 1 writer, then 1 reader (phased)\n"
      "==========================================================\n",
      kNumDocs, kNumReadLookups);

  // Keys are shared across scenarios — same "product:i" strings the Python
  // test used. They're built once upfront so string setup isn't in the timed
  // loop.
  std::printf("\n  Preparing %zu interned keys...\n", kNumDocs);
  auto keys = GenerateKeys(kNumDocs);

  std::vector<Result> results;
  results.reserve(sizeof(kScenarios) / sizeof(kScenarios[0]));
  for (const auto& sc : kScenarios) {
    results.push_back(RunScenario(sc, keys));
  }

  // -------- Summary table --------
  std::printf("\n");
  std::printf("==========================================================\n");
  std::printf("  SUMMARY TABLE\n");
  std::printf("==========================================================\n");
  std::printf("  %-28s %10s %12s %14s %10s\n",
              "Scenario", "Ingest(s)", "Docs/s", "Reads/s", "PerKey(B)");
  std::printf("  %-28s %10s %12s %14s %10s\n",
              "----------------------------", "----------", "------------",
              "--------------", "----------");
  for (const auto& r : results) {
    const double docs_per_sec = r.docs / r.write_seconds;
    const double reads_per_sec =
        r.read_seconds > 0 ? (r.reads / r.read_seconds) : 0.0;
    const double per_key = static_cast<double>(r.mem_delta_bytes) / r.docs;
    std::printf("  %-28s %10.2f %12.0f %14.0f %10.1f\n",
                r.scenario.c_str(), r.write_seconds, docs_per_sec,
                reads_per_sec, per_key);
  }
  std::printf("==========================================================\n");

  // CSV — easy to paste side-by-side across commits.
  std::printf("\nCSV:\n");
  std::printf("scenario,ingest_s,docs_per_s,read_s,reads_per_s,mem_bytes,per_key_bytes\n");
  for (const auto& r : results) {
    const double docs_per_sec = r.docs / r.write_seconds;
    const double reads_per_sec =
        r.read_seconds > 0 ? (r.reads / r.read_seconds) : 0.0;
    const double per_key = static_cast<double>(r.mem_delta_bytes) / r.docs;
    std::printf("\"%s\",%.3f,%.0f,%.3f,%.0f,%lld,%.1f\n", r.scenario.c_str(),
                r.write_seconds, docs_per_sec, r.read_seconds, reads_per_sec,
                static_cast<long long>(r.mem_delta_bytes), per_key);
  }
  return 0;
}
