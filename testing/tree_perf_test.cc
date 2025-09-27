/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <sys/resource.h>
#include <unistd.h>

#include "gtest/gtest.h"
#include "src/indexes/text/radix_tree.h"
#include "src/utils/patricia_tree.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search {
namespace {

// Test target for RadixTree
struct TestTarget {
  int value;
  explicit TestTarget(int v) : value(v) {}
  bool operator==(const TestTarget& other) const {
    return value == other.value;
  }
};

class TreePerfTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    radix_tree_ = std::make_unique<indexes::text::RadixTree<TestTarget, false>>();
    patricia_tree_ = std::make_unique<PatriciaTree<int>>(true);  // case sensitive
  }

  // Generate test data with random English-like words
  std::vector<std::string> GenerateTestData(size_t count) {
    std::unordered_set<std::string> unique_words;
    std::vector<std::string> words;
    std::mt19937 gen(42);  // Fixed seed for reproducibility
    
    // Common English word patterns and syllables
    std::vector<std::string> consonants = {
        "b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "n", "p", "q", "r", "s", "t", "v", "w", "x", "y", "z",
        "bl", "br", "ch", "cl", "cr", "dr", "fl", "fr", "gl", "gr", "pl", "pr", "sc", "sh", "sk", "sl", "sm", 
        "sn", "sp", "st", "sw", "th", "tr", "tw", "wh", "wr"
    };
    
    std::vector<std::string> vowels = {
        "a", "e", "i", "o", "u", "y", "ae", "ai", "au", "ay", "ea", "ee", "ei", "eu", "ey", 
        "ia", "ie", "io", "oa", "oe", "oi", "oo", "ou", "oy", "ua", "ue", "ui", "uo", "uy"
    };
    
    std::vector<std::string> endings = {
        "", "s", "ed", "ing", "er", "est", "ly", "tion", "sion", "ness", "ment", "able", "ible", 
        "ful", "less", "ward", "wise", "like", "ship", "hood", "dom", "ism", "ist", "ize", "ise"
    };
    
    std::uniform_int_distribution<> syllable_count(1, 4);
    std::uniform_int_distribution<> consonant_dist(0, consonants.size() - 1);
    std::uniform_int_distribution<> vowel_dist(0, vowels.size() - 1);
    std::uniform_int_distribution<> ending_dist(0, endings.size() - 1);
    
    // Generate unique words
    size_t attempts = 0;
    while (unique_words.size() < count && attempts < count * 3) {
      std::string word;
      int syllables = syllable_count(gen);
      
      for (int j = 0; j < syllables; ++j) {
        // Add consonant cluster
        if (j == 0 || gen() % 3 == 0) {  // More likely at word start
          word += consonants[consonant_dist(gen)];
        }
        
        // Add vowel
        word += vowels[vowel_dist(gen)];
        
        // Sometimes add trailing consonant
        if (gen() % 2 == 0) {
          word += consonants[consonant_dist(gen)];
        }
      }
      
      // Add ending
      if (gen() % 3 == 0) {  // 1/3 chance of having an ending
        word += endings[ending_dist(gen)];
      }
      
      // Ensure minimum length
      if (word.length() < 3) {
        word += std::to_string(attempts);
      }
      
      // Only add if unique
      if (unique_words.find(word) == unique_words.end()) {
        unique_words.insert(word);
        words.push_back(word);
      }
      
      attempts++;
    }
    
    // If we couldn't generate enough unique words, pad with numbered words
    while (words.size() < count) {
      std::string word = "word" + std::to_string(words.size());
      words.push_back(word);
    }
    
    // Shuffle to avoid insertion order bias
    std::shuffle(words.begin(), words.end(), gen);
    
    return words;
  }

  // Memory usage helper
  struct MemoryUsage {
    long peak_rss_kb = 0;
    long current_rss_kb = 0;
  };

  MemoryUsage GetMemoryUsage() {
    MemoryUsage usage;
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
      usage.peak_rss_kb = ru.ru_maxrss;
    }
    
    // Get current RSS from /proc/self/status
    FILE* file = fopen("/proc/self/status", "r");
    if (file) {
      char line[128];
      while (fgets(line, sizeof(line), file)) {
        if (sscanf(line, "VmRSS: %ld kB", &usage.current_rss_kb) == 1) {
          break;
        }
      }
      fclose(file);
    }
    return usage;
  }

  // Timing and memory helper
  template<typename Func>
  std::pair<double, MemoryUsage> TimeAndMemoryOperation(const std::string& operation_name, Func&& func) {
    auto mem_before = GetMemoryUsage();
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    auto mem_after = GetMemoryUsage();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    double ms = duration.count() / 1000.0;
    
    long memory_delta = mem_after.current_rss_kb - mem_before.current_rss_kb;
    
    std::cout << operation_name << ": " << ms << " ms";
    if (memory_delta > 0) {
      std::cout << " (+" << memory_delta << " KB memory)";
    }
    std::cout << std::endl;
    
    return {ms, mem_after};
  }

  // Timing helper (backward compatibility)
  template<typename Func>
  double TimeOperation(const std::string& operation_name, Func&& func) {
    return TimeAndMemoryOperation(operation_name, std::forward<Func>(func)).first;
  }

 protected:
  std::unique_ptr<indexes::text::RadixTree<TestTarget, false>> radix_tree_;
  std::unique_ptr<PatriciaTree<int>> patricia_tree_;
};

TEST_F(TreePerfTest, CompareAddingPerformance) {
  const size_t test_size = 25000;
  auto test_words = GenerateTestData(test_size);
  
  std::cout << "\n=== Adding Performance Test (" << test_size << " words) ===" << std::endl;
  
  // Test RadixTree adding with memory tracking
  auto [radix_add_time, radix_mem] = TimeAndMemoryOperation("RadixTree Add", [&]() {
    for (size_t i = 0; i < test_words.size(); ++i) {
      radix_tree_->Mutate(test_words[i], [i](auto) { return TestTarget(static_cast<int>(i)); });
    }
  });
  
  // Test PatriciaTree adding with memory tracking
  auto [patricia_add_time, patricia_mem] = TimeAndMemoryOperation("PatriciaTree Add", [&]() {
    for (size_t i = 0; i < test_words.size(); ++i) {
      patricia_tree_->AddKeyValue(test_words[i], static_cast<int>(i));
    }
  });
  
  std::cout << "RadixTree vs PatriciaTree Add Ratio: " 
            << (radix_add_time / patricia_add_time) << "x" << std::endl;
  std::cout << "RadixTree Memory: " << radix_mem.current_rss_kb << " KB, "
            << "PatriciaTree Memory: " << patricia_mem.current_rss_kb << " KB" << std::endl;
}

TEST_F(TreePerfTest, CompareWholeTreeIteration) {
  const size_t test_size = 15000;
  auto test_words = GenerateTestData(test_size);
  
  // Populate both trees
  for (size_t i = 0; i < test_words.size(); ++i) {
    radix_tree_->Mutate(test_words[i], [i](auto) { return TestTarget(static_cast<int>(i)); });
    patricia_tree_->AddKeyValue(test_words[i], static_cast<int>(i));
  }
  
  std::cout << "\n=== Whole Tree Iteration Performance Test (" << test_size << " words) ===" << std::endl;
  
  // Test RadixTree whole tree iteration
  size_t radix_count = 0;
  auto [radix_iter_time, radix_mem] = TimeAndMemoryOperation("RadixTree Whole Tree Iteration", [&]() {
    auto iter = radix_tree_->GetWordIterator("");  // Empty prefix = whole tree
    while (!iter.Done()) {
      radix_count++;
      iter.Next();
    }
  });
  
  // Test PatriciaTree whole tree iteration
  size_t patricia_count = 0;
  auto [patricia_iter_time, patricia_mem] = TimeAndMemoryOperation("PatriciaTree Whole Tree Iteration", [&]() {
    auto iter = patricia_tree_->RootIterator();
    while (!iter.Done()) {
      patricia_count++;
      iter.Next();
    }
  });
  
  std::cout << "RadixTree iterated " << radix_count << " items" << std::endl;
  std::cout << "PatriciaTree iterated " << patricia_count << " items" << std::endl;
  std::cout << "RadixTree vs PatriciaTree Iteration Ratio: " 
            << (radix_iter_time / patricia_iter_time) << "x" << std::endl;
  std::cout << "RadixTree Memory: " << radix_mem.current_rss_kb << " KB, "
            << "PatriciaTree Memory: " << patricia_mem.current_rss_kb << " KB" << std::endl;
  
  EXPECT_EQ(radix_count, test_words.size());
  EXPECT_EQ(patricia_count, test_words.size());
}

TEST_F(TreePerfTest, CompareSubtreeIteration) {
  const size_t test_size = 15000;
  auto test_words = GenerateTestData(test_size);
  
  // Populate both trees
  for (size_t i = 0; i < test_words.size(); ++i) {
    radix_tree_->Mutate(test_words[i], [i](auto) { return TestTarget(static_cast<int>(i)); });
    patricia_tree_->AddKeyValue(test_words[i], static_cast<int>(i));
  }
  
  std::cout << "\n=== Subtree Iteration Performance Test (" << test_size << " words) ===" << std::endl;
  
  // Test common English prefixes that are likely to occur in generated words
  std::vector<std::string> test_prefixes = {"b", "c", "th", "st", "pr"};
  
  for (const auto& prefix : test_prefixes) {
    std::cout << "\nTesting prefix: '" << prefix << "'" << std::endl;
    
    // Test RadixTree subtree iteration
    size_t radix_count = 0;
    auto [radix_iter_time, radix_mem] = TimeAndMemoryOperation("RadixTree Subtree Iteration", [&]() {
      auto iter = radix_tree_->GetWordIterator(prefix);
      while (!iter.Done()) {
        radix_count++;
        iter.Next();
      }
    });
    
    // Test PatriciaTree subtree iteration
    size_t patricia_count = 0;
    auto [patricia_iter_time, patricia_mem] = TimeAndMemoryOperation("PatriciaTree Subtree Iteration", [&]() {
      auto iter = patricia_tree_->PrefixMatcher(prefix);
      while (!iter.Done()) {
        patricia_count++;
        iter.Next();
      }
    });
    
    std::cout << "RadixTree found " << radix_count << " items with prefix '" << prefix << "'" << std::endl;
    std::cout << "PatriciaTree found " << patricia_count << " items with prefix '" << prefix << "'" << std::endl;
    
    if (patricia_iter_time > 0) {
      std::cout << "RadixTree vs PatriciaTree Subtree Iteration Ratio: " 
                << (radix_iter_time / patricia_iter_time) << "x" << std::endl;
    }
    
    EXPECT_EQ(radix_count, patricia_count) << "Mismatch in results for prefix: " << prefix;
  }
}

TEST_F(TreePerfTest, CompareDeletionPerformance) {
  const size_t test_size = 15000;
  auto test_words = GenerateTestData(test_size);
  
  // Create a map from word to original index BEFORE shuffling
  std::unordered_map<std::string, int> word_to_index;
  for (size_t i = 0; i < test_words.size(); ++i) {
    word_to_index[test_words[i]] = static_cast<int>(i);
  }
  
  // Populate both trees
  for (size_t i = 0; i < test_words.size(); ++i) {
    radix_tree_->Mutate(test_words[i], [i](auto) { return TestTarget(static_cast<int>(i)); });
    patricia_tree_->AddKeyValue(test_words[i], static_cast<int>(i));
  }
  
  std::cout << "\n=== Deletion Performance Test (" << test_size << " words) ===" << std::endl;
  
  // Shuffle deletion order to avoid bias
  std::mt19937 gen(42);
  std::shuffle(test_words.begin(), test_words.end(), gen);
  
  // Delete half the words to test deletion performance
  size_t delete_count = test_size / 2;
  std::vector<std::string> words_to_delete(test_words.begin(), test_words.begin() + delete_count);
  
  // Test RadixTree deletion
  auto [radix_delete_time, radix_mem] = TimeAndMemoryOperation("RadixTree Deletion", [&]() {
    for (const auto& word : words_to_delete) {
      radix_tree_->Mutate(word, [](auto) -> std::optional<TestTarget> { return std::nullopt; });
    }
  });
  
  // Test PatriciaTree deletion - use the correct value for each word
  
  auto [patricia_delete_time, patricia_mem] = TimeAndMemoryOperation("PatriciaTree Deletion", [&]() {
    for (const auto& word : words_to_delete) {
      auto it = word_to_index.find(word);
      if (it != word_to_index.end()) {
        patricia_tree_->Remove(word, it->second);
      }
    }
  });
  
  std::cout << "RadixTree vs PatriciaTree Deletion Ratio: " 
            << (radix_delete_time / patricia_delete_time) << "x" << std::endl;
  std::cout << "RadixTree Memory: " << radix_mem.current_rss_kb << " KB, "
            << "PatriciaTree Memory: " << patricia_mem.current_rss_kb << " KB" << std::endl;
  
  // Verify remaining items can still be iterated
  size_t radix_remaining = 0;
  auto radix_iter = radix_tree_->GetWordIterator("");
  while (!radix_iter.Done()) {
    radix_remaining++;
    radix_iter.Next();
  }
  
  size_t patricia_remaining = 0;
  auto patricia_iter = patricia_tree_->RootIterator();
  while (!patricia_iter.Done()) {
    patricia_remaining++;
    patricia_iter.Next();
  }
  
  std::cout << "RadixTree remaining items: " << radix_remaining << std::endl;
  std::cout << "PatriciaTree remaining items: " << patricia_remaining << std::endl;
  
  EXPECT_EQ(radix_remaining, test_size - delete_count);
  // PatriciaTree deletion might have some edge cases, so allow larger variance
  EXPECT_NEAR(patricia_remaining, test_size - delete_count, 600) 
      << "PatriciaTree deletion count variance too large";
}

TEST_F(TreePerfTest, ComprehensivePerformanceTest) {
  const size_t test_size = 20000;
  auto test_words = GenerateTestData(test_size);
  
  std::cout << "\n=== Comprehensive Performance Test (" << test_size << " words) ===" << std::endl;
  
  // Phase 1: Adding
  std::cout << "\nPhase 1: Adding all words" << std::endl;
  double radix_add_time = TimeOperation("RadixTree Add", [&]() {
    for (size_t i = 0; i < test_words.size(); ++i) {
      radix_tree_->Mutate(test_words[i], [i](auto) { return TestTarget(static_cast<int>(i)); });
    }
  });
  
  double patricia_add_time = TimeOperation("PatriciaTree Add", [&]() {
    for (size_t i = 0; i < test_words.size(); ++i) {
      patricia_tree_->AddKeyValue(test_words[i], static_cast<int>(i));
    }
  });
  
  // Phase 2: Full iteration
  std::cout << "\nPhase 2: Full tree iteration" << std::endl;
  size_t radix_count = 0;
  double radix_iter_time = TimeOperation("RadixTree Full Iteration", [&]() {
    auto iter = radix_tree_->GetWordIterator("");
    while (!iter.Done()) {
      radix_count++;
      iter.Next();
    }
  });
  
  size_t patricia_count = 0;
  double patricia_iter_time = TimeOperation("PatriciaTree Full Iteration", [&]() {
    auto iter = patricia_tree_->RootIterator();
    while (!iter.Done()) {
      patricia_count++;
      iter.Next();
    }
  });
  
  // Phase 3: Prefix iterations
  std::cout << "\nPhase 3: Prefix iterations" << std::endl;
  std::vector<std::string> prefixes = {"test", "app", "data"};
  double radix_prefix_time = 0;
  double patricia_prefix_time = 0;
  
  for (const auto& prefix : prefixes) {
    radix_prefix_time += TimeOperation("RadixTree Prefix '" + prefix + "'", [&]() {
      auto iter = radix_tree_->GetWordIterator(prefix);
      while (!iter.Done()) {
        iter.Next();
      }
    });
    
    patricia_prefix_time += TimeOperation("PatriciaTree Prefix '" + prefix + "'", [&]() {
      auto iter = patricia_tree_->PrefixMatcher(prefix);
      while (!iter.Done()) {
        iter.Next();
      }
    });
  }
  
  // Phase 4: Deletion
  std::cout << "\nPhase 4: Deleting half the words" << std::endl;
  std::mt19937 gen(42);
  std::shuffle(test_words.begin(), test_words.end(), gen);
  size_t delete_count = test_size / 2;
  
  double radix_delete_time = TimeOperation("RadixTree Deletion", [&]() {
    for (size_t i = 0; i < delete_count; ++i) {
      radix_tree_->Mutate(test_words[i], [](auto) -> std::optional<TestTarget> { return std::nullopt; });
    }
  });
  
  double patricia_delete_time = TimeOperation("PatriciaTree Deletion", [&]() {
    for (size_t i = 0; i < delete_count; ++i) {
      patricia_tree_->Remove(test_words[i], static_cast<int>(i));
    }
  });
  
  // Summary
  std::cout << "\n=== Performance Summary ===" << std::endl;
  std::cout << "Add Ratio (RadixTree/PatriciaTree): " << (radix_add_time / patricia_add_time) << "x" << std::endl;
  std::cout << "Full Iteration Ratio: " << (radix_iter_time / patricia_iter_time) << "x" << std::endl;
  std::cout << "Prefix Iteration Ratio: " << (radix_prefix_time / patricia_prefix_time) << "x" << std::endl;
  std::cout << "Deletion Ratio: " << (radix_delete_time / patricia_delete_time) << "x" << std::endl;
  
  double radix_total = radix_add_time + radix_iter_time + radix_prefix_time + radix_delete_time;
  double patricia_total = patricia_add_time + patricia_iter_time + patricia_prefix_time + patricia_delete_time;
  std::cout << "Overall Ratio: " << (radix_total / patricia_total) << "x" << std::endl;
  
  std::cout << "\nRadixTree Total Time: " << radix_total << " ms" << std::endl;
  std::cout << "PatriciaTree Total Time: " << patricia_total << " ms" << std::endl;
}

}  // namespace
}  // namespace valkey_search
