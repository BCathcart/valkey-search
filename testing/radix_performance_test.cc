/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Performance comparison test between RadixTree and Rax implementations
 */

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/text/radix_tree.h"
#include "src/indexes/text/rax.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::indexes::text {
namespace {

// Helper to generate random strings
std::string GenerateRandomString(size_t length, std::mt19937& gen) {
  static const char charset[] =
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "0123456789";
  std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
  std::string str;
  str.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    str += charset[dist(gen)];
  }
  return str;
}

// Generate test data with various patterns
std::vector<std::string> GenerateTestData(size_t count, size_t min_len,
                                          size_t max_len, unsigned int seed) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> len_dist(min_len, max_len);
  std::vector<std::string> data;
  data.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    data.push_back(GenerateRandomString(len_dist(gen), gen));
  }
  return data;
}

// Generate test data with common prefixes (more realistic for radix trees)
std::vector<std::string> GenerateTestDataWithPrefixes(size_t count,
                                                      unsigned int seed) {
  std::mt19937 gen(seed);
  std::vector<std::string> prefixes = {"user:", "session:", "cache:", "data:",
                                       "temp:", "log:", "metric:", "event:"};
  std::uniform_int_distribution<> prefix_dist(0, prefixes.size() - 1);
  std::uniform_int_distribution<> suffix_len_dist(5, 20);
  std::vector<std::string> data;
  data.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    std::string key = prefixes[prefix_dist(gen)];
    key += GenerateRandomString(suffix_len_dist(gen), gen);
    data.push_back(key);
  }
  return data;
}

// Target type for RadixTree
struct TestTarget {
  int value;
  TestTarget() : value(-1) {}
  explicit TestTarget(int v) : value(v) {}
  explicit operator bool() const { return value != -1; }
};

// Performance test fixture
class RadixPerformanceTest : public vmsdk::ValkeyTest {
 protected:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
    radix_tree_ = RadixTree<TestTarget>{};
    rax_tree_ = raxNew();
  }

  void TearDown() override {
    if (rax_tree_) {
      raxFree(rax_tree_);
      rax_tree_ = nullptr;
    }
    vmsdk::ValkeyTest::TearDown();
  }

  // Benchmark helper
  template <typename Func>
  double BenchmarkMs(Func&& func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
  }

  void PrintHeader(const std::string& test_name) {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << test_name << "\n";
    std::cout << std::string(70, '=') << "\n";
  }

  void PrintResults(const std::string& operation, double radix_time,
                   double rax_time, size_t count) {
    double speedup = rax_time / radix_time;
    std::cout << std::left << std::setw(20) << operation << " | "
              << std::right << std::setw(10) << std::fixed
              << std::setprecision(2) << radix_time << " ms | " << std::setw(10)
              << rax_time << " ms | " << std::setw(8) << std::setprecision(2)
              << speedup << "x | " << std::setw(8) << count << " ops\n";
  }

  RadixTree<TestTarget> radix_tree_;
  rax* rax_tree_;
};

// Test 1: Sequential Insertion
TEST_F(RadixPerformanceTest, SequentialInsertion) {
  PrintHeader("Sequential Insertion Performance");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(10) << "RadixTree" << " | " << std::setw(10) << "Rax"
            << " | " << std::setw(8) << "Speedup" << " | " << std::setw(8)
            << "Count" << "\n";
  std::cout << std::string(70, '-') << "\n";

  std::vector<size_t> sizes = {1000, 5000, 10000};
  
  for (size_t size : sizes) {
    auto test_data = GenerateTestData(size, 5, 20, 42);

    // RadixTree insertion
    radix_tree_ = RadixTree<TestTarget>{};
    double radix_time = BenchmarkMs([&]() {
      for (size_t i = 0; i < test_data.size(); ++i) {
        radix_tree_.SetTarget(test_data[i], TestTarget(i));
      }
    });

    // Rax insertion
    raxFree(rax_tree_);
    rax_tree_ = raxNew();
    double rax_time = BenchmarkMs([&]() {
      for (size_t i = 0; i < test_data.size(); ++i) {
        int* value = new int(i);
        raxInsert(rax_tree_,
                  reinterpret_cast<unsigned char*>(
                      const_cast<char*>(test_data[i].c_str())),
                  test_data[i].length(), value, nullptr);
      }
    });

    PrintResults("Insert " + std::to_string(size), radix_time, rax_time, size);

    // Cleanup Rax values
    raxIterator iter;
    raxStart(&iter, rax_tree_);
    raxSeek(&iter, "^", nullptr, 0);
    while (raxNext(&iter)) {
      delete static_cast<int*>(iter.data);
    }
    raxStop(&iter);
  }
}

// Test 2: Insertion with Common Prefixes
TEST_F(RadixPerformanceTest, InsertionWithPrefixes) {
  PrintHeader("Insertion with Common Prefixes");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(10) << "RadixTree" << " | " << std::setw(10) << "Rax"
            << " | " << std::setw(8) << "Speedup" << " | " << std::setw(8)
            << "Count" << "\n";
  std::cout << std::string(70, '-') << "\n";

  std::vector<size_t> sizes = {1000, 5000, 10000};
  
  for (size_t size : sizes) {
    auto test_data = GenerateTestDataWithPrefixes(size, 42);

    // RadixTree insertion
    radix_tree_ = RadixTree<TestTarget>{};
    double radix_time = BenchmarkMs([&]() {
      for (size_t i = 0; i < test_data.size(); ++i) {
        radix_tree_.SetTarget(test_data[i], TestTarget(i));
      }
    });

    // Rax insertion
    raxFree(rax_tree_);
    rax_tree_ = raxNew();
    double rax_time = BenchmarkMs([&]() {
      for (size_t i = 0; i < test_data.size(); ++i) {
        int* value = new int(i);
        raxInsert(rax_tree_,
                  reinterpret_cast<unsigned char*>(
                      const_cast<char*>(test_data[i].c_str())),
                  test_data[i].length(), value, nullptr);
      }
    });

    PrintResults("Prefix " + std::to_string(size), radix_time, rax_time, size);

    // Cleanup Rax values
    raxIterator iter;
    raxStart(&iter, rax_tree_);
    raxSeek(&iter, "^", nullptr, 0);
    while (raxNext(&iter)) {
      delete static_cast<int*>(iter.data);
    }
    raxStop(&iter);
  }
}

// Test 3: Lookup Performance
TEST_F(RadixPerformanceTest, LookupPerformance) {
  PrintHeader("Lookup Performance");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(10) << "RadixTree" << " | " << std::setw(10) << "Rax"
            << " | " << std::setw(8) << "Speedup" << " | " << std::setw(8)
            << "Count" << "\n";
  std::cout << std::string(70, '-') << "\n";

  size_t size = 10000;
  auto test_data = GenerateTestData(size, 5, 20, 42);

  // Insert into RadixTree
  for (size_t i = 0; i < test_data.size(); ++i) {
    radix_tree_.SetTarget(test_data[i], TestTarget(i));
  }

  // Insert into Rax
  for (size_t i = 0; i < test_data.size(); ++i) {
    int* value = new int(i);
    raxInsert(rax_tree_,
              reinterpret_cast<unsigned char*>(
                  const_cast<char*>(test_data[i].c_str())),
              test_data[i].length(), value, nullptr);
  }

  // Shuffle for random access
  std::vector<std::string> shuffled_data = test_data;
  std::mt19937 gen(42);
  std::shuffle(shuffled_data.begin(), shuffled_data.end(), gen);

  // RadixTree lookup
  size_t radix_found = 0;
  double radix_time = BenchmarkMs([&]() {
    for (const auto& key : shuffled_data) {
      bool found = false;
      radix_tree_.MutateTarget(key, [&found](auto target) {
        found = static_cast<bool>(target);
        return target;
      });
      if (found) radix_found++;
    }
  });

  // Rax lookup
  size_t rax_found = 0;
  double rax_time = BenchmarkMs([&]() {
    for (const auto& key : shuffled_data) {
      void* result = raxFind(
          rax_tree_,
          reinterpret_cast<unsigned char*>(const_cast<char*>(key.c_str())),
          key.length());
      if (result != raxNotFound) rax_found++;
    }
  });

  PrintResults("Lookup", radix_time, rax_time, size);
  EXPECT_EQ(radix_found, rax_found);

  // Cleanup Rax values
  raxIterator iter;
  raxStart(&iter, rax_tree_);
  raxSeek(&iter, "^", nullptr, 0);
  while (raxNext(&iter)) {
    delete static_cast<int*>(iter.data);
  }
  raxStop(&iter);
}

// Test 4: Iteration Performance
TEST_F(RadixPerformanceTest, IterationPerformance) {
  PrintHeader("Full Iteration Performance");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(10) << "RadixTree" << " | " << std::setw(10) << "Rax"
            << " | " << std::setw(8) << "Speedup" << " | " << std::setw(8)
            << "Count" << "\n";
  std::cout << std::string(70, '-') << "\n";

  size_t size = 10000;
  auto test_data = GenerateTestData(size, 5, 20, 42);

  // Insert into RadixTree
  for (size_t i = 0; i < test_data.size(); ++i) {
    radix_tree_.SetTarget(test_data[i], TestTarget(i));
  }

  // Insert into Rax
  for (size_t i = 0; i < test_data.size(); ++i) {
    int* value = new int(i);
    raxInsert(rax_tree_,
              reinterpret_cast<unsigned char*>(
                  const_cast<char*>(test_data[i].c_str())),
              test_data[i].length(), value, nullptr);
  }

  // RadixTree iteration
  size_t radix_count = 0;
  double radix_time = BenchmarkMs([&]() {
    auto iter = radix_tree_.GetWordIterator("");
    while (!iter.Done()) {
      radix_count++;
      iter.Next();
    }
  });

  // Rax iteration
  size_t rax_count = 0;
  double rax_time = BenchmarkMs([&]() {
    raxIterator iter;
    raxStart(&iter, rax_tree_);
    raxSeek(&iter, "^", nullptr, 0);
    while (raxNext(&iter)) {
      rax_count++;
    }
    raxStop(&iter);
  });

  PrintResults("Iterate", radix_time, rax_time, size);
  EXPECT_EQ(radix_count, rax_count);

  // Cleanup Rax values
  raxIterator iter;
  raxStart(&iter, rax_tree_);
  raxSeek(&iter, "^", nullptr, 0);
  while (raxNext(&iter)) {
    delete static_cast<int*>(iter.data);
  }
  raxStop(&iter);
}

// Test 5: Prefix Iteration Performance
TEST_F(RadixPerformanceTest, PrefixIterationPerformance) {
  PrintHeader("Prefix Iteration Performance");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(10) << "RadixTree" << " | " << std::setw(10) << "Rax"
            << " | " << std::setw(8) << "Speedup" << " | " << std::setw(8)
            << "Count" << "\n";
  std::cout << std::string(70, '-') << "\n";

  size_t size = 10000;
  auto test_data = GenerateTestDataWithPrefixes(size, 42);

  // Insert into RadixTree
  for (size_t i = 0; i < test_data.size(); ++i) {
    radix_tree_.SetTarget(test_data[i], TestTarget(i));
  }

  // Insert into Rax
  for (size_t i = 0; i < test_data.size(); ++i) {
    int* value = new int(i);
    raxInsert(rax_tree_,
              reinterpret_cast<unsigned char*>(
                  const_cast<char*>(test_data[i].c_str())),
              test_data[i].length(), value, nullptr);
  }

  std::string prefix = "user:";

  // RadixTree prefix iteration
  size_t radix_count = 0;
  double radix_time = BenchmarkMs([&]() {
    auto iter = radix_tree_.GetWordIterator(prefix);
    while (!iter.Done()) {
      radix_count++;
      iter.Next();
    }
  });

  // Rax prefix iteration
  size_t rax_count = 0;
  double rax_time = BenchmarkMs([&]() {
    raxIterator iter;
    raxStart(&iter, rax_tree_);
    raxSeek(&iter, ">=",
            reinterpret_cast<unsigned char*>(const_cast<char*>(prefix.c_str())),
            prefix.length());
    while (raxNext(&iter)) {
      if (std::string(reinterpret_cast<char*>(iter.key), iter.key_len)
              .find(prefix) != 0) {
        break;
      }
      rax_count++;
    }
    raxStop(&iter);
  });

  PrintResults("Prefix '" + prefix + "'", radix_time, rax_time, radix_count);

  // Cleanup Rax values
  raxIterator iter;
  raxStart(&iter, rax_tree_);
  raxSeek(&iter, "^", nullptr, 0);
  while (raxNext(&iter)) {
    delete static_cast<int*>(iter.data);
  }
  raxStop(&iter);
}

// Test 6: Mixed Operations
TEST_F(RadixPerformanceTest, MixedOperations) {
  PrintHeader("Mixed Operations (50% Insert, 30% Lookup, 20% Delete)");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(10) << "RadixTree" << " | " << std::setw(10) << "Rax"
            << " | " << std::setw(8) << "Speedup" << " | " << std::setw(8)
            << "Count" << "\n";
  std::cout << std::string(70, '-') << "\n";

  size_t operation_count = 10000;
  auto test_data = GenerateTestData(operation_count, 5, 20, 42);
  std::mt19937 gen(42);
  std::uniform_int_distribution<> op_dist(0, 99);

  // RadixTree mixed operations
  radix_tree_ = RadixTree<TestTarget>{};
  std::vector<std::string> inserted_radix;
  double radix_time = BenchmarkMs([&]() {
    for (size_t i = 0; i < operation_count; ++i) {
      int op = op_dist(gen);
      if (op < 50) {  // Insert
        radix_tree_.SetTarget(test_data[i], TestTarget(i));
        inserted_radix.push_back(test_data[i]);
      } else if (op < 80 && !inserted_radix.empty()) {  // Lookup
        size_t idx = gen() % inserted_radix.size();
        radix_tree_.MutateTarget(inserted_radix[idx],
                                [](auto target) { return target; });
      } else if (!inserted_radix.empty()) {  // Delete
        size_t idx = gen() % inserted_radix.size();
        radix_tree_.SetTarget(inserted_radix[idx], TestTarget{});
        inserted_radix.erase(inserted_radix.begin() + idx);
      }
    }
  });

  // Rax mixed operations
  raxFree(rax_tree_);
  rax_tree_ = raxNew();
  gen.seed(42);  // Reset for same sequence
  std::vector<std::string> inserted_rax;
  double rax_time = BenchmarkMs([&]() {
    for (size_t i = 0; i < operation_count; ++i) {
      int op = op_dist(gen);
      if (op < 50) {  // Insert
        int* value = new int(i);
        raxInsert(rax_tree_,
                  reinterpret_cast<unsigned char*>(
                      const_cast<char*>(test_data[i].c_str())),
                  test_data[i].length(), value, nullptr);
        inserted_rax.push_back(test_data[i]);
      } else if (op < 80 && !inserted_rax.empty()) {  // Lookup
        size_t idx = gen() % inserted_rax.size();
        raxFind(rax_tree_,
                reinterpret_cast<unsigned char*>(
                    const_cast<char*>(inserted_rax[idx].c_str())),
                inserted_rax[idx].length());
      } else if (!inserted_rax.empty()) {  // Delete
        size_t idx = gen() % inserted_rax.size();
        void* old = nullptr;
        raxRemove(rax_tree_,
                  reinterpret_cast<unsigned char*>(
                      const_cast<char*>(inserted_rax[idx].c_str())),
                  inserted_rax[idx].length(), &old);
        if (old) delete static_cast<int*>(old);
        inserted_rax.erase(inserted_rax.begin() + idx);
      }
    }
  });

  PrintResults("Mixed ops", radix_time, rax_time, operation_count);

  // Cleanup Rax values
  raxIterator iter;
  raxStart(&iter, rax_tree_);
  raxSeek(&iter, "^", nullptr, 0);
  while (raxNext(&iter)) {
    delete static_cast<int*>(iter.data);
  }
  raxStop(&iter);
}

// Test 7: Memory Efficiency (via node count)
TEST_F(RadixPerformanceTest, MemoryEfficiency) {
  PrintHeader("Memory Efficiency (Node Count Comparison)");
  std::cout << std::left << std::setw(20) << "Operation" << " | "
            << std::setw(12) << "RadixTree" << " | " << std::setw(12) << "Rax"
            << " | " << std::setw(15) << "Keys\n";
  std::cout << std::string(70, '-') << "\n";

  std::vector<size_t> sizes = {1000, 5000, 10000};

  for (size_t size : sizes) {
    auto test_data = GenerateTestDataWithPrefixes(size, 42);

    // Insert into RadixTree
    radix_tree_ = RadixTree<TestTarget>{};
    for (size_t i = 0; i < test_data.size(); ++i) {
      radix_tree_.SetTarget(test_data[i], TestTarget(i));
    }

    // Insert into Rax
    raxFree(rax_tree_);
    rax_tree_ = raxNew();
    for (size_t i = 0; i < test_data.size(); ++i) {
      int* value = new int(i);
      raxInsert(rax_tree_,
                reinterpret_cast<unsigned char*>(
                    const_cast<char*>(test_data[i].c_str())),
                test_data[i].length(), value, nullptr);
    }

    std::cout << std::left << std::setw(20) << ("Size " + std::to_string(size))
              << " | " << std::right << std::setw(12) << "N/A"
              << " | " << std::setw(12) << rax_tree_->numnodes << " | "
              << std::setw(15) << rax_tree_->numele << "\n";

    // Cleanup Rax values
    raxIterator iter;
    raxStart(&iter, rax_tree_);
    raxSeek(&iter, "^", nullptr, 0);
    while (raxNext(&iter)) {
      delete static_cast<int*>(iter.data);
    }
    raxStop(&iter);
  }
}

}  // namespace
}  // namespace valkey_search::indexes::text
