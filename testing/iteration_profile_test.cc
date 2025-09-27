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
#include <unordered_set>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>

#include "src/indexes/text/radix_tree.h"
#include "src/utils/patricia_tree.h"

namespace valkey_search {

// Test target for RadixTree
struct TestTarget {
  int value;
  explicit TestTarget(int v) : value(v) {}
  bool operator==(const TestTarget& other) const {
    return value == other.value;
  }
};

// Helper class to manage perf recording
class PerfRecorder {
private:
  pid_t perf_pid_;
  std::string output_file_;
  
public:
  PerfRecorder(const std::string& test_name) : perf_pid_(-1) {
    output_file_ = test_name + "_perf.data";
  }
  
  bool StartRecording() {
    std::cout << "Starting perf recording to " << output_file_ << "..." << std::endl;
    
    // Fork to start perf record
    perf_pid_ = fork();
    if (perf_pid_ == 0) {
      // Child process - exec perf record
      std::string pid_str = std::to_string(getppid());
      execl("/usr/bin/perf", "perf", "record", 
            "-g", "--call-graph=dwarf", 
            "-F", "4000",
            "-o", output_file_.c_str(),
            "-p", pid_str.c_str(),
            nullptr);
      
      // If execl fails
      std::cerr << "Failed to start perf record" << std::endl;
      exit(1);
    } else if (perf_pid_ > 0) {
      // Parent process - give perf time to start
      sleep(1);
      std::cout << "Perf recording started (PID: " << perf_pid_ << ")" << std::endl;
      return true;
    } else {
      std::cerr << "Failed to fork for perf record" << std::endl;
      return false;
    }
  }
  
  void StopRecording() {
    if (perf_pid_ > 0) {
      std::cout << "Stopping perf recording..." << std::endl;
      kill(perf_pid_, SIGTERM);
      
      int status;
      waitpid(perf_pid_, &status, 0);
      perf_pid_ = -1;
      
      std::cout << "Perf recording stopped. Data saved to " << output_file_ << std::endl;
      
      // Automatically generate the perf report
      GenerateReport();
    }
  }
  
  ~PerfRecorder() {
    StopRecording();
  }
  
private:
  void GenerateReport() {
    std::string report_file = test_name_from_file(output_file_) + "_report.txt";
    std::cout << "Generating perf report to " << report_file << "..." << std::endl;
    
    // Fork to run perf report
    pid_t report_pid = fork();
    if (report_pid == 0) {
      // Child process - redirect stdout to file and exec perf report
      freopen(report_file.c_str(), "w", stdout);
      execl("/usr/bin/perf", "perf", "report", 
            "-i", output_file_.c_str(),
            "--stdio", "-f",
            nullptr);
      
      // If execl fails
      std::cerr << "Failed to generate perf report" << std::endl;
      exit(1);
    } else if (report_pid > 0) {
      // Parent process - wait for report generation
      int status;
      waitpid(report_pid, &status, 0);
      
      if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        std::cout << "Perf report generated successfully: " << report_file << std::endl;
      } else {
        std::cout << "Warning: Failed to generate perf report automatically" << std::endl;
        std::cout << "Generate manually with: perf report -i " << output_file_ << " --stdio -f > " 
                  << report_file << std::endl;
      }
    } else {
      std::cout << "Warning: Failed to fork for perf report generation" << std::endl;
      std::cout << "Generate manually with: perf report -i " << output_file_ << " --stdio -f > " 
                << report_file << std::endl;
    }
  }
  
  std::string test_name_from_file(const std::string& filename) {
    size_t pos = filename.find("_perf.data");
    if (pos != std::string::npos) {
      return filename.substr(0, pos);
    }
    return filename;
  }
};

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

void ProfileRadixTreeIteration(size_t test_size, int num_runs) {
  std::cout << "=== Profiling RadixTree Iteration ===" << std::endl;
  
  auto test_words = GenerateTestData(test_size);
  
  // Create and populate RadixTree
  auto radix_tree = std::make_unique<indexes::text::RadixTree<TestTarget, false>>();
  for (size_t i = 0; i < test_words.size(); ++i) {
    radix_tree->Mutate(test_words[i], [i](auto) { return TestTarget(static_cast<int>(i)); });
  }
  
  std::cout << "Starting RadixTree iteration of " << test_size << " words for " << num_runs << " runs..." << std::endl;
  
  // Start perf recording
  PerfRecorder perf_recorder("radix_tree_iteration");
  if (!perf_recorder.StartRecording()) {
    std::cout << "Warning: Failed to start perf recording, continuing without profiling..." << std::endl;
  }
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Perform iteration multiple times for better profiling data
  size_t total_count = 0;
  for (int run = 0; run < num_runs; ++run) {
    auto iter = radix_tree->GetWordIterator("");
    size_t count = 0;
    while (!iter.Done()) {
      count++;
      iter.Next();
    }
    total_count += count;
    
    // Print progress every 10 runs for longer tests
    if (num_runs > 20 && (run + 1) % 10 == 0) {
      std::cout << "Completed " << (run + 1) << "/" << num_runs << " runs..." << std::endl;
    }
  }
  
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // Stop perf recording (destructor will handle it, but explicit is better)
  perf_recorder.StopRecording();
  
  std::cout << "RadixTree completed: " << total_count << " total iterations in " 
            << duration.count() << "ms" << std::endl;
  std::cout << "Average iterations per second: " 
            << (total_count * 1000.0 / duration.count()) << std::endl;
}

void ProfilePatriciaTreeIteration(size_t test_size, int num_runs) {
  std::cout << "=== Profiling PatriciaTree Iteration ===" << std::endl;
  
  auto test_words = GenerateTestData(test_size);
  
  // Create and populate PatriciaTree
  auto patricia_tree = std::make_unique<PatriciaTree<int>>(true);
  for (size_t i = 0; i < test_words.size(); ++i) {
    patricia_tree->AddKeyValue(test_words[i], static_cast<int>(i));
  }
  
  std::cout << "Starting PatriciaTree iteration of " << test_size << " words for " << num_runs << " runs..." << std::endl;
  
  // Start perf recording
  PerfRecorder perf_recorder("patricia_tree_iteration");
  if (!perf_recorder.StartRecording()) {
    std::cout << "Warning: Failed to start perf recording, continuing without profiling..." << std::endl;
  }
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Perform iteration multiple times for better profiling data
  size_t total_count = 0;
  for (int run = 0; run < num_runs; ++run) {
    auto iter = patricia_tree->RootIterator();
    size_t count = 0;
    while (!iter.Done()) {
      count++;
      iter.Next();
    }
    total_count += count;
    
    // Print progress every 10 runs for longer tests
    if (num_runs > 20 && (run + 1) % 10 == 0) {
      std::cout << "Completed " << (run + 1) << "/" << num_runs << " runs..." << std::endl;
    }
  }
  
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // Stop perf recording
  perf_recorder.StopRecording();
  
  std::cout << "PatriciaTree completed: " << total_count << " total iterations in " 
            << duration.count() << "ms" << std::endl;
  std::cout << "Average iterations per second: " 
            << (total_count * 1000.0 / duration.count()) << std::endl;
}

}  // namespace valkey_search

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " [radix|patricia] [test_size] [num_runs]" << std::endl;
    std::cout << "  test_size: number of words to test (default: 100000)" << std::endl;
    std::cout << "  num_runs: number of iteration runs (default: 50)" << std::endl;
    return 1;
  }
  
  std::string test_type = argv[1];
  size_t test_size = (argc > 2) ? std::stoul(argv[2]) : 100000;
  int num_runs = (argc > 3) ? std::stoi(argv[3]) : 50;
  
  std::cout << "Configuration: " << test_size << " words, " << num_runs << " runs" << std::endl;
  
  if (test_type == "radix") {
    valkey_search::ProfileRadixTreeIteration(test_size, num_runs);
  } else if (test_type == "patricia") {
    valkey_search::ProfilePatriciaTreeIteration(test_size, num_runs);
  } else {
    std::cout << "Invalid test type. Use 'radix' or 'patricia'" << std::endl;
    return 1;
  }
  
  return 0;
}
