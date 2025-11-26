// /*
//  * Copyright (c) 2025, valkey-search contributors
//  * All rights reserved.
//  * SPDX-License-Identifier: BSD 3-Clause
//  *
//  */

// #include <malloc.h>

// #include <iostream>
// #include <memory>
// #include <optional>

// #include "gtest/gtest.h"
// #include "src/indexes/text/invasive_ptr.h"
// #include "src/indexes/text/posting.h"
// #include "vmsdk/src/memory_allocation.h"
// #include "vmsdk/src/memory_tracker.h"
// #include "vmsdk/src/testing_infra/module.h"
// #include "vmsdk/src/testing_infra/utils.h"

// namespace valkey_search::indexes::text {

// // Simple test structure
// struct TestData {
//   int value1;
//   int value2;
//   double value3;
// };

// TEST(InvasivePtrSizeofTest, CompareMemoryFootprint) {
//   using SharedPtrOptional = std::optional<std::shared_ptr<TestData>>;
//   using IntrusivePtrType = InvasivePtr<TestData>;

//   size_t optional_shared_ptr_size = sizeof(SharedPtrOptional);
//   size_t invasive_ptr_size = sizeof(IntrusivePtrType);

//   // Output the sizes for comparison
//   std::cout << "\n=== Memory Size Comparison ===" << std::endl;
//   std::cout << "sizeof(TestData): " << sizeof(TestData) << " bytes" << std::endl;
//   std::cout << "sizeof(std::shared_ptr<TestData>): " 
//             << sizeof(std::shared_ptr<TestData>) << " bytes" << std::endl;
//   std::cout << "sizeof(std::optional<std::shared_ptr<TestData>>): " 
//             << optional_shared_ptr_size << " bytes" << std::endl;
//   std::cout << "sizeof(InvasivePtr<TestData>): " 
//             << invasive_ptr_size << " bytes" << std::endl;
  
//   size_t memory_savings = optional_shared_ptr_size - invasive_ptr_size;
//   double percentage = (static_cast<double>(memory_savings) / optional_shared_ptr_size) * 100.0;
  
//   std::cout << "\nMemory savings: " << memory_savings 
//             << " bytes (" << percentage << "%)" << std::endl;
//   std::cout << "==============================\n" << std::endl;

//   // Verify that InvasivePtr is more memory efficient
//   EXPECT_LT(invasive_ptr_size, optional_shared_ptr_size)
//       << "InvasivePtr should be more memory efficient than optional<shared_ptr>";

//   // On 64-bit systems, InvasivePtr should be exactly pointer-sized (8 bytes)
//   EXPECT_EQ(invasive_ptr_size, sizeof(void*))
//       << "InvasivePtr should be pointer-sized";
// }

// TEST(InvasivePtrSizeofTest, MultipleTypeSizes) {
//   struct Small { char c; };
//   struct Medium { int data[4]; };
//   struct Large { char data[1024]; };

//   std::cout << "\n=== Size Comparison for Different Types ===" << std::endl;
  
//   // Small type
//   std::cout << "\nSmall (1 byte payload):" << std::endl;
//   std::cout << "  optional<shared_ptr<Small>>: " 
//             << sizeof(std::optional<std::shared_ptr<Small>>) << " bytes" << std::endl;
//   std::cout << "  InvasivePtr<Small>: " 
//             << sizeof(InvasivePtr<Small>) << " bytes" << std::endl;

//   // Medium type
//   std::cout << "\nMedium (16 byte payload):" << std::endl;
//   std::cout << "  optional<shared_ptr<Medium>>: " 
//             << sizeof(std::optional<std::shared_ptr<Medium>>) << " bytes" << std::endl;
//   std::cout << "  InvasivePtr<Medium>: " 
//             << sizeof(InvasivePtr<Medium>) << " bytes" << std::endl;

//   // Large type
//   std::cout << "\nLarge (1024 byte payload):" << std::endl;
//   std::cout << "  optional<shared_ptr<Large>>: " 
//             << sizeof(std::optional<std::shared_ptr<Large>>) << " bytes" << std::endl;
//   std::cout << "  InvasivePtr<Large>: " 
//             << sizeof(InvasivePtr<Large>) << " bytes" << std::endl;
  
//   std::cout << "\n===========================================\n" << std::endl;

//   // All InvasivePtr types should be pointer-sized regardless of payload
//   EXPECT_EQ(sizeof(InvasivePtr<Small>), sizeof(void*));
//   EXPECT_EQ(sizeof(InvasivePtr<Medium>), sizeof(void*));
//   EXPECT_EQ(sizeof(InvasivePtr<Large>), sizeof(void*));
// }

// TEST(InvasivePtrSizeofTest, ActualMemoryAllocation) {
//   // Test actual heap allocation sizes (approximate)
//   struct TestData { int x, y, z; };
  
//   // For InvasivePtr: allocates RefCountWrapper which contains:
//   // - TestData (12 bytes for 3 ints)
//   // - atomic<uint32_t> refcount (4 bytes)
//   // - padding for alignment
//   size_t invasive_heap = sizeof(TestData) + sizeof(std::atomic<uint32_t>);
  
//   // For shared_ptr: allocates control block separately:
//   // - TestData (12 bytes)
//   // - Control block (typically ~24 bytes for refcount + weak count + vtable ptr)
  
//   std::cout << "\n=== Heap Allocation Comparison ===" << std::endl;
//   std::cout << "InvasivePtr heap allocation (approx): " 
//             << invasive_heap << " bytes" << std::endl;
//   std::cout << "  (sizeof(TestData) + sizeof(atomic<uint32_t>))" << std::endl;
//   std::cout << "\nshared_ptr heap allocation (approx): "
//             << sizeof(TestData) << " + ~24 bytes control block" << std::endl;
//   std::cout << "  = ~" << (sizeof(TestData) + 24) << " bytes total" << std::endl;
//   std::cout << "\nStack size comparison:" << std::endl;
//   std::cout << "  InvasivePtr: " << sizeof(InvasivePtr<TestData>) << " bytes" << std::endl;
//   std::cout << "  optional<shared_ptr>: " 
//             << sizeof(std::optional<std::shared_ptr<TestData>>) << " bytes" << std::endl;
//   std::cout << "====================================\n" << std::endl;
// }

// TEST(InvasivePtrSizeofTest, PostingsComparison) {
//   // Test with actual Postings type since that was the change made
//   using SharedPtrOptional = std::optional<std::shared_ptr<Postings>>;
//   using IntrusivePtrType = InvasivePtr<Postings>;

//   size_t optional_shared_ptr_size = sizeof(SharedPtrOptional);
//   size_t invasive_ptr_size = sizeof(IntrusivePtrType);

//   std::cout << "\n=== Postings Memory Size Comparison ===" << std::endl;
//   std::cout << "sizeof(Postings): " << sizeof(Postings) << " bytes" << std::endl;
//   std::cout << "sizeof(std::shared_ptr<Postings>): " 
//             << sizeof(std::shared_ptr<Postings>) << " bytes" << std::endl;
//   std::cout << "sizeof(std::optional<std::shared_ptr<Postings>>): " 
//             << optional_shared_ptr_size << " bytes" << std::endl;
//   std::cout << "sizeof(InvasivePtr<Postings>): " 
//             << invasive_ptr_size << " bytes" << std::endl;
  
//   size_t memory_savings = optional_shared_ptr_size - invasive_ptr_size;
//   double percentage = (static_cast<double>(memory_savings) / optional_shared_ptr_size) * 100.0;
  
//   std::cout << "\nMemory savings per pointer: " << memory_savings 
//             << " bytes (" << percentage << "%)" << std::endl;
//   std::cout << "==========================================\n" << std::endl;

//   // Verify that InvasivePtr is more memory efficient
//   EXPECT_LT(invasive_ptr_size, optional_shared_ptr_size)
//       << "InvasivePtr<Postings> should be more memory efficient than optional<shared_ptr<Postings>>";

//   // On 64-bit systems, InvasivePtr should be exactly pointer-sized (8 bytes)
//   EXPECT_EQ(invasive_ptr_size, sizeof(void*))
//       << "InvasivePtr<Postings> should be pointer-sized";
// }

// TEST(InvasivePtrSizeofTest, HeapAllocationComparison) {
//   // This test measures actual heap-allocated memory including refcount overhead
//   std::cout << "\n=== Complete Heap Allocation Analysis ===" << std::endl;
  
//   // For InvasivePtr: RefCountWrapper = Postings + atomic<uint32_t> refcount
//   size_t postings_size = sizeof(Postings);
//   size_t refcount_size = sizeof(std::atomic<uint32_t>);
  
//   // Calculate the actual wrapper size with alignment
//   // The wrapper contains: T data + atomic<uint32_t> refcount
//   size_t invasive_heap_per_object = postings_size + refcount_size;
//   // Account for potential padding
//   size_t invasive_aligned = ((invasive_heap_per_object + 7) / 8) * 8;
  
//   std::cout << "\nInvasivePtr heap allocation per unique Postings:" << std::endl;
//   std::cout << "  Postings object: " << postings_size << " bytes" << std::endl;
//   std::cout << "  atomic<uint32_t> refcount: " << refcount_size << " bytes" << std::endl;
//   std::cout << "  Alignment padding: ~" << (invasive_aligned - invasive_heap_per_object) << " bytes" << std::endl;
//   std::cout << "  Total: ~" << invasive_aligned << " bytes" << std::endl;
  
//   std::cout << "\nstd::shared_ptr heap allocation per unique Postings:" << std::endl;
//   std::cout << "  Postings object: " << postings_size << " bytes" << std::endl;
//   std::cout << "  Control block: ~24 bytes (refcounts + vtable + deleter)" << std::endl;
//   size_t shared_ptr_heap = postings_size + 24;
//   std::cout << "  Total: ~" << shared_ptr_heap << " bytes" << std::endl;
  
//   std::cout << "\nPer-pointer storage (in containers/stack):" << std::endl;
//   std::cout << "  InvasivePtr<Postings>: " << sizeof(InvasivePtr<Postings>) << " bytes" << std::endl;
//   std::cout << "  optional<shared_ptr<Postings>>: " 
//             << sizeof(std::optional<std::shared_ptr<Postings>>) << " bytes" << std::endl;
  
//   size_t heap_savings = shared_ptr_heap - invasive_aligned;
//   size_t ptr_savings = sizeof(std::optional<std::shared_ptr<Postings>>) - 
//                        sizeof(InvasivePtr<Postings>);
  
//   std::cout << "\nSavings per unique object on heap: ~" << heap_savings << " bytes" << std::endl;
//   std::cout << "Savings per pointer: " << ptr_savings << " bytes" << std::endl;
  
//   std::cout << "\nExample: 10,000 unique Postings with 1 pointer each:" << std::endl;
//   size_t before_total = (sizeof(std::optional<std::shared_ptr<Postings>>) * 10000) + 
//                         (shared_ptr_heap * 10000);
//   size_t after_total = (sizeof(InvasivePtr<Postings>) * 10000) + 
//                        (invasive_aligned * 10000);
//   std::cout << "  Before: " << (before_total / 1024) << " KB" << std::endl;
//   std::cout << "  After:  " << (after_total / 1024) << " KB" << std::endl;
//   std::cout << "  Savings: " << ((before_total - after_total) / 1024) 
//             << " KB (" << (100.0 * (before_total - after_total) / before_total) 
//             << "%)" << std::endl;
//   std::cout << "============================================\n" << std::endl;
// }

// TEST(InvasivePtrSizeofTest, RealHeapAllocationWithMallocUsableSize) {
//   std::cout << "\n=== ACTUAL Heap Allocation (malloc_usable_size) ===" << std::endl;
  
//   // Measure InvasivePtr<Postings> allocation
//   auto invasive_ptr = InvasivePtr<Postings>::Make();
//   size_t invasive_memory = malloc_usable_size(&*invasive_ptr);
  
//   // Measure std::shared_ptr<Postings> allocation
//   // Note: shared_ptr makes TWO allocations (object + control block)
//   // We can only measure the object allocation directly
//   auto shared_ptr = std::make_shared<Postings>();
//   size_t shared_ptr_object_memory = malloc_usable_size(shared_ptr.get());
  
//   std::cout << "\n=== MEASURED Heap Allocations ===" << std::endl;
//   std::cout << "InvasivePtr<Postings> total allocation: " << invasive_memory << " bytes" << std::endl;
//   std::cout << "  (includes Postings + atomic refcount in single block)" << std::endl;
//   std::cout << "\nstd::shared_ptr<Postings> object allocation: " << shared_ptr_object_memory << " bytes" << std::endl;
//   std::cout << "  (object only - control block is separate allocation)" << std::endl;
  
//   std::cout << "\nBreakdown:" << std::endl;
//   std::cout << "  sizeof(Postings): " << sizeof(Postings) << " bytes" << std::endl;
//   std::cout << "  sizeof(atomic<uint32_t>): " << sizeof(std::atomic<uint32_t>) << " bytes" << std::endl;
//   std::cout << "\nInvasivePtr overhead in same allocation: " 
//             << (invasive_memory - sizeof(Postings)) << " bytes" << std::endl;
//   std::cout << "shared_ptr has SEPARATE control block allocation (~24 bytes)" << std::endl;
  
//   std::cout << "\nKey insight:" << std::endl;
//   std::cout << "  InvasivePtr: 1 allocation of " << invasive_memory << " bytes" << std::endl;
//   std::cout << "  shared_ptr: 2 allocations = " << shared_ptr_object_memory 
//             << " (object) + ~24 (control block)" << std::endl;
//   std::cout << "  = ~" << (shared_ptr_object_memory + 24) << " bytes total" << std::endl;
  
//   size_t estimated_shared_ptr_total = shared_ptr_object_memory + 24;
//   int64_t heap_savings = estimated_shared_ptr_total - invasive_memory;
//   double percentage = (static_cast<double>(heap_savings) / estimated_shared_ptr_total) * 100.0;
  
//   std::cout << "\nEstimated heap savings: ~" << heap_savings 
//             << " bytes (~" << percentage << "%)" << std::endl;
  
//   std::cout << "\nPointer storage savings:" << std::endl;
//   std::cout << "  InvasivePtr<Postings>: " << sizeof(InvasivePtr<Postings>) << " bytes" << std::endl;
//   std::cout << "  optional<shared_ptr<Postings>>: " 
//             << sizeof(std::optional<std::shared_ptr<Postings>>) << " bytes" << std::endl;
//   size_t ptr_savings = sizeof(std::optional<std::shared_ptr<Postings>>) - 
//                        sizeof(InvasivePtr<Postings>);
//   std::cout << "  Savings: " << ptr_savings << " bytes per pointer" << std::endl;
  
//   std::cout << "\nTotal savings per Postings:" << std::endl;
//   std::cout << "  Heap: ~" << heap_savings << " bytes" << std::endl;
//   std::cout << "  Pointer: " << ptr_savings << " bytes" << std::endl;
//   std::cout << "  Combined: ~" << (heap_savings + ptr_savings) << " bytes" << std::endl;
//   std::cout << "============================================\n" << std::endl;
  
//   // Verify InvasivePtr uses less memory
//   EXPECT_LT(invasive_memory, estimated_shared_ptr_total)
//       << "InvasivePtr should use less total heap memory than shared_ptr";
// }

// }  // namespace valkey_search::indexes::text
