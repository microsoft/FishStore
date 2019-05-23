// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>

namespace fishstore {
namespace core {

struct Constants {
  inline static void SetAvgRecordSize(uint64_t size) {
    avg_rec_size = size;
  }

  /// Size of cache line in bytes
  static constexpr uint32_t kCacheLineBytes = 64;

  /// We issue 256 writes to disk, to checkpoint the hash table.
  static constexpr uint32_t kNumMergeChunks = 256;

  // Default batch size for reserving space in batch insert.
  // Reserve this much batch size for the vector of insert contexts;
  static constexpr size_t kDefaultBatchSize = 1;

  /// Utilities for Adaptive prefecthing.
  // Total number of IO levels.
  static uint32_t kIoLevels;
  // Scan offset bit for each different IO level.
  static constexpr uint32_t address_offset_bit[5] = { 0, 12, 14, 16, 18 };
  // The gap threshold that we are happy to burn this much IO bandwidth to save an IO.
  static constexpr uint32_t gap_thres = 138240;
  // The average record size we rely on to calculate the gap between the end of
  // previous record to the current record. Note that the gap between two key pointers
  // in the hash chain also includes the size of payload for first record.
  static uint64_t avg_rec_size;
};

uint64_t Constants::avg_rec_size = 600;
uint32_t Constants::kIoLevels = 5;
constexpr uint32_t Constants::address_offset_bit[5];

}
} // namespace fishstore::cire
