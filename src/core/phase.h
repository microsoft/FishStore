// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <thread>

#include "alloc.h"
#include "async.h"
#include "constants.h"
#include "thread.h"
#include "utility.h"

namespace fishstore {
namespace core {

/// Phases, used internally by FishStore to keep track of how far along FishStore has gotten during
/// checkpoint, gc, and grow actions.
enum class Phase : uint8_t {
  /// Checkpoint phases.
  PREP_INDEX_CHKPT,
  INDEX_CHKPT,
  PREPARE,
  IN_PROGRESS,
  WAIT_PENDING,
  WAIT_FLUSH,
  REST,
  PERSISTENCE_CALLBACK,
  /// Garbage-collection phases.
  /// - The log's begin-address has been shifted; finish all outstanding I/Os before trying to
  ///   truncate the log.
  GC_IO_PENDING,
  /// - The log has been truncated, but threads are still cleaning the hash table.
  GC_IN_PROGRESS,
  /// Grow-index phases.
  /// - Each thread waits for all other threads to complete outstanding (synchronous) operations
  ///   against the hash table.
  GROW_PREPARE,
  /// - Each thread copies a chunk of the old hash table into the new hash table.
  GROW_IN_PROGRESS,
  /// ParserShift phases
  /// Waiting for all treads to refresh its epoch so as to rebuild its parser.
  PS_PENDING,
  INVALID
};

}
} // namespace fishstore::core
