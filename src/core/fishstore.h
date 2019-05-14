// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>
#include <functional>
#include <unordered_set>
#include <unordered_map>
#include <experimental/filesystem>
#include <fstream>

#include "device/file_system_disk.h"
#include "tsl/hopscotch_map.h"

#include "adaptors/common_utils.h"

#include "alloc.h"
#include "checkpoint_locks.h"
#include "checkpoint_state.h"
#include "constants.h"
#include "gc_state.h"
#include "grow_state.h"
#include "guid.h"
#include "hash_table.h"
#include "internal_contexts.h"
#include "key_hash.h"
#include "malloc_fixed_page_size.h"
#include "persistent_memory_malloc.h"
#include "record.h"
#include "recovery_status.h"
#include "state_transitions.h"
#include "status.h"
#include "utility.h"
#include "psf.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif

using namespace std::chrono_literals;

namespace fishstore {
namespace core {

#ifdef _TIMER
/// Debug information to keep track some global information
struct Stat {
  static std::atomic<size_t> overflow_buckets;
};
std::atomic<size_t> Stat::overflow_buckets { 0 };

static thread_local double tot_cpu_time = 0.0;
static thread_local double tot_fields_time = 0.0;
static thread_local double tot_preds_time = 0.0;
static thread_local double tot_prep_time = 0.0;
static thread_local double tot_insert_time = 0.0;
static thread_local double tot_memcpy_time = 0.0;
static auto checkpoint_start = std::chrono::high_resolution_clock::now();
static auto checkpoint_end = std::chrono::high_resolution_clock::now();
#endif

#ifdef _SCAN_BENCH
static std::vector<uint64_t> visited_address;
#endif

struct ParserState {
  ParserState() {
    ptr_general_psf.clear();
    ptr_inline_psf.clear();
    main_parser_fields.clear();
    main_parser_field_ids.clear();
  }

  // Registered PSFs
  std::unordered_set<uint16_t> ptr_general_psf;
  std::unordered_set<uint32_t> ptr_inline_psf;

  // Field name list to build the parser
  std::vector<std::string> main_parser_fields;
  // map from parser internal field ID to field ID defined in naming service;
  std::vector<uint16_t> main_parser_field_ids;
};

// Four different type of parser actions.
enum ParserActionType: uint8_t {
  REGISTER_INLINE_PSF,
  DEREGISTER_INLINE_PSF,
  REGISTER_GENERAL_PSF,
  DEREGISTER_GENERAL_PSF
};

struct ParserAction {
  ParserActionType type;
  uint32_t id;
};

// Utility struct for parser shifting.
// Contains a list of parser actions and a callback for passing back
// safe register boundary.
struct PSState {
  std::vector<ParserAction> actions;
  std::function<void(uint64_t)> callback;
};

// Thread-local parser context
template <class A>
struct ParserContext {
  ~ParserContext() {
    if (parser) delete parser;
  }

  void RebuildParser(int8_t parser_no_, const std::vector<std::string>& fields) {
    parser_no = parser_no_;
    if(parser) delete parser;
    parser = A::NewParser(fields);
  }

  int8_t parser_no = 0;
  typename A::parser_t* parser = nullptr;
};

struct LibraryHandle {
  std::experimental::filesystem::path path;
#ifdef _WIN32
  HMODULE handle;
#else
  void* handle;
#endif
};

template <class A>
class alignas(Constants::kCacheLineBytes) ThreadContext {
 public:
  ThreadContext()
    : contexts_{}
    , cur_{ 0 } {
  }

  inline const ExecutionContext& cur() const {
    return contexts_[cur_];
  }
  inline ExecutionContext& cur() {
    return contexts_[cur_];
  }

  inline const ExecutionContext& prev() const {
    return contexts_[(cur_ + 1) % 2];
  }
  inline ExecutionContext& prev() {
    return contexts_[(cur_ + 1) % 2];
  }

  inline void swap() {
    cur_ = (cur_ + 1) % 2;
  }

  inline const ParserContext<A>& parser_context() const {
    return parser_context_;
  }

  inline ParserContext<A>& parser_context() {
    return parser_context_;

  }

 private:
  ExecutionContext contexts_[2];
  ParserContext<A> parser_context_;
  uint8_t cur_;
};

template <class D, class A>
class FishStore {
 public:
  typedef FishStore<D, A> fishstore_t;

  typedef D disk_t;
  typedef typename D::file_t file_t;
  typedef typename D::log_file_t log_file_t;
  typedef A adaptor_t;

  typedef PersistentMemoryMalloc<disk_t> hlog_t;

  /// Contexts that have been deep-copied, for async continuations, and must be accessed via
  /// virtual function calls.
  typedef AsyncPendingReadContext async_pending_read_context_t;
  typedef AsyncPendingInsertContext async_pending_insert_context_t;
  typedef AsyncPendingScanContext async_pending_scan_context_t;
  typedef AsyncPendingFullScanContext async_pending_full_scan_context_t;

  FishStore(uint64_t table_size, uint64_t log_size, const std::string& filename,
            double log_mutable_fraction = 0.5, uint16_t field_cnt = 0)
    : min_table_size_{ table_size }
    , disk{ filename, epoch_ }
    , hlog{ log_size, epoch_, disk, disk.log(), log_mutable_fraction }
    , system_state_{ Action::None, Phase::REST, 1 }
    , num_pending_ios{ 0 }
    , system_parser_no_{ 0 } {
    if(!Utility::IsPowerOfTwo(table_size)) {
      throw std::invalid_argument{ " Size is not a power of 2" };
    }
    if(table_size > INT32_MAX) {
      throw std::invalid_argument{ " Cannot allocate such a large hash table " };
    }

    resize_info_.version = 0;
    state_[0].Initialize(table_size, disk.log().alignment());
    overflow_buckets_allocator_[0].Initialize(disk.log().alignment(), epoch_);

    // Initialize naming service and register `$x` names for CSV.
    field_lookup_map.clear();
    field_names.clear();
    inline_psf_map.clear();
    general_psf_map.clear();
    if(std::is_base_of<adaptor::JsonAdaptor, A>::value) {
      assert(field_cnt == 0);
    } else if(std::is_base_of<adaptor::CsvAdaptor, A>::value) {
      assert(field_cnt > 0);
      /// Initialize original names for CSV.
      std::string buf;
      for(uint16_t i = 0; i < field_cnt; ++i) {
        buf = "$" + std::to_string(i);
        field_names.push_back(buf);
        field_lookup_map.emplace(std::make_pair(buf, i));
      }
    }
  }

  // No copy constructor.
  FishStore(const FishStore& other) = delete;

 public:
  /// Thread-related operations
  Guid StartSession();
  std::pair<uint64_t, uint32_t> ContinueSession(const Guid& guid);
  void StopSession();
  void Refresh();

  /// Store interface

  // Batch Insert load a batch of records from a string witha monotomic serial number, returns
  // the number of records inserted successfully.
  inline uint32_t BatchInsert(const std::string& record_batch, uint64_t monotomic_serial_num,
                              uint32_t offset = 0);

  inline uint32_t BatchInsert(const char* payload, size_t length, uint64_t monotomic_serial_num,
    uint32_t offset = 0);

  template <class RC>
  inline Status Read(RC& context, AsyncCallback callback, uint64_t monotonic_serial_num);

  template <class IC>
  inline Status Insert(IC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
                       uint32_t internal_offset = 0);

  template <class SC>
  inline Status Scan(SC& context, AsyncCallback callback, uint64_t monotonic_serial_num,
                     uint64_t start_addr = 0, uint64_t end_addr = Address::kMaxAddress);

  template <class SC>
  inline Status FullScan(SC& context, AsyncCallback callbakc, uint64_t monotonic_serial_num,
                         uint64_t start_addr = 0, uint64_t end_addr = Address::kMaxAddress);

  inline bool CompletePending(bool wait = false);

  inline bool CompleteAction(bool wait = false);

  // Naming Service
  uint64_t LoadPSFLibrary(const std::string& lib_path);

  uint16_t MakeProjection(const std::string& field_name);

  uint16_t MakeGeneralPSF(const std::vector<std::string>& field_names,
                          size_t lib_id, std::string func_name);

  uint32_t MakeInlinePSF(const std::vector<std::string>& field_names,
                         size_t lib_id, std::string func_name);

  void PrintRegistration();

#ifdef _WIN32
  HMODULE GetLibHandle(size_t lib_id) {
#else
  void* GetLibHandle(size_t lib_id) {
#endif
    return libs.at(lib_id).handle;
  }

  // Helper function for CSV header, i.e., field_id -> string aliasing
  void RegisterHeader(const std::vector<std::string>& header);


  /// Checkpoint/recovery operations.
  bool Checkpoint(const std::function<void(Status)>& index_persistent_callback,
                  const std::function<void(Status, uint64_t, uint32_t)>& hybrid_log_persistence_callback,
                  Guid& token);
  bool CheckpointIndex(const std::function<void(Status)>& index_persistent_callback, Guid& token);
  bool CheckpointHybridLog(const std::function<void(Status, uint64_t, uint32_t)>&
                           hybrid_log_persistence_callback,
                           Guid& token);
  Status Recover(const Guid& index_token, const Guid& hybrid_log_token, uint32_t& version,
                 std::vector<Guid>& session_ids);

  /// Truncating the head of the log.
  bool ShiftBeginAddress(Address address, GcState::truncate_callback_t truncate_callback,
                         GcState::complete_callback_t complete_callback);

  /// Make the hash table larger.
  bool GrowIndex(GrowState::callback_t caller_callback);

  // Applying a list of actions to the parser.
  // Returns safe deregister boundary through returned value
  // Returns safe register boudnary through callback
  uint64_t ApplyParserShift(const std::vector<ParserAction>& actions,
                            const std::function<void(uint64_t)>& callback);

  /// Statistics
  inline uint64_t Size() const {
    return hlog.GetTailAddress().control();
  }
  inline void DumpDistribution() {
    state_[resize_info_.version].DumpDistribution(
      overflow_buckets_allocator_[resize_info_.version]);
  }

 private:
  typedef Record record_t;

  typedef PendingContext pending_context_t;

  uint16_t AcquireFieldID(const std::string& field_name);

  template <class C>
  inline OperationStatus InternalRead(C& pending_context) const;

  template <class C>
  inline OperationStatus InternalInsert(C& pending_context);

  template <class C>
  inline OperationStatus InternalScan(C& pending_context, uint64_t start_addr,
                                      uint64_t end_addr) const;

  template <class C>
  inline OperationStatus InternalFullScan(C& pending_context, uint64_t start_addr, uint64_t end_addr);

  OperationStatus InternalContinuePendingRead(ExecutionContext& ctx,
      AsyncIOContext& io_context);
  OperationStatus InternalContinuePendingScan(ExecutionContext& ctx,
      AsyncIOContext& io_context);

  // Find the hash bucket entry, if any, corresponding to the specified hash.
  inline const AtomicHashBucketEntry* FindEntry(KeyHash hash) const;
  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // create a new entry. The caller can use the "expected_entry" to CAS its desired address into
  // the entry.
  inline AtomicHashBucketEntry* FindOrCreateEntry(KeyHash hash, HashBucketEntry& expected_entry,
      HashBucket*& bucket);

  template <class C>
  inline Address TraceBackForMatch(C& pending_context, Address from_address,
                                   Address min_offset) const;

  Address TraceBackForOtherChainStart(uint64_t old_size,  uint64_t new_size, Address from_address,
                                      Address min_address, uint8_t side);

  // If a hash bucket entry corresponding to the specified hash exists, return it; otherwise,
  // return an unused bucket entry.
  inline AtomicHashBucketEntry* FindTentativeEntry(KeyHash hash, HashBucket* bucket,
      uint8_t version, HashBucketEntry& expected_entry);
  // Looks for an entry that has the same
  inline bool HasConflictingEntry(KeyHash hash, const HashBucket* bucket, uint8_t version,
                                  const AtomicHashBucketEntry* atomic_entry) const;

  inline Address BlockAllocate(uint32_t record_size);

  inline Status HandleOperationStatus(ExecutionContext& ctx,
                                      pending_context_t& pending_context,
                                      OperationStatus internal_status, bool& async);
  inline Status PivotAndRetry(ExecutionContext& ctx, pending_context_t& pending_context,
                              bool& async);
  inline Status RetryLater(ExecutionContext& ctx, pending_context_t& pending_context,
                           bool& async);
  inline constexpr uint32_t MinIoRequestSize() const;

  inline Status IssueAsyncIoRequest(ExecutionContext& ctx,
                                    pending_context_t& pending_context,
                                    bool& async);
  inline Status IssueAsyncScanRequest(ExecutionContext& ctx,
                                      pending_context_t& pending_context,
                                      bool& async);

  void AsyncGetFromDisk(Address address, uint32_t num_records, AsyncIOCallback callback,
                        AsyncIOContext& context);
  static void AsyncGetFromDiskCallback(IAsyncContext* ctxt, Status result,
                                       size_t bytes_transferred);
  static void AsyncScanFromDiskCallback(IAsyncContext* ctxt, Status result,
                                        size_t bytes_transferred);
  static void AsyncFullScanFromDiskCallback(IAsyncContext* ctxt, Status result,
      size_t bytes_transferred);

  void CompleteIoPendingRequests(ExecutionContext& context);
  void CompleteRetryRequests(ExecutionContext& context);

  void InitializeCheckpointLocks();

  /// Checkpoint/recovery methods.
  void HandleSpecialPhases();
  bool GlobalMoveToNextState(SystemState current_state);

  Status CheckpointFuzzyIndex();
  Status CheckpointFuzzyIndexComplete();
  Status RecoverFuzzyIndex();
  Status RecoverFuzzyIndexComplete(bool wait);

  Status WriteIndexMetadata();
  Status ReadIndexMetadata(const Guid& token);
  Status WriteCprMetadata();
  Status ReadCprMetadata(const Guid& token);
  Status WriteCprContext();
  Status ReadCprContexts(const Guid& token, const Guid* guids);

  Status RecoverHybridLog();
  Status RecoverHybridLogFromSnapshotFile();
  Status RecoverFromPage(Address from_address, Address to_address);
  Status RestoreHybridLog();

  void MarkAllPendingRequests();

  inline void HeavyEnter();
  bool CleanHashTableBuckets();
  void SplitHashTableBuckets();
  void AddHashEntry(HashBucket*& bucket, uint32_t& next_idx, uint8_t version,
                    HashBucketEntry entry);

  void ParserStateApplyActions(ParserState& state, const std::vector<ParserAction>& actions);

  /// Access the current and previous (thread-local) execution contexts.
  const ExecutionContext& thread_ctx() const {
    return thread_contexts_[Thread::id()].cur();
  }
  ExecutionContext& thread_ctx() {
    return thread_contexts_[Thread::id()].cur();
  }
  ExecutionContext& prev_thread_ctx() {
    return thread_contexts_[Thread::id()].prev();
  }
  ParserContext<A>& parser_ctx() {
    return thread_contexts_[Thread::id()].parser_context();
  }

 private:
  LightEpoch epoch_;

 public:
  disk_t disk;
  hlog_t hlog;

 private:
  static constexpr bool kCopyReadsToTail = false;
  static constexpr uint64_t kGcHashTableChunkSize = 16384;
  static constexpr uint64_t kGrowHashTableChunkSize = 16384;

  bool fold_over_snapshot = true;

  /// Initial size of the table
  uint64_t min_table_size_;

  // Allocator for the hash buckets that don't fit in the hash table.
  MallocFixedPageSize<HashBucket, disk_t> overflow_buckets_allocator_[2];

  // An array of size two, that contains the old and new versions of the hash-table
  InternalHashTable<disk_t> state_[2];

  CheckpointLocks checkpoint_locks_;

  ResizeInfo resize_info_;

  AtomicSystemState system_state_;

  /// Checkpoint/recovery state.
  CheckpointState<file_t> checkpoint_;
  /// Garbage collection state.
  GcState gc_;
  /// Grow (hash table) state.
  GrowState grow_;
  PSState ps_;

  /// Global count of pending I/Os, used for throttling.
  std::atomic<uint64_t> num_pending_ios;

  /// Space for two contexts per thread, stored inline.
  ThreadContext<A> thread_contexts_[Thread::kMaxNumThreads];

  // FishStore Meta-data.
  // Mutex is used to protect the field_lookup_map during field and
  // predicate registration, which is not on the execution hot path.
  std::mutex mutex;
  // Field and predicate global registry.
  std::unordered_map<std::string, uint16_t> field_lookup_map;
  std::vector<LibraryHandle> libs;
  concurrent_vector<std::string> field_names;

  concurrent_vector<InlinePSF<A>> inline_psf_map;
  concurrent_vector<GeneralPSF<A>> general_psf_map;

  ParserState parser_states[2];
  std::atomic_int8_t system_parser_no_;
};

// Implementations.
template <class D, class A>
inline Guid FishStore<D, A>::StartSession() {
  SystemState state = system_state_.load();
  if(state.phase != Phase::REST) {
    throw std::runtime_error{ "Can acquire only in REST phase!" };
  }
  int8_t parser_no = system_parser_no_;
  thread_ctx().Initialize(state.phase, state.version, Guid::Create(), 0, 0);
  parser_ctx().RebuildParser(parser_no, parser_states[parser_no].main_parser_fields);
  Refresh();
  return thread_ctx().guid;
}

template <class D, class A>
inline std::pair<uint64_t, uint32_t> FishStore<D, A>::ContinueSession(const Guid& session_id) {
  auto iter = checkpoint_.continue_tokens.find(session_id);
  if(iter == checkpoint_.continue_tokens.end()) {
    throw std::invalid_argument{ "Unknown session ID" };
  }

  SystemState state = system_state_.load();
  if(state.phase != Phase::REST) {
    throw std::runtime_error{ "Can continue only in REST phase!" };
  }
  int8_t parser_no = system_parser_no_;
  //XX: This need to be fixed;
  thread_ctx().Initialize(state.phase, state.version, session_id, iter->second.first,
                          iter->second.second);
  parser_ctx().RebuildParser(parser_no, parser_states[parser_no].main_parser_fields);
  Refresh();
  return iter->second;
}

template <class D, class A>
inline void FishStore<D, A>::Refresh() {
  epoch_.ProtectAndDrain();
  // We check if we are in normal mode
  SystemState new_state = system_state_.load();
  if(thread_ctx().phase == Phase::REST && new_state.phase == Phase::REST) {
    return;
  }
  HandleSpecialPhases();
}

template <class D, class A>
inline void FishStore<D, A>::StopSession() {
  // If this thread is still involved in some activity, wait until it finishes.
  while(thread_ctx().phase != Phase::REST ||
        !thread_ctx().pending_ios.empty() ||
        !thread_ctx().retry_requests.empty()) {
    CompletePending(false);
    std::this_thread::yield();
  }

  assert(thread_ctx().retry_requests.empty());
  assert(thread_ctx().pending_ios.empty());
  assert(thread_ctx().io_responses.empty());

  assert(prev_thread_ctx().retry_requests.empty());
  assert(prev_thread_ctx().pending_ios.empty());
  assert(prev_thread_ctx().io_responses.empty());

  assert(thread_ctx().phase == Phase::REST);

  epoch_.Unprotect();
}

template <class D, class A>
inline const AtomicHashBucketEntry* FishStore<D, A>::FindEntry(KeyHash hash) const {
  // Truncate the hash to get a bucket page_index < state[version].size.
  uint32_t version = resize_info_.version;
  const HashBucket* bucket = &state_[version].bucket(hash);
  assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry.unused()) {
        continue;
      }
      if(hash.tag() == entry.tag()) {
        // Found a matching tag. (So, the input hash matches the entry on 14 tag bits +
        // log_2(table size) address bits.)
        if(!entry.tentative()) {
          // If (final key, return immediately)
          return &bucket->entries[entry_idx];
        }
      }
    }

    // Go to next bucket in the chain
    HashBucketOverflowEntry entry = bucket->overflow_entry.load();
    if(entry.unused()) {
      // No more buckets in the chain.
      return nullptr;
    }
    bucket = &overflow_buckets_allocator_[version].Get(entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class D, class A>
inline AtomicHashBucketEntry* FishStore<D, A>::FindTentativeEntry(KeyHash hash,
    HashBucket* bucket,
    uint8_t version, HashBucketEntry& expected_entry) {
  expected_entry = HashBucketEntry::kInvalidEntry;
  AtomicHashBucketEntry* atomic_entry = nullptr;
  // Try to find a slot that contains the right tag or that's free.
  while(true) {
    // Search through the bucket looking for our key. Last entry is reserved
    // for the overflow pointer.
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry.unused()) {
        if(!atomic_entry) {
          // Found a free slot; keep track of it, and continue looking for a match.
          atomic_entry = &bucket->entries[entry_idx];
        }
        continue;
      }
      if(hash.tag() == entry.tag() && !entry.tentative()) {
        // Found a match. (So, the input hash matches the entry on 14 tag bits +
        // log_2(table size) address bits.) Return it to caller.
        expected_entry = entry;
        return &bucket->entries[entry_idx];
      }
    }
    // Go to next bucket in the chain
    HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
    if(overflow_entry.unused()) {
      // No more buckets in the chain.
      if(atomic_entry) {
        // We found a free slot earlier (possibly inside an earlier bucket).
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return atomic_entry;
      }

#ifdef __TIMER
      ++Stat::overflow_buckets;
#endif

      // We didn't find any free slots, so allocate new bucket.
      FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
      bool success;
      do {
        HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
        success = bucket->overflow_entry.compare_exchange_strong(overflow_entry,
                  new_bucket_entry);
      } while(!success && overflow_entry.unused());
      if(!success) {
        // Install failed, undo allocation; use the winner's entry
        overflow_buckets_allocator_[version].FreeAtEpoch(new_bucket_addr, 0);
      } else {
        // Install succeeded; we have a new bucket on the chain. Return its first slot.
        bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
        assert(expected_entry == HashBucketEntry::kInvalidEntry);
        return &bucket->entries[0];
      }
    }
    // Go to the next bucket.
    bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class D, class A>
bool FishStore<D, A>::HasConflictingEntry(KeyHash hash, const HashBucket* bucket,
    uint8_t version,
    const AtomicHashBucketEntry* atomic_entry) const {
  uint16_t tag = atomic_entry->load().tag();
  while(true) {
    for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
      HashBucketEntry entry = bucket->entries[entry_idx].load();
      if(entry != HashBucketEntry::kInvalidEntry &&
          entry.tag() == tag &&
          atomic_entry != &bucket->entries[entry_idx]) {
        // Found a conflict.
        return true;
      }
    }
    // Go to next bucket in the chain
    HashBucketOverflowEntry entry = bucket->overflow_entry.load();
    if(entry.unused()) {
      // Reached the end of the bucket chain; no conflicts found.
      return false;
    }
    // Go to the next bucket.
    bucket = &overflow_buckets_allocator_[version].Get(entry.address());
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
  }
}

template <class D, class A>
inline AtomicHashBucketEntry* FishStore<D, A>::FindOrCreateEntry(KeyHash hash,
    HashBucketEntry& expected_entry, HashBucket*& bucket) {
  bucket = nullptr;
  // Truncate the hash to get a bucket page_index < state[version].size.
  uint32_t version = resize_info_.version;
  assert(version <= 1);

  while(true) {
    bucket = &state_[version].bucket(hash);
    assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);

    AtomicHashBucketEntry* atomic_entry = FindTentativeEntry(hash, bucket, version,
                                          expected_entry);
    if(expected_entry != HashBucketEntry::kInvalidEntry) {
      // Found an existing hash bucket entry; nothing further to check.
      return atomic_entry;
    }
    // We have a free slot.
    assert(atomic_entry);
    assert(expected_entry == HashBucketEntry::kInvalidEntry);
    // Try to install tentative tag in free slot.
    HashBucketEntry entry{ Address::kInvalidAddress, hash.tag(), true };
    if(atomic_entry->compare_exchange_strong(expected_entry, entry)) {
      // See if some other thread is also trying to install this tag.
      if(HasConflictingEntry(hash, bucket, version, atomic_entry)) {
        // Back off and try again.
        atomic_entry->store(HashBucketEntry::kInvalidEntry);
      } else {
        // No other thread was trying to install this tag, so we can clear our entry's "tentative"
        // bit.
        expected_entry = HashBucketEntry{ Address::kInvalidAddress, hash.tag(), false };
        atomic_entry->store(expected_entry);
        return atomic_entry;
      }
    }
  }
  assert(false);
  return nullptr; // NOT REACHED
}

template <class D, class A>
template <class RC>
inline Status FishStore<D, A>::Read(RC& context, AsyncCallback callback,
                                    uint64_t monotonic_serial_num) {
  typedef RC read_context_t;
  typedef PendingReadContext<RC> pending_read_context_t;

  pending_read_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalRead(pending_context);
  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else if(internal_status == OperationStatus::NOT_FOUND) {
    status = Status::NotFound;
  } else {
    assert(internal_status == OperationStatus::RECORD_ON_DISK);
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  thread_ctx().offset = 0;
  return status;
}

template <class D, class A>
inline uint32_t FishStore<D, A>::BatchInsert(const std::string& record_batch,
  uint64_t monotonic_serial_num, uint32_t internal_offset) {

  return BatchInsert(record_batch.c_str(), record_batch.length(), monotonic_serial_num, internal_offset);

}

template <class D, class A>
inline uint32_t FishStore<D, A>::BatchInsert(const char* data, size_t length,
    uint64_t monotonic_serial_num, uint32_t internal_offset) {

#ifdef _TIMER
  auto t_start = std::chrono::high_resolution_clock::now();
#endif

  if (length == internal_offset) return 0;

  // Fetch the thread-local parser and parser record batch.
  const ParserState& parser_state = parser_states[parser_ctx().parser_no];
  auto records = A::Parse(parser_ctx().parser, data, length, internal_offset);

  // make reservation for insert_contexts, kpts, field map, PSF arguments
  // to prevent re-allocation.
  std::vector<RecordInsertContext> insert_contexts;
  insert_contexts.reserve(Constants::kDefaultBatchSize);
  size_t max_kpt_size =
    parser_state.ptr_general_psf.size() + parser_state.ptr_inline_psf.size();
  size_t field_size = parser_state.main_parser_fields.size();
  // `field_map` maps the field ID registered in naming service to parsed out
  // value.
  tsl::hopscotch_map<uint16_t, typename A::field_t> field_map(field_size);
  std::vector<typename A::field_t> psf_args;
  psf_args.reserve(field_size);

#ifdef _TIMER
  auto t_parse_start = std::chrono::high_resolution_clock::now();
#endif

  uint32_t current_offset = internal_offset;
  // Iterate through records and fields so as to construct insert context for
  // each record. Insert context contains the payload and the corresponding info
  // to build all key pointers, stored in kpts.
  for(auto& record : records) {
    // Get the full record payload.
    auto rec_ref = record.GetAsRawTextRef();
    current_offset += static_cast<uint32_t>(rec_ref.Length());
    insert_contexts.emplace_back(RecordInsertContext{
      rec_ref.Data(), static_cast<uint32_t>(rec_ref.Length()), current_offset});
    RecordInsertContext& insert_context = insert_contexts.back();
    // Iterate through all parsed out fields to populate the field map for this
    // record.
    field_map.clear();
    for(auto& field : record) {
      field_map.emplace(parser_state.main_parser_field_ids[field.FieldId()], field);
    }

    // Construct key pointer utilities. For KPTUtil definition, pls refer to
    // `internal_context.h`
    std::vector<KPTUtil>& kpts = insert_context.kpts();
    kpts.reserve(max_kpt_size);
    std::vector<NullableStringRef>& options = insert_context.options();

#ifdef _TIMER
    auto t_fields_start = std::chrono::high_resolution_clock::now();
#endif

    uint32_t optional_offset = static_cast<uint32_t>(rec_ref.Length());
    uint16_t n_general_pts = 0;
    for(auto general_psf_id: parser_state.ptr_general_psf) {
      psf_args.clear();
      auto& psf = general_psf_map[general_psf_id];
      uint32_t value_offset, value_size;
      uint64_t hash;
      bool missing_fields = false;
      for (auto& field_id : psf.fields) {
        auto it = field_map.find(field_id);
        if (it != field_map.end())
          psf_args.emplace_back(it->second);
        else {
          missing_fields = true;
          break;
        }
      }

      if (missing_fields) continue;
      // If all the fields are found and predicate evaluates true, we build a
      // key pointer for this predicate on the record.
      NullableStringRef res;
      //Get the benefit of inlining....
      if (psf.lib_id == -1) res = projection<A>(psf_args);
      else res = psf.eval_(psf_args);
      if (res.isNull) continue;

      hash = Utility::HashBytesWithPSFID(general_psf_id, res.payload, res.size);
      value_size = res.size;
      if (rec_ref.Data() <= res.payload && res.payload <= rec_ref.Data() + rec_ref.Length()) {
        value_offset = static_cast<uint32_t>(res.payload - rec_ref.Data());
      } else {
        value_offset = optional_offset;
        optional_offset += res.size;
        options.emplace_back(res);
      }
      kpts.emplace_back(KPTUtil{ hash, general_psf_id, value_offset, value_size });
      ++n_general_pts;
    }
    insert_context.set_n_general_pts(n_general_pts);
    insert_context.set_optional_size(optional_offset - static_cast<uint32_t>(rec_ref.Length()));

#ifdef _TIMER
    auto t_fields_end = std::chrono::high_resolution_clock::now();
    tot_fields_time += std::chrono::duration<double>(t_fields_end - t_fields_start).count();
    auto t_preds_start = std::chrono::high_resolution_clock::now();
#endif

    for(auto inline_psf_id: parser_state.ptr_inline_psf) {
      psf_args.clear();
      auto& psf = inline_psf_map[inline_psf_id];
      bool flag = false;
      for(auto& field_id : psf.fields) {
        auto it = field_map.find(field_id);
        if(it != field_map.end())
          psf_args.emplace_back(it->second);
        else {
          flag = true;
          break;
        }
      }

      if(!flag) {
        NullableInt res = psf.eval_(psf_args);
        if(!res.isNull) {
          kpts.emplace_back(KPTUtil{inline_psf_id, res.value});
        }
      }
    }

#ifdef _TIMER
    auto t_preds_end = std::chrono::high_resolution_clock::now();
    tot_preds_time += std::chrono::duration<double>(t_preds_end - t_preds_start).count();
#endif
  }

#ifdef _TIMER
  auto t_parse_end = std::chrono::high_resolution_clock::now();
  tot_prep_time += std::chrono::duration<double>(t_parse_end - t_parse_start).count();
#endif

  // Finish preparing insert contexts, start populating the store.
  uint32_t op_cnt = 0;
  for(auto& insert_context : insert_contexts) {
    // Refresh periodically.
    if(op_cnt % Constants::kRefreshRate == 0) {
      Refresh();
    }
    auto callback = [](IAsyncContext* ctxt, Status result) {
      CallbackContext<RecordInsertContext> context{ctxt};
      assert(result == Status::Ok);
    };

#ifdef _TIMER
    auto t_insert_start = std::chrono::high_resolution_clock::now();
#endif

    auto status = Insert(insert_context, callback, monotonic_serial_num, insert_context.offset());

#ifdef _TIMER
    auto t_insert_end = std::chrono::high_resolution_clock::now();
    tot_insert_time += std::chrono::duration<double>(t_insert_end - t_insert_start).count();
#endif

    assert(status == Status::Ok || status == Status::Pending);
    ++op_cnt;
  }
  CompletePending(true);

#ifdef _TIMER
  auto t_end = std::chrono::high_resolution_clock::now();
  tot_cpu_time += std::chrono::duration<double>(t_end - t_start).count();
#endif

  return op_cnt;

}

template <class D, class A>
template <class IC>
inline Status FishStore<D, A>::Insert(IC& context, AsyncCallback callback,
                                      uint64_t monotonic_serial_num, uint32_t internal_offset) {
  typedef IC insert_context_t;
  typedef PendingInsertContext<IC> pending_insert_context_t;

  pending_insert_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalInsert(pending_context);
  Status status;

  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else {
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  thread_ctx().offset = internal_offset;
  return status;
}

template <class D, class A>
template <class SC>
inline Status FishStore<D, A>::Scan(SC& context, AsyncCallback callback,
                                    uint64_t monotonic_serial_num,
                                    uint64_t start_addr, uint64_t end_addr) {
  typedef SC scan_context_t;
  typedef PendingScanContext<SC> pending_scan_context_t;

  pending_scan_context_t pending_context{context, callback};
  OperationStatus internal_status = InternalScan(pending_context, start_addr, end_addr);
  Status status;

  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else {
    assert(internal_status == OperationStatus::SCAN_IN_PROGRESS);
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  thread_ctx().offset = 0;
  return status;
}

template <class D, class A>
template <class SC>
inline Status FishStore<D, A>::FullScan(SC& context, AsyncCallback callback,
                                        uint64_t monotonic_serial_num,
                                        uint64_t start_addr, uint64_t end_addr) {
  typedef SC full_scan_context_t;
  typedef PendingFullScanContext<SC> pending_full_scan_context_t;

  pending_full_scan_context_t pending_context{ context, callback };
  OperationStatus internal_status = InternalFullScan(pending_context, start_addr, end_addr);
  Status status;
  if(internal_status == OperationStatus::SUCCESS) {
    status = Status::Ok;
  } else {
    assert(internal_status == OperationStatus::SCAN_IN_PROGRESS);
    bool async;
    status = HandleOperationStatus(thread_ctx(), pending_context, internal_status, async);
  }
  thread_ctx().serial_num = monotonic_serial_num;
  thread_ctx().offset = 0;
  return status;
}

template <class D, class A>
inline bool FishStore<D, A>::CompletePending(bool wait) {
  do {
    disk.TryComplete();

    bool done = true;
    if(thread_ctx().phase != Phase::WAIT_PENDING && thread_ctx().phase != Phase::IN_PROGRESS) {
      CompleteIoPendingRequests(thread_ctx());
    }
    Refresh();
    CompleteRetryRequests(thread_ctx());

    //done = (thread_ctx().pending_ios.empty() && thread_ctx().retry_requests.empty());
    done = (thread_ctx().pending_io_cnt == 0 && thread_ctx().retry_requests.empty());

    if(thread_ctx().phase != Phase::REST) {
      CompleteIoPendingRequests(prev_thread_ctx());
      Refresh();
      CompleteRetryRequests(prev_thread_ctx());
      done = false;
    }
    if(done) {
      return true;
    }
  } while(wait);
  return false;
}

template <class D, class A>
inline bool FishStore<D, A>::CompleteAction(bool wait) {
  do {
    CompletePending();
    auto state = system_state_.load();
    if(state.phase == Phase::REST && state.action == Action::None) {
      CompletePending();
      return true;
    }
  } while(wait);
  return false;
}

template <class D, class A>
inline void FishStore<D, A>::CompleteIoPendingRequests(ExecutionContext& context) {
  AsyncIOContext* ctxt;
  // Clear this thread's I/O response queue. (Does not clear I/Os issued by this thread that have
  // not yet completed.)
  while(context.io_responses.try_pop(ctxt)) {
    CallbackContext<AsyncIOContext> io_context{ ctxt };
    CallbackContext<pending_context_t> pending_context{ io_context->caller_context };
    // This I/O is no longer pending, since we popped its response off the queue.
    auto pending_io = context.pending_ios.find(io_context->io_id);
    assert(pending_io != context.pending_ios.end() ||
           pending_context.get()->type == OperationType::FullScan);
    if(pending_io != context.pending_ios.end())
      context.pending_ios.erase(pending_io);
    --context.pending_io_cnt;

    // Issue the continue command
    OperationStatus internal_status;
    if(pending_context->type == OperationType::Read) {
      internal_status = InternalContinuePendingRead(context, *io_context.get());
    } else {
      assert(pending_context->type == OperationType::Scan ||
             pending_context->type == OperationType::FullScan);
      internal_status = InternalContinuePendingScan(context, *io_context.get());
    }
    Status result;
    if(internal_status == OperationStatus::SUCCESS) {
      result = Status::Ok;
    } else if(internal_status == OperationStatus::NOT_FOUND) {
      result = Status::NotFound;
    } else {
      result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                     pending_context.async);
    }
    if(!pending_context.async) {
      pending_context->caller_callback(pending_context->caller_context, result);
    }
  }
}

template <class D, class A>
inline void FishStore<D, A>::CompleteRetryRequests(ExecutionContext& context) {
  // If we can't complete a request, it will be pushed back onto the deque. Retry each request
  // only once.
  size_t size = context.retry_requests.size();
  for(size_t idx = 0; idx < size; ++idx) {
    CallbackContext<pending_context_t> pending_context{ context.retry_requests.front() };
    context.retry_requests.pop_front();
    // Issue retry command
    OperationStatus internal_status;
    switch(pending_context->type) {
    case OperationType::Insert:
      internal_status = InternalInsert(
                          *static_cast<async_pending_insert_context_t*>(pending_context.get()));
      break;
    default:
      assert(false);
      throw std::runtime_error{ "Cannot happen!" };
    }
    // Handle operation status
    Status result;
    if(internal_status == OperationStatus::SUCCESS) {
      result = Status::Ok;
    } else {
      result = HandleOperationStatus(context, *pending_context.get(), internal_status,
                                     pending_context.async);
    }

    // If done, callback user code.
    if(!pending_context.async) {
      pending_context->caller_callback(pending_context->caller_context, result);
    }
  }
}

template <class D, class A>
template <class C>
inline OperationStatus FishStore<D, A>::InternalRead(C& pending_context) const {
  typedef C pending_read_context_t;

  KeyHash hash = pending_context.get_hash();

  if(thread_ctx().phase != Phase::REST) {
    const_cast<fishstore_t*>(this)->HeavyEnter();
  }

  const AtomicHashBucketEntry* atomic_entry = FindEntry(hash);
  if(!atomic_entry) {
    // no record found
    return OperationStatus::NOT_FOUND;
  }

  HashBucketEntry entry = atomic_entry->load();
  Address address = entry.address();
  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();
  Address safe_read_only_address = hlog.safe_read_only_address.load();
  Address read_only_address = hlog.read_only_address.load();
  uint64_t latest_record_version = 0;

  if(address >= head_address) {
    // Look through the in-memory portion of the log, to find the first record (if any) whose key
    // matches.
    const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(address));
    const record_t* record = kpt->get_record();
    latest_record_version = record->header.checkpoint_version;
    if(record->header.invalid || !pending_context.check(kpt)) {
      address = TraceBackForMatch(pending_context, kpt->prev_address, head_address);
    }
  }

  switch(thread_ctx().phase) {
  case Phase::PREPARE:
    // Reading old version (v).
    if(latest_record_version > thread_ctx().version) {
      // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
      // what we've seen.
      pending_context.go_async(thread_ctx().phase, thread_ctx().version, address);
      return OperationStatus::CPR_SHIFT_DETECTED;
    }
    break;
  default:
    break;
  }

  if(address >= head_address) {
    const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(address));
    const record_t* record = kpt->get_record();
    pending_context.Get(record);
    return OperationStatus::SUCCESS;
  } else if(address >= begin_address) {
#ifdef _NULL_DISK
    return OperationStatus::NOT_FOUND;
#else
    // Record not available in-memory
    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address);
    return OperationStatus::RECORD_ON_DISK;
#endif
  } else {
    // No record found
    return OperationStatus::NOT_FOUND;
  }
}

struct HashBucketCASHelper {
  AtomicHashBucketEntry* atomic_entry = nullptr;
  HashBucketEntry expected_entry;
};

template <class D, class A>
template <class C>
inline OperationStatus FishStore<D, A>::InternalInsert(C& pending_context) {
  typedef C pending_insert_context_t;

  if(thread_ctx().phase != Phase::REST) {
    HeavyEnter();
  }

  const std::vector<KPTUtil>& kpts = pending_context.kpts();
  uint16_t n_general_pts = pending_context.n_general_pts();

  // (Note that address will be Address::kInvalidAddress, if the atomic_entry was created.)
  Address head_address = hlog.head_address.load();
  uint64_t latest_record_version = 0;
  std::vector<HashBucketCASHelper> cas_helpers(kpts.size());
  for(uint16_t i = 0; i < kpts.size(); ++i) {
    HashBucket* bucket;
    cas_helpers[i].atomic_entry = FindOrCreateEntry(kpts[i].hash, cas_helpers[i].expected_entry,
                                  bucket);
    Address address = cas_helpers[i].expected_entry.address();
    if(address >= head_address) {
      record_t* record =
        reinterpret_cast<KeyPointer*>(hlog.Get(address))->get_record();
      latest_record_version =
        std::max(latest_record_version, record->header.checkpoint_version);
    }
  }

  // Threre will be no actual checkpoint locking happening since the only writer to a record is
  // the its ingestion thread and no other readers is able to read it until the record is successfully
  // inserted. The only thing we need to take care of is to pivot and retry any insertions when a newer
  // record among all the hash chains appears.
  if(thread_ctx().phase == Phase::PREPARE && latest_record_version > thread_ctx().version) {
    // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
    // what we've seen.
    pending_context.go_async(thread_ctx().phase, thread_ctx().version, Address::kInvalidAddress);
    return OperationStatus::CPR_SHIFT_DETECTED;
  }

  // Calculate size of the record and allocate its slot in the log. Note that
  // invalid bit is set to true at the very beginning. Thus, the record is not
  // yet visible.
  uint32_t record_size = record_t::size(static_cast<uint32_t>(kpts.size()),
                                        pending_context.payload_size(), pending_context.optional_size());
  Address new_address = BlockAllocate(record_size);
  record_t* record = reinterpret_cast<record_t*>(hlog.Get(new_address));
  new(record) record_t{RecordInfo{
      static_cast<uint16_t>(thread_ctx().version), true, false, true,
      static_cast<uint32_t>(kpts.size()), pending_context.payload_size(), pending_context.optional_size()}};

#ifdef _TIMER
  auto t_memcpy_start = std::chrono::high_resolution_clock::now();
#endif

  // Copy in the payload.
  memcpy(record->payload(), pending_context.payload(), pending_context.payload_size());
  if(pending_context.optional_size() > 0) {
    char* ptr = record->payload() + pending_context.payload_size();
    for(const auto& option : pending_context.options()) {
      memcpy(ptr, option.payload, option.size);
      ptr += option.size;
      if (option.need_free) delete option.payload;
    }
  }

#ifdef _TIMER
  auto t_memcpy_end = std::chrono::high_resolution_clock::now();
  tot_memcpy_time += std::chrono::duration<double>(t_memcpy_end - t_memcpy_start).count();
#endif

  // Construct key pointers and swap them into hash chains. `kpt_address`
  // tracks the address for the key pointer we are currently dealing with.
  uint16_t i = 0;
  Address kpt_address = new_address;
  kpt_address += sizeof(RecordInfo);
  // Swap in all field-based key pointers.
  for(i = 0; i < n_general_pts; ++i) {
    // Initialzie the key pointer with its KPTUtil.
    KeyPointer* now = record->get_ptr(i);
    HashBucketEntry expected_entry = cas_helpers[i].expected_entry;
    new(now)
    KeyPointer(i, expected_entry.address().control(), kpts[i].general_psf_id,
               kpts[i].value_offset, kpts[i].value_size);

    // CAS Trick to swap in key pointer into the hash chain without storage
    // amplification. Invariant: pointers on the hash chain should always
    // point backwards, i.e., from high address to low address.
    AtomicHashBucketEntry* atomic_entry = cas_helpers[i].atomic_entry;
    HashBucketEntry updated_entry{kpt_address, kpts[i].hash.tag(), false};

    // Try to swap the hash bucket first.
    bool success;
    while(!(success = atomic_entry->compare_exchange_strong(
                        expected_entry, updated_entry))) {
      // Failure handle:
      // - If the hash bucket now points to an address lower than the
      // candidate, try again.
      // - Otherwise, we will never win the hash bucket since it will break
      // the invariant.
      if(expected_entry.address() < kpt_address) {
        now->prev_address = expected_entry.address().control();
      } else
        break;
    }

    if(!success) {
      // We found that we cannot win the hash bucket, try to win somewhere
      // else.
      KeyPointer* expected_kpt =
        reinterpret_cast<KeyPointer*>(hlog.Get(expected_entry.address()));
      KPTCASUtil expected_kpt_cas{expected_kpt->control.load()};
      do {
        // Find the first key pointer whose address is higher the candidate
        // while its previous address points to somewhere lower than the
        // candiate.
        while(expected_kpt_cas.prev_address > kpt_address.control()) {
          expected_kpt = reinterpret_cast<KeyPointer*>(
                           hlog.Get(expected_kpt_cas.prev_address));
          expected_kpt_cas.control = expected_kpt->control.load();
        }
        // Try to insert the key pointer right before the key pointer we just
        // found. If it fails again, recursively do this until succuess.
        now->prev_address = expected_kpt_cas.prev_address;
      } while(!expected_kpt->alter(kpt_address, expected_kpt_cas));
    }

    kpt_address += sizeof(KeyPointer);
  }
  // Swap in all predicate-based key pointers.
  for(; i < kpts.size(); ++i) {
    // Initialzie the key pointer with its KPTUtil.
    KeyPointer* now = record->get_ptr(i);
    HashBucketEntry expected_entry = cas_helpers[i].expected_entry;
    new(now) KeyPointer(i, expected_entry.address().control(),
                        kpts[i].inline_psf_id, kpts[i].value);

    // Same compare and swap trick as above.
    AtomicHashBucketEntry* atomic_entry = cas_helpers[i].atomic_entry;
    HashBucketEntry updated_entry{kpt_address, kpts[i].hash.tag(), false};
    bool success;
    while(!(success = atomic_entry->compare_exchange_strong(
                        expected_entry, updated_entry))) {
      if(expected_entry.address() < kpt_address) {
        now->prev_address = expected_entry.address().control();
      } else
        break;
    }

    if(!success) {
      KeyPointer* expected_kpt =
        reinterpret_cast<KeyPointer*>(hlog.Get(expected_entry.address()));
      KPTCASUtil expected_kpt_cas{expected_kpt->control.load()};
      do {
        while(expected_kpt_cas.prev_address > kpt_address.control()) {
          expected_kpt = reinterpret_cast<KeyPointer*>(
                           hlog.Get(expected_kpt_cas.prev_address));
          expected_kpt_cas.control = expected_kpt->control.load();
        }
        now->prev_address = expected_kpt_cas.prev_address;
      } while(!expected_kpt->alter(kpt_address, expected_kpt_cas));
    }

    kpt_address += sizeof(KeyPointer);
  }

  record->header.invalid = false;
  return OperationStatus::SUCCESS;
}

template <class D, class A>
template <class C>
inline OperationStatus FishStore<D, A>::InternalScan(C& pending_context,
    uint64_t start_addr, uint64_t end_addr) const {
  assert(start_addr < end_addr);
  typedef C pending_scan_context_t;

  KeyHash hash = pending_context.get_hash();

  if(thread_ctx().phase != Phase::REST) {
    const_cast<fishstore_t*>(this)->HeavyEnter();
  }

  const AtomicHashBucketEntry* atomic_entry = FindEntry(hash);
  if(!atomic_entry) {
    // Nothing found.
    // There is no NOT_FOUND status for Scan.
    // If nothing is found, still call finalize.
    pending_context.Finalize();
    return OperationStatus::SUCCESS;
  }

  HashBucketEntry entry = atomic_entry->load();
  Address address = entry.address();
  Address begin_address = hlog.begin_address.load();
  Address head_address = hlog.head_address.load();
  uint64_t latest_record_version = 0;

  const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(address));
  while(address >= end_addr && address >= head_address) {
    address = kpt->prev_address;
    kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(address));
  }

  // We only concern about one chain right now.
  if(address >= head_address) {
    const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(address));
    record_t* record = kpt->get_record();
    latest_record_version = record->header.checkpoint_version;
    if(record->header.invalid || !pending_context.check(kpt)) {
      address =
        TraceBackForMatch(pending_context, kpt->prev_address, head_address);
    }
  }

  switch(thread_ctx().phase) {
  case Phase::PREPARE:
    // Reading old version (v).
    if(latest_record_version > thread_ctx().version) {
      // CPR shift detected: we are in the "PREPARE" phase, and a record has a version later than
      // what we've seen.
      pending_context.go_async(thread_ctx().phase, thread_ctx().version, address);
      return OperationStatus::CPR_SHIFT_DETECTED;
    }
    break;
  default:
    break;
  }

  // Explore the hash chain until hit head_address. Thus, all in-memory records
  // on the chain will be touched.
  while(address >= start_addr && address >= head_address) {
#ifdef _SCAN_BENCH
    visited_address.emplace_back(address.control());
#endif
    const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(address));
    record_t* record = kpt->get_record();
    pending_context.Touch(record);
    address =
      TraceBackForMatch(pending_context, kpt->prev_address, head_address);
  }

  if(address < start_addr || address < hlog.begin_address.load()) {
    // Hitting the end of hash chain, scan is complete.
    pending_context.Finalize();
    return OperationStatus::SUCCESS;
  } else {
#ifdef _NULL_DISK
    pending_context.Finalize();
    return OperationStatus::SUCCESS;
#else
    // Kick async to explore the disk part of the hash chain.
    pending_context.go_async(thread_ctx().phase, thread_ctx().version, address,
                             start_addr, end_addr);
    return OperationStatus::SCAN_IN_PROGRESS;
#endif
  }
}

template <class D, class A>
template <class C>
inline OperationStatus FishStore<D, A>::InternalFullScan(C& pending_context,
    uint64_t begin_addr, uint64_t end_addr) {
  assert(begin_addr < end_addr);

  typedef C pending_full_scan_context_t;

  // Scan starts from the current address, go reverse page by page.
  Address to_address = hlog.GetTailAddress();
  if(end_addr < to_address.control()) to_address = end_addr;
  uint32_t current_page = to_address.page();
  Address start_address{begin_addr};
  while(current_page >= 0) {
    // If the page trying to read is below head page, ready to go into disk.
    Address address(current_page, 0);
    if(current_page < hlog.head_address.load().page()) break;

    // Scan the page, record by record to the end of the page or current tail.
    while(address <= to_address) {
      record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
      if(record->header.IsNull()) {
        address += sizeof(RecordInfo);
        continue;
      }

      if(address <= start_address) {
        address += record->size();
        continue;
      }

      if(!record->header.invalid &&
          pending_context.check(record->payload(), record->payload_size())) {
        pending_context.Touch(record);
      }

      address += record->size();
    }

    if(current_page == start_address.page() ||
        current_page == hlog.begin_address.load().page()) {
      // Finish scanning the whole log in memory.
      pending_context.Finalize();
      return OperationStatus::SUCCESS;
    }
    // Do a refresh after finish reading a page so that we can release the
    // protection for the page. Note that head address may change after
    // Refresh().
    Refresh();
    --current_page;
    to_address = Address{current_page, Address::kMaxOffset};
  }

#ifdef _NULL_DISK
  pending_context.Finalize();
  return OperationStatus::SUCCESS;
#else
  // Kick async to disk to scan log live on disk.
  pending_context.go_async(thread_ctx().phase, thread_ctx().version, Address{current_page, 0},
                           begin_addr, end_addr);
  return OperationStatus::SCAN_IN_PROGRESS;
#endif
}

template <class D, class A>
template <class C>
inline Address FishStore<D, A>::TraceBackForMatch(C& pending_context,
    Address from_address,
    Address min_offset) const {
  // The new trace back interface. Trace back based on key pointer now, relying
  // on the check function and key pointer based navigation to check record
  // payload.
  while(from_address >= min_offset) {
    const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(from_address));
    if(!kpt->get_record()->header.invalid && pending_context.check(kpt)) {
      return from_address;
    } else {
      from_address = kpt->prev_address;
      continue;
    }
  }
  return from_address;
}

template <class D, class A>
inline Status FishStore<D, A>::HandleOperationStatus(ExecutionContext& ctx,
    pending_context_t& pending_context, OperationStatus internal_status, bool& async) {
  async = false;
  switch(internal_status) {
  case OperationStatus::RETRY_NOW:
    // We do not retry full scan, since it may be super slow.
    // Should we stop doing rety for hash chain scan as well???
    switch(pending_context.type) {
    case OperationType::Read: {
      async_pending_read_context_t& read_context =
        *static_cast<async_pending_read_context_t*>(&pending_context);
      internal_status = InternalRead(read_context);
      break;
    }
    case OperationType::Insert: {
      async_pending_insert_context_t& insert_context =
        *static_cast<async_pending_insert_context_t*>(&pending_context);
      internal_status = InternalInsert(insert_context);
      break;
    }
    case OperationType::Scan: {
      async_pending_scan_context_t& scan_context =
        *static_cast<async_pending_scan_context_t*>(&pending_context);
      internal_status =
        InternalScan(scan_context, pending_context.start_addr.control(),
                    pending_context.end_addr.control());
      break;
    }
    default: {
      assert(false);
    }
    }

    if(internal_status == OperationStatus::SUCCESS) {
      return Status::Ok;
    } else {
      return HandleOperationStatus(ctx, pending_context, internal_status, async);
    }
  case OperationStatus::RETRY_LATER:
    return RetryLater(ctx, pending_context, async);
  case OperationStatus::RECORD_ON_DISK:
    if(thread_ctx().phase == Phase::PREPARE) {
      assert(pending_context.type == OperationType::Read);
      // Can I be marking an operation again and again?
      if(!checkpoint_locks_.get_lock(pending_context.get_hash()).try_lock_old()) {
        return PivotAndRetry(ctx, pending_context, async);
      }
    }
    return IssueAsyncIoRequest(ctx, pending_context, async);
  case OperationStatus::SCAN_IN_PROGRESS:
    // Only try mark or retry for hash chain scan.
    // FullScan is not protected against CPR. Should we do this to hash chain
    // as well?
    if(thread_ctx().phase == Phase::PREPARE &&
        pending_context.type == OperationType::Scan) {
      // Can I be marking an operation again and again?
      if(!checkpoint_locks_.get_lock(pending_context.get_hash()).try_lock_old()) {
        return PivotAndRetry(ctx, pending_context, async);
      }
    }
    return IssueAsyncScanRequest(ctx, pending_context, async);
  case OperationStatus::SUCCESS_UNMARK:
    checkpoint_locks_.get_lock(pending_context.get_hash()).unlock_old();
    return Status::Ok;
  case OperationStatus::NOT_FOUND_UNMARK:
    checkpoint_locks_.get_lock(pending_context.get_hash()).unlock_old();
    return Status::NotFound;
  case OperationStatus::CPR_SHIFT_DETECTED:
    return PivotAndRetry(ctx, pending_context, async);
  default: break;
  }
  // not reached
  assert(false);
  return Status::Corruption;
}

template <class D, class A>
inline Status FishStore<D, A>::PivotAndRetry(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  // Some invariants
  assert(ctx.version == thread_ctx().version);
  assert(thread_ctx().phase == Phase::PREPARE);
  Refresh();
  // thread must have moved to IN_PROGRESS phase
  assert(thread_ctx().version == ctx.version + 1);
  // retry with new contexts
  pending_context.phase = thread_ctx().phase;
  pending_context.version = thread_ctx().version;
  return HandleOperationStatus(thread_ctx(), pending_context, OperationStatus::RETRY_NOW, async);
}

template <class D, class A>
inline Status FishStore<D, A>::RetryLater(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  IAsyncContext* context_copy;
  Status result = pending_context.DeepCopy(context_copy);
  if(result == Status::Ok) {
    async = true;
    ctx.retry_requests.push_back(context_copy);
    return Status::Pending;
  } else {
    async = false;
    return result;
  }
}

template <class D, class A>
inline constexpr uint32_t FishStore<D, A>::MinIoRequestSize() const {
  return static_cast<uint32_t>(sizeof(KeyPointer));
}

template <class D, class A>
inline Status FishStore<D, A>::IssueAsyncIoRequest(ExecutionContext& ctx,
    pending_context_t& pending_context, bool& async) {
  // Issue asynchronous I/O request
  uint64_t io_id = thread_ctx().io_id++;
  thread_ctx().pending_ios.insert({ io_id, pending_context.get_hash() });
  thread_ctx().pending_io_cnt++;
  async = true;
  AsyncIOContext io_request{ this, pending_context.address, &pending_context,
                             &thread_ctx().io_responses, io_id };
  AsyncGetFromDisk(pending_context.address, MinIoRequestSize(), AsyncGetFromDiskCallback,
                   io_request);
  return Status::Pending;
}

template <class D, class A>
inline Status FishStore<D, A>::IssueAsyncScanRequest(
  ExecutionContext& ctx, pending_context_t& pending_context, bool& async) {
  // Issue asynchronous Scan I/O request
  uint64_t io_id = thread_ctx().io_id++;
  // Note that Full Scan is not bounded with any entry on the hash table.
  if(pending_context.type == OperationType::Scan) {
    thread_ctx().pending_ios.insert({io_id, pending_context.get_hash()});
  }
  thread_ctx().pending_io_cnt++;
  async = true;
  AsyncIOContext io_request{this, pending_context.address, &pending_context,
                            &thread_ctx().io_responses, io_id};

  // Based on different scan type, issue different type of IOs. Distinguished by
  // callback.
  if(pending_context.type == OperationType::Scan) {
    // Starting from IO level 0, i.e., pure random IO. MinIoRequestSize is the
    // size of a key pointer. Since the IO is sector aligned (4K aligned), we
    // can mostly hope to have the header or even the payload.
    assert(static_cast<async_pending_scan_context_t*>(&pending_context)->scan_offset_bit() == 0);
    AsyncGetFromDisk(pending_context.address, MinIoRequestSize(),
                     AsyncScanFromDiskCallback, io_request);
  } else {
    assert(pending_context.type == OperationType::FullScan);
    // Full Scan, issue IO page by page.
    AsyncGetFromDisk(pending_context.address, Address::kMaxOffset + 1,
                     AsyncFullScanFromDiskCallback, io_request);
  }
  return Status::Pending;
}

template <class D, class A>
inline Address FishStore<D, A>::BlockAllocate(uint32_t record_size) {
  uint32_t page;
  Address retval = hlog.Allocate(record_size, page);
  while(retval < hlog.read_only_address.load()) {
    Refresh();
    // Don't overrun the hlog's tail offset.
    bool page_closed = (retval == Address::kInvalidAddress);
    while(page_closed) {
      page_closed = !hlog.NewPage(page);
      Refresh();
    }
    retval = hlog.Allocate(record_size, page);
  }
  return retval;
}

template <class D, class A>
void FishStore<D, A>::AsyncGetFromDisk(Address address, uint32_t num_records,
                                       AsyncIOCallback callback, AsyncIOContext& context) {
  if(epoch_.IsProtected()) {
    /// Throttling. (Thread pool, unprotected threads are not throttled.)
    while(num_pending_ios.load() > 120) {
      disk.TryComplete();
      std::this_thread::yield();
      epoch_.ProtectAndDrain();
    }
  }
  ++num_pending_ios;
  hlog.AsyncGetFromDisk(address, num_records, callback, context);
}

template <class D, class A>
void FishStore<D, A>::AsyncGetFromDiskCallback(IAsyncContext* ctxt, Status result,
    size_t bytes_transferred) {
  CallbackContext<AsyncIOContext> context{ctxt};
  fishstore_t* fishstore = reinterpret_cast<fishstore_t*>(context->fishstore);
  auto pending_context =
    static_cast<async_pending_read_context_t*>(context->caller_context);

  --fishstore->num_pending_ios;

  context.async = true;

  // Some important numbers we need to refer in IO callback.
  // `context->record` is the sector aligned memory fetched by an IO.
  // `context->record.valid_offset` is the offset to requested IO address,
  // i.e., how many bytes before the requested address we can use.
  // `context->available_bytes` is the number of bytes that is available
  // starting from requested address. `context->kpt_offset` indicates how many
  // bytes between requested address to requested key pointer.
  pending_context->result = result;
  if(result == Status::Ok) {
    if(context->kpt_offset == 0) {
      // The returned IO region starts at a KeyPointer.
      KeyPointer* kpt =
        reinterpret_cast<KeyPointer*>(context->record.GetValidPointer());
      uint32_t ptr_offset = record_t::ptr_offset(kpt->ptr_offset);

      // Move context_address to the head of the record.
      context->address = context->address - ptr_offset;
      if(context->record.valid_offset < ptr_offset) {
        // We lost the header, do the IO again so that I can get the header.
        context->kpt_offset = ptr_offset;
        fishstore->AsyncGetFromDisk(context->address,
                                    fishstore->MinIoRequestSize() + ptr_offset,
                                    AsyncGetFromDiskCallback, *context.get());
        context.async = true;
      } else {
        // Now we have the header, we can infer the record size now.
        record_t* record = kpt->get_record();
        uint32_t available_bytes_for_record =
          ptr_offset + context->record.available_bytes;
        if(record->size() > available_bytes_for_record) {
          // We don't have the full record yet.
          context->kpt_offset = ptr_offset;
          fishstore->AsyncGetFromDisk(context->address, record->size(),
                                      AsyncGetFromDiskCallback, *context.get());
          context.async = true;
        } else if(!record->header.invalid && pending_context->check(kpt)) {
          // Got a hit.
          const char* data = record->payload();
          context->thread_io_responses->push(context.get());
        } else {
          context->address = kpt->prev_address;
          if(context->address >= fishstore->hlog.begin_address.load()) {
            // Does not match, traverse the chain. Reset kpt_offset back to 0,
            // since the context address is now pointing to a KeyPointer agian.
            fishstore->AsyncGetFromDisk(context->address,
                                        fishstore->MinIoRequestSize(),
                                        AsyncGetFromDiskCallback, *context.get());
          } else {
            // Hitting Invalid region. Finish hash chain exploration.
            context->thread_io_responses->push(context.get());
          }
        }
      }

    } else {
      // The retured IO region starts at a record header.
      record_t* record =
        reinterpret_cast<record_t*>(context->record.GetValidPointer());
      uint32_t available_bytes = context->record.available_bytes;
      assert(available_bytes >= context->kpt_offset + sizeof(KeyPointer));
      if(record->size() > available_bytes) {
        // We don't have the full record yet.
        fishstore->AsyncGetFromDisk(context->address, record->size(),
                                    AsyncGetFromDiskCallback, *context.get());
        context.async = true;
      } else {
        // Find the requested key pointer.
        KeyPointer* kpt = reinterpret_cast<KeyPointer*>(
                            context->record.GetValidPointer() + context->kpt_offset);
        if(!record->header.invalid && pending_context->check(kpt)) {
          // Got a hig.
          context->thread_io_responses->push(context.get());
        } else {
          context->address = kpt->prev_address;
          if(context->address >= fishstore->hlog.begin_address.load()) {
            // Does not match, traverse the chain. Reset kpt_offset back to 0,
            // since the context address is now pointing to a KeyPointer agian.
            context->kpt_offset = 0;
            fishstore->AsyncGetFromDisk(context->address,
                                        fishstore->MinIoRequestSize(),
                                        AsyncGetFromDiskCallback, *context.get());
          } else {
            // Hitting Invalid region. Finish hash chain exploration.
            context->thread_io_responses->push(context.get());
          }
        }
      }
    }
  }
}

// Scan IO callback based on adaptive IO callback.
template <class D, class A>
void FishStore<D, A>::AsyncScanFromDiskCallback(IAsyncContext* ctxt, Status result,
    size_t bytes_transferred) {
  CallbackContext<AsyncIOContext> context{ctxt};
  fishstore_t* fishstore = reinterpret_cast<fishstore_t*>(context->fishstore);
  auto pending_context =
    static_cast<async_pending_scan_context_t*>(context->caller_context);
  // Get the current adaptive prefetch level.
  auto current_scan_offset_bit = pending_context->scan_offset_bit();

  --fishstore->num_pending_ios;

  context.async = true;

  pending_context->result = result;
  if(result == Status::Ok) {
    // Common Utilities:
    // Term scan page indicates a page of the current IO level size.
    // `io_base` gives the base of IO, i.e., the base pointer to the requested
    // address. Note that request address is always the head of scan page. (when
    // IO level is 0, scan page head is exactly the original requested address)
    // `read_offset` gives the offset from `io_base` to the actual requested
    // address for a key pointer or record. Thus, the actual data we want starts
    // at `io_base + read_offset`. `scan_page` provides the current scan page
    // number, which can be used as an indicator to check if next record on the
    // chain is available in the current IO block. `available_bytes` gives the
    // available bytes starting from the actual requested address. Note that it
    // is not the available bytes starting from `io_base`. `valid_offset`
    // provides the number of bytes that is available before `io_base`.
    uint8_t* io_base = context->record.GetValidPointer();
    uint32_t read_offset =
      context->address.scan_offset(current_scan_offset_bit);
    uint64_t scan_page = context->address.scan_page(current_scan_offset_bit);
    uint32_t available_bytes = context->record.available_bytes - read_offset;
    uint32_t valid_offset = context->record.valid_offset;

    // If we still have not reach end_addr;
    if(context->address > pending_context->end_addr) {
      KeyPointer* kpt = reinterpret_cast<KeyPointer*>(io_base + read_offset);
      Address current_address = kpt->prev_address;
      uint64_t gap = (context->address - current_address).control();
      KeyPointer* kpt_now;
      // Explore the hash chain that has already retrieved. Stop either
      // hitting the end of the chain or hitting something outside the
      // current IO block.
      while(current_address >= pending_context->start_addr &&
            current_address >= fishstore->hlog.begin_address.load() &&
            current_address.scan_page(current_scan_offset_bit) == scan_page) {
        kpt_now = reinterpret_cast<KeyPointer*>(
                    io_base + current_address.scan_offset(current_scan_offset_bit));

        if(current_address <= pending_context->end_addr) {
          // If we got the key pointer but lose the header, break to issue
          // another IO block.
          if(current_address.scan_offset(current_scan_offset_bit) <
              record_t::ptr_offset(kpt_now->ptr_offset))
            break;

          record_t* record = kpt_now->get_record();
          if(!record->header.invalid && pending_context->check(kpt_now)) {
            // Got a hit on the chain.
            pending_context->Touch(record);
          }
        }

        gap = (current_address - kpt_now->prev_address).control();
        current_address = kpt_now->prev_address;
      }

      if(current_address < fishstore->hlog.begin_address.load() ||
          current_address < pending_context->start_addr) {
        // Hitting the end of chain, end of IOs.
        context->address = current_address;
        context->thread_io_responses->push(context.get());
      } else if(current_address.scan_page(current_scan_offset_bit) ==
                scan_page) {
        // Key pointer is in this IO block but lose the header.
        // Issuing another IO to get the header, meanwhile get the whole
        // scan page and hope for locality.
        context->address =
          current_address - record_t::ptr_offset(kpt_now->ptr_offset);
        context->kpt_offset = record_t::ptr_offset(kpt_now->ptr_offset);
        // IO size is the minimum to get the header while maintain the same
        // IO level.
        fishstore->AsyncGetFromDisk(
          context->address.scan_page_head(current_scan_offset_bit),
          std::max(context->kpt_offset + fishstore->MinIoRequestSize(),
                   Address::scan_page_size(current_scan_offset_bit) +
                   current_address.scan_offset(current_scan_offset_bit) +
                   fishstore->MinIoRequestSize()),
          AsyncScanFromDiskCallback, *context.get());
      } else {
        // Determine the new IO level based on the gap observed.
        pending_context->setIOLevel(gap);
        auto new_scan_offset_bit = pending_context->scan_offset_bit();

        // Issue the next IO with new scan page size, gurantees to get the
        // full key pointer at least.
        context->address = current_address;
        context->kpt_offset = 0;
        fishstore->AsyncGetFromDisk(
          current_address.scan_page_head(new_scan_offset_bit),
          std::max(Address::scan_page_size(new_scan_offset_bit),
                   current_address.scan_offset(new_scan_offset_bit) +
                   fishstore->MinIoRequestSize()),
          AsyncScanFromDiskCallback, *context.get());
      }

    } else if(context->kpt_offset == 0) {
      // The returned IO region starts at a KeyPointer.
      KeyPointer* kpt = reinterpret_cast<KeyPointer*>(io_base + read_offset);
      uint32_t ptr_offset = record_t::ptr_offset(kpt->ptr_offset);

      // Move context_address to the head of the record. Calculate the new scan
      // page head against the head of the record.
      context->address = context->address - ptr_offset;
#ifdef _SCAN_BENCH
      visited_address.emplace_back(context->address.control());
#endif
      Address scan_page_head =
        context->address.scan_page_head(current_scan_offset_bit);
      if(valid_offset + read_offset < ptr_offset) {
        // We lost the header, do the IO again so that I can get the header.
        // IO size at least includes the header up to the current key pointer.
        // Keep the IO level of next one same as the current IO.
        context->kpt_offset = ptr_offset;
        fishstore->AsyncGetFromDisk(
          scan_page_head,
          std::max(ptr_offset + fishstore->MinIoRequestSize(),
                   Address::scan_page_size(current_scan_offset_bit) +
                   read_offset + fishstore->MinIoRequestSize()),
          AsyncScanFromDiskCallback, *context.get());
        context.async = true;
      } else {
        // Now we have the header, we can infer the record size now.
        record_t* record = kpt->get_record();
        uint32_t available_bytes_for_record = ptr_offset + available_bytes;
        if(record->size() > available_bytes_for_record) {
          // We don't have the full record yet. Issue another IO for the full
          // record.
          context->kpt_offset = ptr_offset;
          fishstore->AsyncGetFromDisk(scan_page_head,
                                      read_offset - ptr_offset + record->size(),
                                      AsyncScanFromDiskCallback, *context.get());
          context.async = true;
        } else {
          // Check the first record see if it is valid.
          if(!record->header.invalid && pending_context->check(kpt)) {
            pending_context->Touch(record);
          }

          // Keep track of the gap between the current record and the previous
          // one in the chain.
          Address current_address = kpt->prev_address;
          uint64_t gap = (context->address - current_address).control();
          KeyPointer* kpt_now;
          // Explore the hash chain that has already retrieved. Stop either
          // hitting the end of the chain or hitting something outside the
          // current IO block.
          while(current_address >= pending_context->start_addr &&
                current_address >= fishstore->hlog.begin_address.load() &&
                current_address.scan_page(current_scan_offset_bit) ==
                scan_page) {
            kpt_now = reinterpret_cast<KeyPointer*>(
                        io_base + current_address.scan_offset(current_scan_offset_bit));

            // If we got the key pointer but lose the header, break to issue
            // another IO block.
            if(current_address.scan_offset(current_scan_offset_bit) <
                record_t::ptr_offset(kpt_now->ptr_offset))
              break;

            record_t* record = kpt_now->get_record();
            if(!record->header.invalid && pending_context->check(kpt_now)) {
              // Got a hit on the chain.
              pending_context->Touch(record);
            }

            // calculating the gap.
            gap = (current_address - kpt_now->prev_address).control();
            current_address = kpt_now->prev_address;
          }

          if(current_address < pending_context->start_addr ||
              current_address < fishstore->hlog.begin_address.load()) {
            // Hitting the end of chain, end of IOs.
            context->address = current_address;
            context->thread_io_responses->push(context.get());
          } else if(current_address.scan_page(current_scan_offset_bit) ==
                    scan_page) {
            // Key pointer is in this IO block but lose the header.
            // Issuing another IO to get the header, meanwhile get the whole
            // scan page and hope for locality.
            context->address =
              current_address - record_t::ptr_offset(kpt_now->ptr_offset);
            context->kpt_offset = record_t::ptr_offset(kpt_now->ptr_offset);
            // IO size is the minimum to get the header while maintain the same
            // IO level.
            fishstore->AsyncGetFromDisk(
              context->address.scan_page_head(current_scan_offset_bit),
              std::max(
                context->kpt_offset + fishstore->MinIoRequestSize(),
                Address::scan_page_size(current_scan_offset_bit) +
                current_address.scan_offset(current_scan_offset_bit) +
                fishstore->MinIoRequestSize()),
              AsyncScanFromDiskCallback, *context.get());
          } else {
            // Determine the new IO level based on the gap observed.
            pending_context->setIOLevel(gap);
            auto new_scan_offset_bit = pending_context->scan_offset_bit();

            // Issue the next IO with new scan page size, gurantees to get the
            // full key pointer at least.
            context->address = current_address;
            context->kpt_offset = 0;
            fishstore->AsyncGetFromDisk(
              current_address.scan_page_head(new_scan_offset_bit),
              std::max(Address::scan_page_size(new_scan_offset_bit),
                       current_address.scan_offset(new_scan_offset_bit) +
                       fishstore->MinIoRequestSize()),
              AsyncScanFromDiskCallback, *context.get());
          }
        }
      }

    } else {
      // The retured IO region starts at a record header.
      record_t* record = reinterpret_cast<record_t*>(io_base + read_offset);
      assert(available_bytes >= context->kpt_offset + sizeof(KeyPointer));
      if(record->size() > available_bytes) {
        // We don't have the full record yet.
        fishstore->AsyncGetFromDisk(
          context->address.scan_page_head(current_scan_offset_bit),
          read_offset + record->size(), AsyncScanFromDiskCallback,
          *context.get());
        context.async = true;
      } else {
        // Similar IO path as above.
        KeyPointer* kpt = reinterpret_cast<KeyPointer*>(io_base + read_offset +
                          context->kpt_offset);

        if(!record->header.invalid && pending_context->check(kpt)) {
          pending_context->Touch(record);
        }

        Address current_address = kpt->prev_address;
        uint64_t gap = (context->address - current_address).control();
        KeyPointer* kpt_now = nullptr;
        while(current_address >= pending_context->start_addr &&
              current_address >= fishstore->hlog.begin_address.load() &&
              current_address.scan_page(current_scan_offset_bit) == scan_page) {

          kpt_now = reinterpret_cast<KeyPointer*>(io_base + current_address.scan_offset(current_scan_offset_bit));
          if(current_address.scan_offset(current_scan_offset_bit) < record_t::ptr_offset(kpt_now->ptr_offset))
            break;

          record_t* record = kpt_now->get_record();
          if(!record->header.invalid && pending_context->check(kpt_now)) {
            pending_context->Touch(record);
          }
          gap = (current_address - kpt_now->prev_address).control();
          current_address = kpt_now->prev_address;
        }

        if(current_address < pending_context->start_addr ||
            current_address < fishstore->hlog.begin_address.load()) {
          context->address = current_address;
          context->thread_io_responses->push(context.get());
        } else if(current_address.scan_page(current_scan_offset_bit) == scan_page) {
          context->address =
            current_address - record_t::ptr_offset(kpt_now->ptr_offset);
          context->kpt_offset = record_t::ptr_offset(kpt_now->ptr_offset);
          fishstore->AsyncGetFromDisk(
            context->address.scan_page_head(current_scan_offset_bit),
            std::max(
              context->kpt_offset + fishstore->MinIoRequestSize(),
              Address::scan_page_size(current_scan_offset_bit) +
              current_address.scan_offset(current_scan_offset_bit) +
              fishstore->MinIoRequestSize()),
            AsyncScanFromDiskCallback, *context.get());
        } else {
          pending_context->setIOLevel(gap);
          auto new_scan_offset_bit = pending_context->scan_offset_bit();

          context->address = current_address;
          context->kpt_offset = 0;
          fishstore->AsyncGetFromDisk(
            current_address.scan_page_head(new_scan_offset_bit),
            std::max(Address::scan_page_size(new_scan_offset_bit),
                     current_address.scan_offset(new_scan_offset_bit) +
                     fishstore->MinIoRequestSize()),
            AsyncScanFromDiskCallback, *context.get());
        }
      }
    }
  }
}

template <class D, class A>
void FishStore<D, A>::AsyncFullScanFromDiskCallback(IAsyncContext* ctxt,
    Status result,
    size_t bytes_transferred) {
  CallbackContext<AsyncIOContext> context{ctxt};
  fishstore_t* fishstore = reinterpret_cast<fishstore_t*>(context->fishstore);
  auto pending_context =
    static_cast<async_pending_full_scan_context_t*>(context->caller_context);

  --fishstore->num_pending_ios;
  context.async = true;

  pending_context->result = result;
  if(result == Status::Ok) {
    // Always have the full page.
    assert(context->record.available_bytes >= Address::kMaxOffset + 1);
    uintptr_t from_address, to_address;
    if(context->address.page() == pending_context->start_addr.page()) {
      from_address =
        reinterpret_cast<uintptr_t>(context->record.GetValidPointer() +
                                    pending_context->start_addr.offset());
    } else
      from_address =
        reinterpret_cast<uintptr_t>(context->record.GetValidPointer());

    if(context->address.page() == pending_context->end_addr.page()) {
      to_address =
        reinterpret_cast<uintptr_t>(context->record.GetValidPointer() +
                                    pending_context->end_addr.offset());
    } else
      to_address = reinterpret_cast<uintptr_t>(
                     context->record.GetValidPointer() + +Address::kMaxOffset);

    uintptr_t address;
    // Iterate through all records in the page.
    for(address = from_address; address <= to_address;) {
      record_t* record = reinterpret_cast<record_t*>(address);
      if(record->header.IsNull()) {
        address += sizeof(RecordInfo);
        continue;
      }
      if(!record->header.invalid &&
          pending_context->check(record->payload(), record->payload_size())) {
        // Got a hit.
        pending_context->Touch(record);
      }
      address += record->size();
    }

    if(context->address.page() == fishstore->hlog.begin_address.load().page() ||
        context->address.page() == pending_context->start_addr.page()) {
      // Hitting the begin page, finish full scan.
      context->thread_io_responses->push(context.get());
    } else {
      // Issue an IO to retrieve the previous page.
      context->address = Address{context->address.page() - 1, 0};
      fishstore->AsyncGetFromDisk(context->address, Address::kMaxOffset + 1,
                                  AsyncFullScanFromDiskCallback, *context.get());
      context.async = true;
    }
  }
}

template <class D, class A>
OperationStatus FishStore<D, A>::InternalContinuePendingRead(ExecutionContext& context,
    AsyncIOContext& io_context) {
  if(io_context.address >= hlog.begin_address.load()) {
    async_pending_read_context_t* pending_context = static_cast<async_pending_read_context_t*>(
          io_context.caller_context);
    record_t* record = reinterpret_cast<record_t*>(io_context.record.GetValidPointer());
    pending_context->Get(record);
    assert(!kCopyReadsToTail);
    return (thread_ctx().version > context.version) ? OperationStatus::SUCCESS_UNMARK :
           OperationStatus::SUCCESS;
  } else {
    return (thread_ctx().version > context.version) ? OperationStatus::NOT_FOUND_UNMARK :
           OperationStatus::NOT_FOUND;
  }
}

template <class D, class A>
OperationStatus FishStore<D, A>::InternalContinuePendingScan(
  ExecutionContext& context, AsyncIOContext& io_context) {
  // Scan must go to the end of hash chain
  pending_context_t* pending_context =
    static_cast<pending_context_t*>(io_context.caller_context);
  assert(io_context.address < pending_context->start_addr ||
         io_context.address < hlog.begin_address.load());
  if(pending_context->type == OperationType::Scan) {
    // If it is a scan, finalize and check marking.
    async_pending_scan_context_t* scan_context =
      static_cast<async_pending_scan_context_t*>(pending_context);
    scan_context->Finalize();
    return (thread_ctx().version > context.version)
           ? OperationStatus::SUCCESS_UNMARK
           : OperationStatus::SUCCESS;
  } else {
    // If it is a full scan, nothing we need to concern about hash table.
    // Finalize the scan and return success.
    assert(pending_context->type == OperationType::FullScan);
    async_pending_full_scan_context_t* full_scan_context =
      static_cast<async_pending_full_scan_context_t*>(pending_context);
    full_scan_context->Finalize();
    return OperationStatus::SUCCESS;
  }
}

template <class D, class A>
void FishStore<D, A>::InitializeCheckpointLocks() {
  uint32_t table_version = resize_info_.version;
  uint64_t size = state_[table_version].size();
  checkpoint_locks_.Initialize(size);
}

template <class D, class A>
Status FishStore<D, A>::WriteIndexMetadata() {
  std::string filename = disk.index_checkpoint_path(checkpoint_.index_token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::ReadIndexMetadata(const Guid& token) {
  std::string filename = disk.index_checkpoint_path(token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "rb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fread(&checkpoint_.index_metadata, sizeof(checkpoint_.index_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::WriteCprMetadata() {
  std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::ReadCprMetadata(const Guid& token) {
  std::string filename = disk.cpr_checkpoint_path(token) + "info.dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "rb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fread(&checkpoint_.log_metadata, sizeof(checkpoint_.log_metadata), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::WriteCprContext() {
  std::string filename = disk.cpr_checkpoint_path(checkpoint_.hybrid_log_token);
  const Guid& guid = prev_thread_ctx().guid;
  filename += guid.ToString();
  filename += ".dat";
  // (This code will need to be refactored into the disk_t interface, if we want to support
  // unformatted disks.)
  std::FILE* file = std::fopen(filename.c_str(), "wb");
  if(!file) {
    return Status::IOError;
  }
  if(std::fwrite(static_cast<PersistentExecContext*>(&prev_thread_ctx()),
                 sizeof(PersistentExecContext), 1, file) != 1) {
    std::fclose(file);
    return Status::IOError;
  }
  if(std::fclose(file) != 0) {
    return Status::IOError;
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::ReadCprContexts(const Guid& token, const Guid* guids) {
  for(size_t idx = 0; idx < Thread::kMaxNumThreads; ++idx) {
    const Guid& guid = guids[idx];
    if(guid == Guid{}) {
      continue;
    }
    std::string filename = disk.cpr_checkpoint_path(token);
    filename += guid.ToString();
    filename += ".dat";
    // (This code will need to be refactored into the disk_t interface, if we want to support
    // unformatted disks.)
    std::FILE* file = std::fopen(filename.c_str(), "rb");
    if(!file) {
      return Status::IOError;
    }
    PersistentExecContext context{};
    if(std::fread(&context, sizeof(PersistentExecContext), 1, file) != 1) {
      std::fclose(file);
      return Status::IOError;
    }
    if(std::fclose(file) != 0) {
      return Status::IOError;
    }
    auto result = checkpoint_.continue_tokens.insert({ context.guid, std::make_pair(context.serial_num, context.offset) });
    assert(result.second);
  }
  if(checkpoint_.continue_tokens.size() != checkpoint_.log_metadata.num_threads) {
    return Status::Corruption;
  } else {
    return Status::Ok;
  }
}

template <class D, class A>
Status FishStore<D, A>::CheckpointFuzzyIndex() {
  uint32_t hash_table_version = resize_info_.version;
  // Checkpoint the main hash table.
  file_t ht_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&disk.handler()));
  RETURN_NOT_OK(state_[hash_table_version].Checkpoint(disk, std::move(ht_file),
                checkpoint_.index_metadata.num_ht_bytes));
  // Checkpoint the hash table's overflow buckets.
  file_t ofb_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                 "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&disk.handler()));
  RETURN_NOT_OK(overflow_buckets_allocator_[hash_table_version].Checkpoint(disk,
                std::move(ofb_file), checkpoint_.index_metadata.num_ofb_bytes));
  checkpoint_.index_checkpoint_started = true;
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::CheckpointFuzzyIndexComplete() {
  if(!checkpoint_.index_checkpoint_started) {
    return Status::Pending;
  }
  uint32_t hash_table_version = resize_info_.version;
  Status result = state_[hash_table_version].CheckpointComplete(false);
  if(result == Status::Pending) {
    return Status::Pending;
  } else if(result != Status::Ok) {
    return result;
  } else {
    return overflow_buckets_allocator_[hash_table_version].CheckpointComplete(false);
  }
}

template <class D, class A>
Status FishStore<D, A>::RecoverFuzzyIndex() {
  uint8_t hash_table_version = resize_info_.version;
  assert(state_[hash_table_version].size() == checkpoint_.index_metadata.table_size);

  // Recover the main hash table.
  file_t ht_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                "ht.dat");
  RETURN_NOT_OK(ht_file.Open(&disk.handler()));
  RETURN_NOT_OK(state_[hash_table_version].Recover(disk, std::move(ht_file),
                checkpoint_.index_metadata.num_ht_bytes));
  // Recover the hash table's overflow buckets.
  file_t ofb_file = disk.NewFile(disk.relative_index_checkpoint_path(checkpoint_.index_token) +
                                 "ofb.dat");
  RETURN_NOT_OK(ofb_file.Open(&disk.handler()));
  return overflow_buckets_allocator_[hash_table_version].Recover(disk, std::move(ofb_file),
         checkpoint_.index_metadata.num_ofb_bytes, checkpoint_.index_metadata.ofb_count);
}

template <class D, class A>
Status FishStore<D, A>::RecoverFuzzyIndexComplete(bool wait) {
  uint8_t hash_table_version = resize_info_.version;
  Status result = state_[hash_table_version].RecoverComplete(true);
  if(result != Status::Ok) {
    return result;
  }
  result = overflow_buckets_allocator_[hash_table_version].RecoverComplete(true);
  if(result != Status::Ok) {
    return result;
  }

  // Clear all tentative entries.
  for(uint64_t bucket_idx = 0; bucket_idx < state_[hash_table_version].size(); ++bucket_idx) {
    HashBucket* bucket = &state_[hash_table_version].bucket(bucket_idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        if(bucket->entries[entry_idx].load().tentative()) {
          bucket->entries[entry_idx].store(HashBucketEntry::kInvalidEntry);
        }
      }
      // Go to next bucket in the chain
      HashBucketOverflowEntry entry = bucket->overflow_entry.load();
      if(entry.unused()) {
        // No more buckets in the chain.
        break;
      }
      bucket = &overflow_buckets_allocator_[hash_table_version].Get(entry.address());
      assert(reinterpret_cast<size_t>(bucket) % Constants::kCacheLineBytes == 0);
    }
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::RecoverHybridLog() {
  class Context : public IAsyncContext {
   public:
    Context(hlog_t& hlog_, uint32_t page_, RecoveryStatus& recovery_status_)
      : hlog{ &hlog_}
      , page{ page_ }
      , recovery_status{ &recovery_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : hlog{ other.hlog }
      , page{ other.page }
      , recovery_status{ other.recovery_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    hlog_t* hlog;
    uint32_t page;
    RecoveryStatus* recovery_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<Context> context{ ctxt };
    result = context->hlog->AsyncReadPagesFromLog(context->page, 1, *context->recovery_status);
  };

  Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
  Address to_address = checkpoint_.log_metadata.final_address;

  uint32_t start_page = from_address.page();
  uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
  uint32_t capacity = hlog.buffer_size();
  RecoveryStatus recovery_status{ start_page, end_page };
  // Initially issue read request for all pages that can be held in memory
  uint32_t total_pages_to_read = end_page - start_page;
  uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
  RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, pages_to_read_first, recovery_status));

  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }

    // handle start and end at non-page boundaries
    RETURN_NOT_OK(RecoverFromPage(page == start_page ? from_address : Address{ page, 0 },
                                  page + 1 == end_page ? to_address :
                                  Address{ page, Address::kMaxOffset }));

    // OS thread flushes current page and issues a read request if necessary
    if(page + capacity < end_page) {
      Context context{ hlog, page + capacity, recovery_status };
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
    } else {
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
    }
  }
  // Wait until all pages have been flushed
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::RecoverHybridLogFromSnapshotFile() {
  class Context : public IAsyncContext {
   public:
    Context(hlog_t& hlog_, file_t& file_, uint32_t file_start_page_, uint32_t page_,
            RecoveryStatus& recovery_status_)
      : hlog{ &hlog_ }
      , file{ &file_ }
      , file_start_page{ file_start_page_ }
      , page{ page_ }
      , recovery_status{ &recovery_status_ } {
    }
    /// The deep-copy constructor
    Context(const Context& other)
      : hlog{ other.hlog }
      , file{ other.file }
      , file_start_page{ other.file_start_page }
      , page{ other.page }
      , recovery_status{ other.recovery_status } {
    }
   protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
      return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
   public:
    hlog_t* hlog;
    file_t* file;
    uint32_t file_start_page;
    uint32_t page;
    RecoveryStatus* recovery_status;
  };

  auto callback = [](IAsyncContext* ctxt, Status result) {
    CallbackContext<Context> context{ ctxt };
    result = context->hlog->AsyncReadPagesFromSnapshot(*context->file,
             context->file_start_page, context->page, 1, *context->recovery_status);
  };

  Address file_start_address = checkpoint_.log_metadata.flushed_address;
  Address from_address = checkpoint_.index_metadata.checkpoint_start_address;
  Address to_address = checkpoint_.log_metadata.final_address;

  uint32_t start_page = file_start_address.page();
  uint32_t end_page = to_address.offset() > 0 ? to_address.page() + 1 : to_address.page();
  uint32_t capacity = hlog.buffer_size();
  RecoveryStatus recovery_status{ start_page, end_page };
  checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                checkpoint_.hybrid_log_token) + "snapshot.dat");
  RETURN_NOT_OK(checkpoint_.snapshot_file.Open(&disk.handler()));

  // Initially issue read request for all pages that can be held in memory
  uint32_t total_pages_to_read = end_page - start_page;
  uint32_t pages_to_read_first = std::min(capacity, total_pages_to_read);
  RETURN_NOT_OK(hlog.AsyncReadPagesFromSnapshot(checkpoint_.snapshot_file, start_page, start_page,
                pages_to_read_first, recovery_status));

  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }

    // Perform recovery if page in fuzzy portion of the log
    if(Address{ page + 1, 0 } > from_address) {
      // handle start and end at non-page boundaries
      RETURN_NOT_OK(RecoverFromPage(page == from_address.page() ? from_address :
                                    Address{ page, 0 },
                                    page + 1 == end_page ? to_address :
                                    Address{ page, Address::kMaxOffset }));
    }

    // OS thread flushes current page and issues a read request if necessary
    if(page + capacity < end_page) {
      Context context{ hlog, checkpoint_.snapshot_file, start_page, page + capacity,
                       recovery_status };
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, callback, &context));
    } else {
      RETURN_NOT_OK(hlog.AsyncFlushPage(page, recovery_status, nullptr, nullptr));
    }
  }
  // Wait until all pages have been flushed
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::FlushDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::RecoverFromPage(Address from_address, Address to_address) {
  assert(from_address.page() == to_address.page());
  for(Address address = from_address; address < to_address;) {
    record_t* record = reinterpret_cast<record_t*>(hlog.Get(address));
    if(record->header.IsNull()) {
      address += sizeof(record->header);
      continue;
    }
    if(record->header.invalid) {
      address += record->size();
      continue;
    }
    // Reconstruct hash chain.
    if(record->header.checkpoint_version <= checkpoint_.log_metadata.version) {
      Address ptr_address = address;
      ptr_address += sizeof(RecordInfo);
      for(uint16_t i = 0; i < record->header.ptr_cnt; ++i) {
        KeyPointer* now = record->get_ptr(i);
        KeyHash hash;
        if(now->mode == 0) {
          hash = Utility::HashBytesWithPSFID(now->general_psf_id, now->get_value(),
                                             now->value_size);
        } else if(now->mode == 1) {
          hash = Utility::GetHashCode(now->inline_psf_id, now->value);
        }
        HashBucketEntry expected_entry;
        HashBucket* bucket;
        AtomicHashBucketEntry* atomic_entry =
          FindOrCreateEntry(hash, expected_entry, bucket);
        HashBucketEntry new_entry{ptr_address, hash.tag(), false};
        atomic_entry->store(new_entry);
        ptr_address += sizeof(KeyPointer);
      }
    } else {
      record->header.invalid = true;
    }
    address += record->size();
  }

  return Status::Ok;
}

template <class D, class A>
Status FishStore<D, A>::RestoreHybridLog() {
  Address tail_address = checkpoint_.log_metadata.final_address;
  uint32_t end_page = tail_address.offset() > 0 ? tail_address.page() + 1 : tail_address.page();
  uint32_t capacity = hlog.buffer_size();
  // Restore as much of the log as will fit in memory.
  uint32_t start_page;
  if(end_page < capacity - hlog.kNumHeadPages) {
    start_page = 0;
  } else {
    start_page = end_page - (capacity - hlog.kNumHeadPages);
  }
  RecoveryStatus recovery_status{ start_page, end_page };

  uint32_t num_pages = end_page - start_page;
  RETURN_NOT_OK(hlog.AsyncReadPagesFromLog(start_page, num_pages, recovery_status));

  // Wait until all pages have been read.
  for(uint32_t page = start_page; page < end_page; ++page) {
    while(recovery_status.page_status(page) != PageRecoveryStatus::ReadDone) {
      disk.TryComplete();
      std::this_thread::sleep_for(10ms);
    }
  }
  // Skip the null page.
  Address head_address = start_page == 0 ? Address{ 0, Constants::kCacheLineBytes } :
                         Address{ start_page, 0 };
  hlog.RecoveryReset(checkpoint_.index_metadata.log_begin_address, head_address, tail_address);
  return Status::Ok;
}

template <class D, class A>
void FishStore<D, A>::HeavyEnter() {
  if(thread_ctx().phase == Phase::GC_IO_PENDING || thread_ctx().phase == Phase::GC_IN_PROGRESS) {
    CleanHashTableBuckets();
    return;
  }
  while(thread_ctx().phase == Phase::GROW_PREPARE) {
    // We spin-wait as a simplification
    // Could instead do a "heavy operation" here
    std::this_thread::yield();
    Refresh();
  }
  if(thread_ctx().phase == Phase::GROW_IN_PROGRESS) {
    SplitHashTableBuckets();
  }
}

template <class D, class A>
bool FishStore<D, A>::CleanHashTableBuckets() {
  uint64_t chunk = gc_.next_chunk++;
  if(chunk >= gc_.num_chunks) {
    // No chunk left to clean.
    return false;
  }
  uint8_t version = resize_info_.version;
  Address begin_address = hlog.begin_address.load();
  uint64_t upper_bound;
  if(chunk + 1 < grow_.num_chunks) {
    // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
    upper_bound = kGrowHashTableChunkSize;
  } else {
    // Last chunk might contain more or fewer elements.
    upper_bound = state_[version].size() - (chunk * kGcHashTableChunkSize);
  }
  for(uint64_t idx = 0; idx < upper_bound; ++idx) {
    HashBucket* bucket = &state_[version].bucket(chunk * kGcHashTableChunkSize + idx);
    while(true) {
      for(uint32_t entry_idx = 0; entry_idx < HashBucket::kNumEntries; ++entry_idx) {
        AtomicHashBucketEntry& atomic_entry = bucket->entries[entry_idx];
        HashBucketEntry expected_entry = atomic_entry.load();
        if(!expected_entry.unused() && expected_entry.address() != Address::kInvalidAddress &&
            expected_entry.address() < begin_address) {
          // The record that this entry points to was truncated; try to delete the entry.
          atomic_entry.compare_exchange_strong(expected_entry, HashBucketEntry::kInvalidEntry);
          // If deletion failed, then some other thread must have added a new record to the entry.
        }
      }
      // Go to next bucket in the chain.
      HashBucketOverflowEntry overflow_entry = bucket->overflow_entry.load();
      if(overflow_entry.unused()) {
        // No more buckets in the chain.
        break;
      }
      bucket = &overflow_buckets_allocator_[version].Get(overflow_entry.address());
    }
  }
  // Done with this chunk--did some work.
  return true;
}

template <class D, class A>
void FishStore<D, A>::AddHashEntry(HashBucket*& bucket, uint32_t& next_idx, uint8_t version,
                                   HashBucketEntry entry) {
  if(next_idx == HashBucket::kNumEntries) {
    // Need to allocate a new bucket, first.
    FixedPageAddress new_bucket_addr = overflow_buckets_allocator_[version].Allocate();
    HashBucketOverflowEntry new_bucket_entry{ new_bucket_addr };
    bucket->overflow_entry.store(new_bucket_entry);
    bucket = &overflow_buckets_allocator_[version].Get(new_bucket_addr);
    next_idx = 0;
  }
  bucket->entries[next_idx].store(entry);
  ++next_idx;
}

template <class D, class A>
Address FishStore<D, A>::TraceBackForOtherChainStart(uint64_t old_size, uint64_t new_size,
    Address from_address, Address min_address, uint8_t side) {
  assert(side == 0 || side == 1);
  // Search back as far as min_address.
  while(from_address >= min_address) {
    const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(from_address));
    KeyHash hash = kpt->get_hash();
    if((hash.idx(new_size) < old_size) != (side == 0)) {
      // Record's key hashes to the other side.
      return from_address;
    }
    from_address = kpt->prev_address;
  }
  return from_address;
}

template <class D, class A>
void FishStore<D, A>::SplitHashTableBuckets() {
  // This thread won't exit until all hash table buckets have been split.
  Address head_address = hlog.head_address.load();
  Address begin_address = hlog.begin_address.load();
  for(uint64_t chunk = grow_.next_chunk++; chunk < grow_.num_chunks; chunk = grow_.next_chunk++) {
    uint64_t old_size = state_[grow_.old_version].size();
    uint64_t new_size = state_[grow_.new_version].size();
    assert(new_size == old_size * 2);
    // Split this chunk.
    uint64_t upper_bound;
    if(chunk + 1 < grow_.num_chunks) {
      // All chunks but the last chunk contain kGrowHashTableChunkSize elements.
      upper_bound = kGrowHashTableChunkSize;
    } else {
      // Last chunk might contain more or fewer elements.
      upper_bound = old_size - (chunk * kGrowHashTableChunkSize);
    }
    for(uint64_t idx = 0; idx < upper_bound; ++idx) {

      // Split this (chain of) bucket(s).
      HashBucket* old_bucket = &state_[grow_.old_version].bucket(
                                 chunk * kGrowHashTableChunkSize + idx);
      HashBucket* new_bucket0 = &state_[grow_.new_version].bucket(
                                  chunk * kGrowHashTableChunkSize + idx);
      HashBucket* new_bucket1 = &state_[grow_.new_version].bucket(
                                  old_size + chunk * kGrowHashTableChunkSize + idx);
      uint32_t new_entry_idx0 = 0;
      uint32_t new_entry_idx1 = 0;
      while(true) {
        for(uint32_t old_entry_idx = 0; old_entry_idx < HashBucket::kNumEntries; ++old_entry_idx) {
          HashBucketEntry old_entry = old_bucket->entries[old_entry_idx].load();
          if(old_entry.unused()) {
            // Nothing to do.
            continue;
          } else if(old_entry.address() < head_address) {
            // Can't tell which new bucket the entry should go into; put it in both.
            AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version, old_entry);
            AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version, old_entry);
            continue;
          }

          const KeyPointer* kpt = reinterpret_cast<const KeyPointer*>(hlog.Get(old_entry.address()));
          KeyHash hash = kpt->get_hash();
          if(hash.idx(new_size) < old_size) {
            // Record's key hashes to the 0 side of the new hash table.
            AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart(old_size, new_size,
                                    kpt->prev_address, head_address, 0);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 1 side of
              // the new hash table.
              AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version,
                           HashBucketEntry{ other_address, old_entry.tag(), false });
            }
          } else {
            // Record's key hashes to the 1 side of the new hash table.
            AddHashEntry(new_bucket1, new_entry_idx1, grow_.new_version, old_entry);
            Address other_address = TraceBackForOtherChainStart(old_size, new_size,
                                    kpt->prev_address, head_address, 1);
            if(other_address >= begin_address) {
              // We found a record that either is on disk or has a key that hashes to the 0 side of
              // the new hash table.
              AddHashEntry(new_bucket0, new_entry_idx0, grow_.new_version,
                           HashBucketEntry{ other_address, old_entry.tag(), false });
            }
          }
        }
        // Go to next bucket in the chain.
        HashBucketOverflowEntry overflow_entry = old_bucket->overflow_entry.load();
        if(overflow_entry.unused()) {
          // No more buckets in the chain.
          break;
        }
        old_bucket = &overflow_buckets_allocator_[grow_.old_version].Get(overflow_entry.address());
      }
    }
    // Done with this chunk.
    if(--grow_.num_pending_chunks == 0) {
      // Free the old hash table.
      state_[grow_.old_version].Uninitialize();
      overflow_buckets_allocator_[grow_.old_version].Uninitialize();
      break;
    }
  }
  // Thread has finished growing its part of the hash table.
  thread_ctx().phase = Phase::REST;
  // Thread ack that it has finished growing the hash table.
  if(epoch_.FinishThreadPhase(Phase::GROW_IN_PROGRESS)) {
    // Let other threads know that they can use the new hash table now.
    GlobalMoveToNextState(SystemState{ Action::GrowIndex, Phase::GROW_IN_PROGRESS,
                                       thread_ctx().version });
  } else {
    while(system_state_.load().phase == Phase::GROW_IN_PROGRESS) {
      // Spin until all other threads have finished splitting their chunks.
      std::this_thread::yield();
    }
  }
}

template <class D, class A>
bool FishStore<D, A>::GlobalMoveToNextState(SystemState current_state) {
  SystemState next_state = current_state.GetNextState();
  if(!system_state_.compare_exchange_strong(current_state, next_state)) {
    return false;
  }

  switch(next_state.action) {
  case Action::CheckpointFull:
  case Action::CheckpointIndex:
  case Action::CheckpointHybridLog:
    switch(next_state.phase) {
    case Phase::PREP_INDEX_CHKPT:
      // This case is handled directly inside Checkpoint[Index]().
      assert(false);
      break;
    case Phase::INDEX_CHKPT:
      assert(next_state.action != Action::CheckpointHybridLog);
      // Issue async request for fuzzy checkpoint
      assert(!checkpoint_.failed);
      if(CheckpointFuzzyIndex() != Status::Ok) {
        checkpoint_.failed = true;
      }
      break;
    case Phase::PREPARE:
      // Index checkpoint will never reach this state; and CheckpointHybridLog() will handle this
      // case directly.
      assert(next_state.action == Action::CheckpointFull);
      // INDEX_CHKPT -> PREPARE
      // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the ofb.
      // (Ensures that recovery won't accidentally reallocate from the ofb.)
      checkpoint_.index_metadata.ofb_count =
        overflow_buckets_allocator_[resize_info_.version].count();
      // Write index meta data on disk
      if(WriteIndexMetadata() != Status::Ok) {
        checkpoint_.failed = true;
      }
      if(checkpoint_.index_persistence_callback) {
        // Notify the host that the index checkpoint has completed.
        checkpoint_.index_persistence_callback(Status::Ok);
      }
      break;
    case Phase::IN_PROGRESS: {
      assert(next_state.action != Action::CheckpointIndex);
      // PREPARE -> IN_PROGRESS
      // Do nothing
      break;
    }
    case Phase::WAIT_PENDING:
      assert(next_state.action != Action::CheckpointIndex);
      // IN_PROGRESS -> WAIT_PENDING
      // Do nothing
      break;
    case Phase::WAIT_FLUSH:
      assert(next_state.action != Action::CheckpointIndex);
      // WAIT_PENDING -> WAIT_FLUSH
      if(fold_over_snapshot) {
        // Move read-only to tail
        Address tail_address = hlog.ShiftReadOnlyToTail();
        // Get final address for CPR
        checkpoint_.log_metadata.final_address = tail_address;
      } else {
        Address tail_address = hlog.GetTailAddress();
        // Get final address for CPR
        checkpoint_.log_metadata.final_address = tail_address;
        checkpoint_.snapshot_file = disk.NewFile(disk.relative_cpr_checkpoint_path(
                                      checkpoint_.hybrid_log_token) + "snapshot.dat");
        if(checkpoint_.snapshot_file.Open(&disk.handler()) != Status::Ok) {
          checkpoint_.failed = true;
        }
        // Flush the log to a snapshot.
        hlog.AsyncFlushPagesToFile(checkpoint_.log_metadata.flushed_address.page(),
                                   checkpoint_.log_metadata.final_address, checkpoint_.snapshot_file,
                                   checkpoint_.flush_pending);
      }
      // Write CPR meta data file
      if(WriteCprMetadata() != Status::Ok) {
        checkpoint_.failed = true;
      }
      break;
    case Phase::PERSISTENCE_CALLBACK:
      assert(next_state.action != Action::CheckpointIndex);
      // WAIT_FLUSH -> PERSISTENCE_CALLBACK
      break;
    case Phase::REST:
      // PERSISTENCE_CALLBACK -> REST or INDEX_CHKPT -> REST
      if(next_state.action != Action::CheckpointIndex) {
        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
        // another checkpoint can be started.)
        checkpoint_.CheckpointDone();
        // Free checkpoint locks!
        checkpoint_locks_.Free();
        // Checkpoint is done--no more work for threads to do.
        system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      } else {
        // Get an overestimate for the ofb's tail, after we've finished fuzzy-checkpointing the
        // ofb. (Ensures that recovery won't accidentally reallocate from the ofb.)
        checkpoint_.index_metadata.ofb_count =
          overflow_buckets_allocator_[resize_info_.version].count();
        // Write index meta data on disk
        if(WriteIndexMetadata() != Status::Ok) {
          checkpoint_.failed = true;
        }
        auto index_persistence_callback = checkpoint_.index_persistence_callback;
        // The checkpoint is done; we can reset the contexts now. (Have to reset contexts before
        // another checkpoint can be started.)
        checkpoint_.CheckpointDone();
        // Checkpoint is done--no more work for threads to do.
        system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
        if(index_persistence_callback) {
          // Notify the host that the index checkpoint has completed.
          index_persistence_callback(Status::Ok);
        }
      }
#ifdef _TIMER
      checkpoint_end = std::chrono::high_resolution_clock::now();
      printf("Checkpoint done in %.6f seconds...\n",
             std::chrono::duration<double>(checkpoint_end - checkpoint_start).count());
#endif
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;
  case Action::GC:
    switch(next_state.phase) {
    case Phase::GC_IO_PENDING:
      // This case is handled directly inside ShiftBeginAddress().
      assert(false);
      break;
    case Phase::GC_IN_PROGRESS:
      // GC_IO_PENDING -> GC_IN_PROGRESS
      // Tell the disk to truncate the log.
      hlog.Truncate(gc_.truncate_callback);
      break;
    case Phase::REST:
      // GC_IN_PROGRESS -> REST
      // GC is done--no more work for threads to do.
      if(gc_.complete_callback) {
        gc_.complete_callback();
      }
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;
  case Action::GrowIndex:
    switch(next_state.phase) {
    case Phase::GROW_PREPARE:
      // This case is handled directly inside GrowIndex().
      assert(false);
      break;
    case Phase::GROW_IN_PROGRESS:
      // Swap hash table versions so that all threads will use the new version after populating it.
      resize_info_.version = grow_.new_version;
      break;
    case Phase::REST:
      if(grow_.callback) {
        grow_.callback(state_[grow_.new_version].size());
      }
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      // not reached
      assert(false);
      break;
    }
    break;
  case Action::ParserShift:
    switch(next_state.phase) {
    case Phase::PS_PENDING:
      // This case is handled directly inside ApplyParserShift().
      assert(false);
      break;
    case Phase::REST:
      // When finish parsing shifting, apply changes to backup meta.
      // So as to keep the invariant that two metas are identical.
      ParserStateApplyActions(parser_states[1 - system_parser_no_],
                              ps_.actions);
      // Finish parser shift, make the callback and send the safe
      // registration boundary.
      ps_.callback(hlog.GetTailAddress().control());
      // Clear the parser shift utility.
      ps_.actions.clear();
      ps_.callback = nullptr;
      // Set system state back to normal.
      system_state_.store(SystemState{ Action::None, Phase::REST, next_state.version });
      break;
    default:
      assert(false);
      break;
    }
    break;
  default:
    // not reached
    assert(false);
    break;
  }
  return true;
}

template <class D, class A>
void FishStore<D, A>::MarkAllPendingRequests() {
  uint32_t table_version = resize_info_.version;
  uint64_t table_size = state_[table_version].size();

  // In FishStore, RETRY_LATER may not happen.
  /*
  for(const IAsyncContext* ctxt : thread_ctx().retry_requests) {
    const pending_context_t* context = static_cast<const pending_context_t*>(ctxt);
    // We will succeed, since no other thread can currently advance the entry's version, since this
    // thread hasn't acked "PENDING" phase completion yet.
    bool result = checkpoint_locks_.get_lock(context->key().GetHash()).try_lock_old();
    assert(result);
  }
  */
  for(const auto& pending_io : thread_ctx().pending_ios) {
    // We will succeed, since no other thread can currently advance the entry's version, since this
    // thread hasn't acked "PENDING" phase completion yet.
    bool result = checkpoint_locks_.get_lock(pending_io.second).try_lock_old();
    assert(result);
  }
}

template <class D, class A>
void FishStore<D, A>::HandleSpecialPhases() {
  SystemState final_state = system_state_.load();
  if(final_state.phase == Phase::REST) {
    // Nothing to do; just reset thread context.
    thread_ctx().phase = Phase::REST;
    thread_ctx().version = final_state.version;
    return;
  }
  SystemState previous_state{ final_state.action, thread_ctx().phase, thread_ctx().version };
  do {
    // Identify the transition (currentState -> nextState)
    SystemState current_state = (previous_state == final_state) ? final_state :
                                previous_state.GetNextState();
    switch(current_state.action) {
    case Action::CheckpointFull:
    case Action::CheckpointIndex:
    case Action::CheckpointHybridLog:
      switch(current_state.phase) {
      case Phase::PREP_INDEX_CHKPT:
        assert(current_state.action != Action::CheckpointHybridLog);
        // Both from REST -> PREP_INDEX_CHKPT and PREP_INDEX_CHKPT -> PREP_INDEX_CHKPT
        if(previous_state.phase == Phase::REST) {
          // Thread ack that we're performing a checkpoint.
          if(epoch_.FinishThreadPhase(Phase::PREP_INDEX_CHKPT)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::INDEX_CHKPT: {
        assert(current_state.action != Action::CheckpointHybridLog);
        // Both from PREP_INDEX_CHKPT -> INDEX_CHKPT and INDEX_CHKPT -> INDEX_CHKPT
        Status result = CheckpointFuzzyIndexComplete();
        if(result != Status::Pending && result != Status::Ok) {
          checkpoint_.failed = true;
        }
        if(result != Status::Pending) {
          if(current_state.action == Action::CheckpointIndex) {
            // This thread is done now.
            thread_ctx().phase = Phase::REST;
            // Thread ack that it is done.
            if(epoch_.FinishThreadPhase(Phase::INDEX_CHKPT)) {
              GlobalMoveToNextState(current_state);
            }
          } else {
            // Index checkpoint is done; move on to PREPARE phase.
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      }
      case Phase::PREPARE:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle (INDEX_CHKPT -> PREPARE or REST -> PREPARE) and PREPARE -> PREPARE
        if(previous_state.phase != Phase::PREPARE) {
          // mark pending requests
          MarkAllPendingRequests();
          // keep a count of number of threads
          ++checkpoint_.log_metadata.num_threads;
          // set the thread index
          checkpoint_.log_metadata.guids[Thread::id()] = thread_ctx().guid;
          // Thread ack that it has finished marking its pending requests.
          if(epoch_.FinishThreadPhase(Phase::PREPARE)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::IN_PROGRESS:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle PREPARE -> IN_PROGRESS and IN_PROGRESS -> IN_PROGRESS
        if(previous_state.phase == Phase::PREPARE) {
          assert(prev_thread_ctx().retry_requests.empty());
          assert(prev_thread_ctx().pending_ios.empty());
          assert(prev_thread_ctx().io_responses.empty());

          // Get a new thread context; keep track of the old one as "previous."
          thread_contexts_[Thread::id()].swap();
          // initialize a new local context
          thread_ctx().Initialize(Phase::IN_PROGRESS, current_state.version,
                                  prev_thread_ctx().guid, prev_thread_ctx().serial_num, prev_thread_ctx().offset);
          // Thread ack that it has swapped contexts.
          if(epoch_.FinishThreadPhase(Phase::IN_PROGRESS)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      case Phase::WAIT_PENDING:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle IN_PROGRESS -> WAIT_PENDING and WAIT_PENDING -> WAIT_PENDING
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Thread ack that it has completed its pending I/Os.
            if(epoch_.FinishThreadPhase(Phase::WAIT_PENDING)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::WAIT_FLUSH:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle WAIT_PENDING -> WAIT_FLUSH and WAIT_FLUSH -> WAIT_FLUSH
        if(!epoch_.HasThreadFinishedPhase(Phase::WAIT_FLUSH)) {
          bool flushed;
          if(fold_over_snapshot) {
            flushed = hlog.flushed_until_address.load() >= checkpoint_.log_metadata.final_address;
          } else {
            flushed = checkpoint_.flush_pending.load() == 0;
          }
          if(flushed) {
            // write context info
            WriteCprContext();
            // Thread ack that it has written its CPU context.
            if(epoch_.FinishThreadPhase(Phase::WAIT_FLUSH)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::PERSISTENCE_CALLBACK:
        assert(current_state.action != Action::CheckpointIndex);
        // Handle WAIT_FLUSH -> PERSISTENCE_CALLBACK and PERSISTENCE_CALLBACK -> PERSISTENCE_CALLBACK
        if(previous_state.phase == Phase::WAIT_FLUSH) {
          // Persistence callback
          if(checkpoint_.hybrid_log_persistence_callback) {
            checkpoint_.hybrid_log_persistence_callback(Status::Ok, prev_thread_ctx().serial_num,
                prev_thread_ctx().offset);
          }
          // Thread has finished checkpointing.
          thread_ctx().phase = Phase::REST;
          // Thread ack that it has finished checkpointing.
          if(epoch_.FinishThreadPhase(Phase::PERSISTENCE_CALLBACK)) {
            GlobalMoveToNextState(current_state);
          }
        }
        break;
      default:
        // nothing to do.
        break;
      }
      break;
    case Action::GC:
      switch(current_state.phase) {
      case Phase::GC_IO_PENDING:
        // Handle REST -> GC_IO_PENDING and GC_IO_PENDING -> GC_IO_PENDING.
        if(previous_state.phase == Phase::REST) {
          assert(prev_thread_ctx().retry_requests.empty());
          assert(prev_thread_ctx().pending_ios.empty());
          assert(prev_thread_ctx().io_responses.empty());
          // Get a new thread context; keep track of the old one as "previous."
          thread_contexts_[Thread::id()].swap();
          // initialize a new local context
          thread_ctx().Initialize(Phase::GC_IO_PENDING, current_state.version,
                                  prev_thread_ctx().guid, prev_thread_ctx().serial_num, prev_thread_ctx().offset);
        }

        // See if the old thread context has completed its pending I/Os.
        if(!epoch_.HasThreadFinishedPhase(Phase::GC_IO_PENDING)) {
          if(prev_thread_ctx().pending_ios.empty() &&
              prev_thread_ctx().retry_requests.empty()) {
            // Thread ack that it has completed its pending I/Os.
            if(epoch_.FinishThreadPhase(Phase::GC_IO_PENDING)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      case Phase::GC_IN_PROGRESS:
        // Handle GC_IO_PENDING -> GC_IN_PROGRESS and GC_IN_PROGRESS -> GC_IN_PROGRESS.
        if(!epoch_.HasThreadFinishedPhase(Phase::GC_IN_PROGRESS)) {
          if(!CleanHashTableBuckets()) {
            // No more buckets for this thread to clean; thread has finished GC.
            thread_ctx().phase = Phase::REST;
            // Thread ack that it has finished GC.
            if(epoch_.FinishThreadPhase(Phase::GC_IN_PROGRESS)) {
              GlobalMoveToNextState(current_state);
            }
          }
        }
        break;
      default:
        assert(false); // not reached
        break;
      }
      break;
    case Action::GrowIndex:
      switch(current_state.phase) {
      case Phase::GROW_PREPARE:
        if(previous_state.phase == Phase::REST) {
          // Thread ack that we're going to grow the hash table.
          if(epoch_.FinishThreadPhase(Phase::GROW_PREPARE)) {
            GlobalMoveToNextState(current_state);
          }
        } else {
          // Wait for all other threads to finish their outstanding (synchronous) hash table
          // operations.
          std::this_thread::yield();
        }
        break;
      case Phase::GROW_IN_PROGRESS:
        SplitHashTableBuckets();
        break;
      default: break;
      }
      break;
    case Action::ParserShift:
      switch(current_state.phase) {
      case Phase::PS_PENDING:
        /// Shifting to the new parser;
        /// As long as the parser we observe in this thread is diffrent than
        /// that specified by the system
        if(previous_state.phase == Phase::REST) {
          // Rebuild parser when observing a PS_PENDING system state and the
          // current thread is in REST.
          assert(parser_ctx().parser_no != system_parser_no_);
          parser_ctx().RebuildParser(
            system_parser_no_,
            parser_states[system_parser_no_].main_parser_fields);
          thread_ctx().phase = Phase::PS_PENDING;
        }
        // When everyone has finish shifting parser, move to next global
        // state.
        if(epoch_.FinishThreadPhase(Phase::PS_PENDING)) {
          GlobalMoveToNextState(current_state);
        }
        break;
      default:
        break;
      }
      break;
    default: break;
    }
    thread_ctx().phase = current_state.phase;
    thread_ctx().version = current_state.version;
    previous_state = current_state;
  } while(previous_state != final_state);
}

template <class D, class A>
bool FishStore<D, A>::Checkpoint(const std::function<void(Status)>& index_persistence_callback,
                                 const std::function<void(Status, uint64_t, uint32_t)>& hybrid_log_persistence_callback,
                                 Guid& token) {
#ifdef _TIMER
  checkpoint_start = std::chrono::high_resolution_clock::now();
#endif

  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointFull, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateIndexCheckpointDirectory(token);
  disk.CreateCprCheckpointDirectory(token);

  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeCheckpoint(token, desired.version, state_[resize_info_.version].size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), true,
                                     hlog.flushed_until_address.load(),
                                     index_persistence_callback,
                                     hybrid_log_persistence_callback);
  } else {
    checkpoint_.InitializeCheckpoint(token, desired.version, state_[resize_info_.version].size(),
                                     hlog.begin_address.load(),  hlog.GetTailAddress(), false,
                                     Address::kInvalidAddress, index_persistence_callback,
                                     hybrid_log_persistence_callback);

  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class D, class A>
bool FishStore<D, A>::CheckpointIndex(const std::function<void(Status)>& index_persistence_callback,
                                      Guid& token) {
#ifdef _TIMER
  checkpoint_start = std::chrono::high_resolution_clock::now();
#endif

  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointIndex, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateIndexCheckpointDirectory(token);
  checkpoint_.InitializeIndexCheckpoint(token, desired.version,
                                        state_[resize_info_.version].size(),
                                        hlog.begin_address.load(), hlog.GetTailAddress(),
                                        index_persistence_callback);
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class D, class A>
bool FishStore<D, A>::CheckpointHybridLog(const std::function<void(Status, uint64_t, uint32_t)>&
    hybrid_log_persistence_callback, Guid& token) {
#ifdef _TIMER
  checkpoint_start = std::chrono::high_resolution_clock::now();
#endif
  // Only one thread can initiate a checkpoint at a time.
  SystemState expected{ Action::None, Phase::REST, system_state_.load().version };
  SystemState desired{ Action::CheckpointHybridLog, Phase::REST, expected.version };
  if(!system_state_.compare_exchange_strong(expected, desired)) {
    // Can't start a new checkpoint while a checkpoint or recovery is already in progress.
    return false;
  }
  // We are going to start a checkpoint.
  epoch_.ResetPhaseFinished();
  // Initialize all contexts
  token = Guid::Create();
  disk.CreateCprCheckpointDirectory(token);

  FILE* naming_file = fopen(disk.naming_checkpoint_path(token).c_str(), "w");
  fprintf(naming_file, "%zu\n", field_names.size());
  for(auto& field: field_names) {
    fprintf(naming_file, "%s\n", field.c_str());
  }
  fprintf(naming_file, "%zu\n", libs.size());
  for(auto& lib : libs) {
    fprintf(naming_file, "%s\n", lib.path.string().c_str());
  }
  fprintf(naming_file, "%zu\n", general_psf_map.size());
  for(const GeneralPSF<A>& psf : general_psf_map) {
    fprintf(naming_file, "%zu", psf.fields.size());
    for(auto& field : psf.fields) {
      fprintf(naming_file, " %u",field);
    }
    fprintf(naming_file, " %zd %s\n", psf.lib_id,
            psf.func_name.c_str());
  }
  fprintf(naming_file, "%zu\n", inline_psf_map.size());
  for(const InlinePSF<A>& psf : inline_psf_map) {
    fprintf(naming_file, "%zu", psf.fields.size());
    for(auto& field : psf.fields) {
      fprintf(naming_file, " %u",field);
    }
    fprintf(naming_file, " %zd %s\n", psf.lib_id,
            psf.func_name.c_str());
  }
  const ParserState& parser_state = parser_states[system_parser_no_.load()];
  fprintf(naming_file, "%zu\n", parser_state.ptr_general_psf.size());
  for(const auto& psf_id : parser_state.ptr_general_psf) {
    fprintf(naming_file, "%u\n", psf_id);
  }
  fprintf(naming_file, "%zu\n", parser_state.ptr_inline_psf.size());
  for(const auto& psf_id : parser_state.ptr_inline_psf) {
    fprintf(naming_file, "%u\n", psf_id);
  }
  fclose(naming_file);

  // Obtain tail address for fuzzy index checkpoint
  if(!fold_over_snapshot) {
    checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, true,
        hlog.flushed_until_address.load(), hybrid_log_persistence_callback);
  } else {
    checkpoint_.InitializeHybridLogCheckpoint(token, desired.version, false,
        Address::kInvalidAddress, hybrid_log_persistence_callback);
  }
  InitializeCheckpointLocks();
  // Let other threads know that the checkpoint has started.
  system_state_.store(desired.GetNextState());
  return true;
}

template <class D, class A>
Status FishStore<D, A>::Recover(const Guid& index_token, const Guid& hybrid_log_token,
                                uint32_t& version,
                                std::vector<Guid>& session_ids) {
  version = 0;
  session_ids.clear();
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::Recover, Phase::REST, expected.version })) {
    return Status::Aborted;
  }
  checkpoint_.InitializeRecover(index_token, hybrid_log_token);
  Status status;
#define BREAK_NOT_OK(s) \
    status = (s); \
    if (status != Status::Ok) break

  do {
    // Index and log metadata.
    BREAK_NOT_OK(ReadIndexMetadata(index_token));
    BREAK_NOT_OK(ReadCprMetadata(hybrid_log_token));
    if(checkpoint_.index_metadata.version != checkpoint_.log_metadata.version) {
      // Index and hybrid-log checkpoints should have the same version.
      status = Status::Corruption;
      break;
    }

    system_state_.store(SystemState{ Action::Recover, Phase::REST,
                                     checkpoint_.log_metadata.version + 1 });

    BREAK_NOT_OK(ReadCprContexts(hybrid_log_token, checkpoint_.log_metadata.guids));
    // The index itself (including overflow buckets).
    BREAK_NOT_OK(RecoverFuzzyIndex());
    BREAK_NOT_OK(RecoverFuzzyIndexComplete(true));
    // Any changes made to the log while the index was being fuzzy-checkpointed.
    if(fold_over_snapshot) {
      BREAK_NOT_OK(RecoverHybridLog());
    } else {
      BREAK_NOT_OK(RecoverHybridLogFromSnapshotFile());
    }
    BREAK_NOT_OK(RestoreHybridLog());
  } while(false);

  std::ifstream naming_file(disk.naming_checkpoint_path(hybrid_log_token));
  size_t n_fields;
  naming_file >> n_fields;
  for(size_t i = 0; i < n_fields; ++i) {
    std::string field_name;
    naming_file >> field_name;
    field_names.push_back(field_name);
    field_lookup_map.emplace(std::make_pair(field_name, static_cast<uint16_t>(i)));
  }

  size_t n_libs;
  naming_file >> n_libs;
  for(size_t i = 0; i < n_libs; ++i) {
    std::string path;
    naming_file >> path;
    LibraryHandle lib;
    lib.path = std::experimental::filesystem::absolute(path);
#ifdef _WIN32
    lib.handle = LoadLibrary(lib.path.string().c_str());
#else
    lib.handle = dlopen(lib.path.string().c_str(), RTLD_LAZY);
#endif
    assert(lib.handle);
    libs.emplace_back(lib);
  }

  size_t n_general_psf;
  naming_file >> n_general_psf;
  for(size_t i = 0; i < n_general_psf; ++i) {
    int64_t lib_id;
    size_t field_cnt;
    std::string func_name;
    GeneralPSF<A> psf;
    naming_file >> field_cnt;
    for(size_t i = 0; i < field_cnt; ++i) {
      uint16_t field_id;
      naming_file >> field_id;
      psf.fields.push_back(field_id);
    }
    naming_file >> lib_id >> func_name;
    psf.lib_id = lib_id;
    psf.func_name = func_name;
    if (psf.lib_id != -1) {
#ifdef _WIN32
      psf.eval_ = (general_psf_t<A>)GetProcAddress(libs[lib_id].handle, func_name.c_str());
#else
      psf.eval_ = (general_psf_t<A>)dlsym(libs[lib_id].handle, func_name.c_str());
#endif
    } else {
      psf.eval_ = projection<A>;
    }
    assert(psf.eval_);
    general_psf_map.push_back(psf);
  }

  size_t n_inline_psf;
  naming_file >> n_inline_psf;
  for(size_t i = 0; i < n_inline_psf; ++i) {
    int64_t lib_id;
    size_t field_cnt;
    std::string func_name;
    InlinePSF<A> psf;
    naming_file >> field_cnt;
    for(size_t i = 0; i < field_cnt; ++i) {
      uint16_t field_id;
      naming_file >> field_id;
      psf.fields.push_back(field_id);
    }
    naming_file >> lib_id >> func_name;
    psf.lib_id = lib_id;
    psf.func_name = func_name;
    if (psf.lib_id != -1) {
#ifdef _WIN32
      psf.eval_ = (inline_psf_t<A>)GetProcAddress(libs[lib_id].handle, func_name.c_str());
#else
      psf.eval_ = (inline_psf_t<A>)dlsym(libs[lib_id].handle, func_name.c_str());
#endif
    }
    assert(psf.eval_);
    inline_psf_map.push_back(psf);
  }

  ParserState parser_state;
  size_t n_reg_general;
  naming_file >> n_reg_general;
  for(size_t i = 0; i < n_reg_general; ++i) {
    uint16_t general_psf_id;
    naming_file >> general_psf_id;
    parser_state.ptr_general_psf.emplace(general_psf_id);
  }
  size_t n_reg_inline;
  naming_file >> n_reg_inline;
  for(size_t i = 0; i < n_reg_inline; ++i) {
    uint32_t inline_psf_id;
    naming_file >> inline_psf_id;
    parser_state.ptr_inline_psf.emplace(inline_psf_id);
  }

  // Construct parser essentials
  std::unordered_set<uint16_t> field_ids;
  for(auto psf_id : parser_state.ptr_general_psf) {
    for(auto field_id : general_psf_map[psf_id].fields) {
      field_ids.insert(field_id);
    }
  }

  for(auto psf_id : parser_state.ptr_inline_psf) {
    for(auto field_id : inline_psf_map[psf_id].fields) {
      field_ids.insert(field_id);
    }
  }

  parser_state.main_parser_fields.clear();
  parser_state.main_parser_field_ids.clear();
  for(auto field_id : field_ids) {
    parser_state.main_parser_fields.push_back(field_names[field_id]);
    parser_state.main_parser_field_ids.push_back(field_id);
  }

  parser_states[0] = parser_state;
  parser_states[1] = parser_state;
  system_parser_no_ = 0;


  naming_file.close();

  if(status == Status::Ok) {
    for(const auto& token : checkpoint_.continue_tokens) {
      session_ids.push_back(token.first);
    }
    version = checkpoint_.log_metadata.version;
  }

  checkpoint_.RecoverDone();
  system_state_.store(SystemState{ Action::None, Phase::REST,
                                   checkpoint_.log_metadata.version + 1 });
  return status;
#undef BREAK_NOT_OK
}

template <class D, class A>
bool FishStore<D, A>::ShiftBeginAddress(Address address,
                                        GcState::truncate_callback_t truncate_callback,
                                        GcState::complete_callback_t complete_callback) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GC, Phase::REST, expected.version })) {
    // Can't start a GC while an action is already in progress.
    return false;
  }
  hlog.begin_address.store(address);
  // Each active thread will notify the epoch when all pending I/Os have completed.
  epoch_.ResetPhaseFinished();
  uint64_t num_chunks = std::max(state_[resize_info_.version].size() / kGcHashTableChunkSize,
                                 (uint64_t)1);
  gc_.Initialize(truncate_callback, complete_callback, num_chunks);
  // Let other threads know to complete their pending I/Os, so that the log can be truncated.
  system_state_.store(SystemState{ Action::GC, Phase::GC_IO_PENDING, expected.version });
  return true;
}

template <class D, class A>
bool FishStore<D, A>::GrowIndex(GrowState::callback_t caller_callback) {
  SystemState expected = SystemState{ Action::None, Phase::REST, system_state_.load().version };
  if(!system_state_.compare_exchange_strong(expected,
      SystemState{ Action::GrowIndex, Phase::REST, expected.version })) {
    // An action is already in progress.
    return false;
  }
  epoch_.ResetPhaseFinished();
  uint8_t current_version = resize_info_.version;
  assert(current_version == 0 || current_version == 1);
  uint8_t next_version = 1 - current_version;
  uint64_t num_chunks = std::max(state_[current_version].size() / kGrowHashTableChunkSize,
                                 (uint64_t)1);
  grow_.Initialize(caller_callback, current_version, num_chunks);
  // Initialize the next version of our hash table to be twice the size of the current version.
  state_[next_version].Initialize(state_[current_version].size() * 2, disk.log().alignment());
  overflow_buckets_allocator_[next_version].Initialize(disk.log().alignment(), epoch_);

  SystemState next = SystemState{ Action::GrowIndex, Phase::GROW_PREPARE, expected.version };
  system_state_.store(next);

  // Let this thread know it should be growing the index.
  Refresh();
  return true;
}

template <class D, class A>
uint16_t FishStore<D, A>::AcquireFieldID(const std::string& field) {
  // Naming service: register a field ID if it has not been seen, or return
  // its field ID that already registered in the naming service.
  std::lock_guard<std::mutex> lk(mutex);
  uint16_t field_id;
  auto it = field_lookup_map.find(field);
  if(it != field_lookup_map.end()) {
    field_id = it->second;
  } else if(std::is_base_of<adaptor::JsonAdaptor, A>::value) {
    field_id = static_cast<uint16_t>(field_names.size());
    field_names.push_back(field);
    field_lookup_map[field] = field_id;
  } else {
    printf("Field not reconginzed!!!\n");
    return -1;
  }
  return field_id;
}

template <class D, class A>
size_t FishStore<D, A>::LoadPSFLibrary(const std::string& lib_path) {
  std::lock_guard<std::mutex> lk(mutex);
  LibraryHandle lib;
  lib.path = std::experimental::filesystem::absolute(lib_path);
#ifdef _WIN32
  lib.handle = LoadLibrary(lib.path.string().c_str());
#else
  lib.handle = dlopen(lib.path.string().c_str(), RTLD_LAZY);
#endif
  if(!lib.handle) {
    printf("Cannnot load library at %s...\n", lib.path.string().c_str());
    return -1;
  }
  printf("Successfully load library at %s..\n", lib.path.string().c_str());
  auto res = libs.size();
  libs.emplace_back(lib);
  return res;
}

template <class D, class A>
uint32_t FishStore<D, A>::MakeInlinePSF(const std::vector<std::string>& fields,
                                        size_t lib_id, std::string func_name) {
  std::lock_guard<std::mutex> lk(mutex);
  InlinePSF<A> psf;
  if(lib_id >= libs.size()) {
    printf("Invalid library handle!!\n");
    return -1;
  }
#ifdef _WIN32
  auto func_ptr = (inline_psf_t<A>)GetProcAddress(libs[lib_id].handle, func_name.c_str());
#else
  auto func_ptr = (inline_psf_t<A>)dlsym(libs[lib_id].handle, func_name.c_str());
#endif
  if(!func_ptr) {
    printf("Function with name %s does not exist in library %s...\n",
           func_name.c_str(), libs[lib_id].path.string().c_str());
    return -1;
  }
  psf.lib_id = lib_id;
  psf.func_name = func_name;
  psf.eval_ = func_ptr;
  psf.fields.clear();
  for(auto field : fields) {
    uint16_t field_id;
    auto it = field_lookup_map.find(field);
    if(it != field_lookup_map.end()) {
      field_id = it->second;
    } else if(std::is_base_of<adaptor::JsonAdaptor, A>::value) {
      // You may register a field on the fly during making predicates in json.
      field_id = static_cast<uint16_t>(field_names.size());
      field_names.push_back(field);
      field_lookup_map[field] = field_id;
    } else {
      printf("Predicate registered failed! Field not reconginze!!\n");
      return -1;
    }
    psf.fields.push_back(field_id);
  }

  uint64_t inline_psf_id = inline_psf_map.size();
  if(inline_psf_id > (1LL << 32) - 1) {
    printf("Maximum # inline PSF exceeded...\n");
    return -1;
  }
  inline_psf_map.push_back(psf);
  return static_cast<uint32_t>(inline_psf_id);
}

template <class D, class A>
uint16_t FishStore<D, A>::MakeProjection(const std::string& field) {
  GeneralPSF<A> psf;
  psf.lib_id = -1;
  psf.func_name = "proj";
  psf.eval_ = projection<A>;
  psf.fields.clear();

  uint16_t field_id;
  auto it = field_lookup_map.find(field);
  if (it != field_lookup_map.end()) {
	  field_id = it->second;
  } else if (std::is_base_of<adaptor::JsonAdaptor, A>::value) {
	  // You may register a field on the fly during making predicates in json.
	  field_id = static_cast<uint16_t>(field_names.size());
	  field_names.push_back(field);
	  field_lookup_map[field] = field_id;
  } else {
	  printf("Predicate registered failed! Field not reconginze!!\n");
	  return -1;
  }
  psf.fields.push_back(field_id);

  uint64_t general_psf_id = general_psf_map.size();
  if (general_psf_id > (1LL << 14) - 1) {
    printf("Maximum # general PSF exceeded...\n");
    return -1;
  }
  general_psf_map.push_back(psf);
  return static_cast<uint16_t>(general_psf_id);
}

template <class D, class A>
uint16_t FishStore<D, A>::MakeGeneralPSF(const std::vector<std::string>& fields,
    size_t lib_id, std::string func_name) {

  std::lock_guard<std::mutex> lk(mutex);
  GeneralPSF<A> psf;
  if(lib_id >= libs.size()) {
    printf("Invalid library handle!!\n");
    return -1;
  }
#ifdef _WIN32
  auto func_ptr = (general_psf_t<A>)GetProcAddress(libs[lib_id].handle, func_name.c_str());
#else
  auto func_ptr = (general_psf_t<A>)dlsym(libs[lib_id].handle, func_name.c_str());
#endif
  if(!func_ptr) {
    printf("Function with name %s does not exist in library %s...\n",
           func_name.c_str(), libs[lib_id].path.string().c_str());
    return -1;
  }
  psf.lib_id = lib_id;
  psf.func_name = func_name;
  psf.eval_ = func_ptr;
  psf.fields.clear();
  for(auto field : fields) {
    uint16_t field_id;
    auto it = field_lookup_map.find(field);
    if(it != field_lookup_map.end()) {
      field_id = it->second;
    } else if(std::is_base_of<adaptor::JsonAdaptor, A>::value) {
      // You may register a field on the fly during making predicates in json.
      field_id = static_cast<uint16_t>(field_names.size());
      field_names.push_back(field);
      field_lookup_map[field] = field_id;
    } else {
      printf("Predicate registered failed! Field not reconginze!!\n");
      return -1;
    }
    psf.fields.push_back(field_id);
  }

  uint64_t general_psf_id = general_psf_map.size();
  if(general_psf_id > (1LL << 14) - 1) {
    printf("Maximum # general PSF exceeded...\n");
    return -1;
  }
  general_psf_map.push_back(psf);
  return static_cast<uint16_t>(general_psf_id);
}

template <class D, class A>
void FishStore<D, A>::RegisterHeader(const std::vector<std::string>& header) {
  // Naming service: register the header of a CSV data. Thus, we can assign the
  // field ID for each field name.
  if(std::is_base_of<adaptor::JsonAdaptor, A>::value) {
    fprintf(stderr, "Register Header does not work with Json.\n");
    return;
  }
  std::lock_guard<std::mutex> lk(mutex);
  if(header.size() != field_names.size()) {
    fprintf(stderr, "Header size does not align with that of fishstore");
  } else {
    for(size_t i = 0; i < header.size(); ++i)
      field_lookup_map[header[i]] = static_cast<uint16_t>(i);
  }
}

template <class D, class A>
void FishStore<D, A>::ParserStateApplyActions(
  ParserState& state, const std::vector<ParserAction>& actions) {
  // Apply a list of parser actions to a global parser state. (the new one or
  // the backup) First apply changes to ptr sets.
  for(auto& action : actions) {
    switch(action.type) {
    case ParserActionType::REGISTER_INLINE_PSF:
      state.ptr_inline_psf.insert(action.id);
      break;
    case ParserActionType::REGISTER_GENERAL_PSF:
      assert(action.id <= UINT16_MAX);
      state.ptr_general_psf.insert(static_cast<uint16_t>(action.id));
      break;
    case ParserActionType::DEREGISTER_INLINE_PSF:
      state.ptr_inline_psf.erase(action.id);
      break;
    case ParserActionType::DEREGISTER_GENERAL_PSF:
      assert(action.id <= UINT16_MAX);
      state.ptr_general_psf.erase(static_cast<uint16_t>(action.id));
      break;
    }
  }
  // Construct parser essentials
  std::unordered_set<uint16_t> field_ids;
  for(auto psf_id : state.ptr_inline_psf) {
    for(auto field_id : inline_psf_map[psf_id].fields) {
      field_ids.insert(field_id);
    }
  }

  for(auto psf_id : state.ptr_general_psf) {
    for(auto field_id : general_psf_map[psf_id].fields) {
      field_ids.insert(field_id);
    }
  }

  state.main_parser_fields.clear();
  state.main_parser_field_ids.clear();
  for(auto field_id : field_ids) {
    state.main_parser_fields.push_back(field_names[field_id]);
    state.main_parser_field_ids.push_back(field_id);
  }
}

template <class D, class A>
uint64_t FishStore<D, A>::ApplyParserShift(
  const std::vector<ParserAction>& actions,
  const std::function<void(uint64_t)>& callback) {
  SystemState expected =
    SystemState{Action::None, Phase::REST, system_state_.load().version};
  if(!system_state_.compare_exchange_strong(
        expected,
        SystemState{Action::ParserShift, Phase::REST, expected.version})) {
    // If the system is not in the right state, return a invalid address to
    // inform user about failure.
    return Address::kInvalidAddress;
  }
  // set all sessions in the epoch to be not complete ps pending yet.
  ps_.actions = actions;
  ps_.callback = callback;
  epoch_.ResetPhaseFinished();

  // Apply the parser change to the backup parser state, then change system
  // parser state to the original backup.
  int8_t next_parser_no = 1 - system_parser_no_;
  ParserStateApplyActions(parser_states[next_parser_no], actions);
  system_parser_no_ = next_parser_no;

  // safe unregister address is the tail address before the first session move
  // out of the old parser state.
  uint64_t safe_unregister_address = hlog.GetTailAddress().control();
  // Ready to start shifting, change system state to PS_PENDING.
  system_state_.store(
    SystemState{Action::ParserShift, Phase::PS_PENDING, expected.version});

  // Force current thread to refresh.
  Refresh();
  return safe_unregister_address;
}

template <class D, class A>
void FishStore<D, A>::PrintRegistration() {
  printf("Loaded Libraries:\n");
  for(size_t i = 0; i < libs.size(); ++i) {
    printf("%zu %s\n", i, libs[i].path.string().c_str());
  }
  ParserState state = parser_states[system_parser_no_.load()];
  printf("Registered inline PSFs:\n");
  for(const auto& psf_id : state.ptr_inline_psf) {
    const InlinePSF<A>& psf = inline_psf_map[psf_id];
    printf("%u %zd %s ->", psf_id, psf.lib_id, psf.func_name.c_str());
    for(const auto& field: psf.fields) {
      printf(" %s", field_names[field].c_str());
    }
    printf("\n");
  }
  printf("Registered general PSFs:\n");
  for(const auto& psf_id : state.ptr_general_psf) {
    const GeneralPSF<A>& psf = general_psf_map[psf_id];
    printf("%u %zd %s ->", psf_id, psf.lib_id, psf.func_name.c_str());
    for(const auto& field: psf.fields) {
      printf(" %s", field_names[field].c_str());
    }
    printf("\n");
  }
}

}
} // namespace fishstore::core
