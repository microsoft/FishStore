// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <deque>
#include <unordered_map>
#include <string>
#include "address.h"
#include "guid.h"
#include "hash_bucket.h"
#include "native_buffer_pool.h"
#include "record.h"
#include "state_transitions.h"
#include "thread.h"
#include "psf.h"

namespace fishstore {
namespace core {

/// Internal contexts, used by FishStore.

enum class OperationType : uint8_t {
  Read,
  Insert,
  Scan,
  FullScan,
  Delete
};

enum class OperationStatus : uint8_t {
  SUCCESS,
  NOT_FOUND,
  RETRY_NOW,
  RETRY_LATER,
  RECORD_ON_DISK,
  SCAN_IN_PROGRESS,
  SUCCESS_UNMARK,
  NOT_FOUND_UNMARK,
  CPR_SHIFT_DETECTED
};

struct KPTUtil {
  KPTUtil(uint64_t hash_, uint16_t general_psf_id_, uint32_t value_offset_, uint32_t value_size_)
    : hash(hash_),
      general_psf_id(general_psf_id_),
      value_offset(value_offset_),
      value_size(value_size_) {
  }

  KPTUtil(uint32_t inline_psf_id_, int32_t value_)
    : hash(Utility::GetHashCode(inline_psf_id_, value_)),
      inline_psf_id(inline_psf_id_),
      value(value_) {}

  KeyHash hash;
  // Two modes carry two different type of information.
  union {
    // Field-based key pointers.
    struct {
      uint64_t general_psf_id : 14;
      uint64_t value_offset : 25;
      uint64_t value_size : 25;
    };

    // Predicate-based key pointers.
    struct {
      uint32_t inline_psf_id;
      int32_t value;
    };
  };
};

/// Internal FishStore context.
class PendingContext : public IAsyncContext {
 protected:
  PendingContext(OperationType type_, IAsyncContext& caller_context_,
                 AsyncCallback caller_callback_)
    : type{ type_ }
    , caller_context{ &caller_context_ }
    , caller_callback{ caller_callback_ }
    , version{ UINT32_MAX }
    , phase{ Phase::INVALID }
    , result{ Status::Pending }
    , address{ Address::kInvalidAddress } {
  }

 public:
  /// The deep-copy constructor.
  PendingContext(const PendingContext& other, IAsyncContext* caller_context_)
    : type{ other.type }
    , caller_context{ caller_context_ }
    , caller_callback{ other.caller_callback }
    , version{ other.version }
    , phase{ other.phase }
    , result{ other.result }
    , address{ other.address }
    , start_addr{ other.start_addr }
    , end_addr{ other.end_addr } {
  }

 public:
  virtual KeyHash get_hash() const {
    assert(false);
    return KeyHash{ 0 };
  }

  /// Go async, for the first time.
  void go_async(Phase phase_, uint32_t version_, Address address_,
                uint64_t start = 0, uint64_t end = Address::kMaxAddress) {
    phase = phase_;
    version = version_;
    address = address_;
    start_addr = start;
    end_addr = end;
  }

  /// Go async, again.
  void continue_async(Address address_) {
    address = address_;
  }

  /// Caller context.
  IAsyncContext* caller_context;
  /// Caller callback.
  AsyncCallback caller_callback;
  /// Checkpoint version.
  uint32_t version;
  /// Checkpoint phase.
  Phase phase;
  /// Type of operation (Read, Upsert, RMW, etc.).
  OperationType type;
  /// Result of operation.
  Status result;
  /// Address of the record being read or modified.
  Address address;

  Address start_addr, end_addr;
};

/// FishStore's internal Read() context.

/// An internal Read() context that has gone async and lost its type information.
class AsyncPendingReadContext : public PendingContext {
 protected:
  AsyncPendingReadContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_)
    : PendingContext(OperationType::Read, caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  AsyncPendingReadContext(AsyncPendingReadContext& other, IAsyncContext* caller_context)
    : PendingContext(other, caller_context) {
  }
 public:
  virtual void Get(const void* rec) = 0;
  virtual bool check(const KeyPointer* key_pointer) = 0;
};

/// A synchronous Read() context preserves its type information.
template <class RC>
class PendingReadContext : public AsyncPendingReadContext {
 public:
  typedef RC read_context_t;
  typedef Record record_t;

  PendingReadContext(read_context_t& caller_context_, AsyncCallback caller_callback_)
    : AsyncPendingReadContext(caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  PendingReadContext(PendingReadContext& other, IAsyncContext* caller_context_)
    : AsyncPendingReadContext(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext::caller_context, context_copy);
  }
 private:
  inline const read_context_t& read_context() const {
    return *static_cast<const read_context_t*>(PendingContext::caller_context);
  }
  inline read_context_t& read_context() {
    return *static_cast<read_context_t*>(PendingContext::caller_context);
  }
 public:
  inline KeyHash get_hash() const final {
    return read_context().get_hash();
  }
  inline void Get(const void* rec) final {
    const record_t* record = reinterpret_cast<const record_t*>(rec);
    read_context().Get(record->payload(), record->payload_size());
  }
  inline bool check(const KeyPointer* key_pointer) final {
    return read_context().check(key_pointer);
  }
};

/// Insert context used in batch insertions.
class RecordInsertContext : public IAsyncContext {
 public:
  RecordInsertContext(const char* payload, uint32_t payload_size, uint32_t offset)
    : payload_(payload), payload_size_(payload_size), n_general_pts_(0), offset_(offset), optional_size_(0) {
    kpts_.clear();
  }

  RecordInsertContext(const char* payload, uint32_t payload_size,
                      const std::vector<KPTUtil>& kpts, uint16_t n_general_pts, uint32_t offset, uint32_t option_size)
    : payload_(payload), payload_size_(payload_size), kpts_(kpts), n_general_pts_(n_general_pts), optional_size_(option_size),
      offset_(offset) {
  }

  ~RecordInsertContext() {
    if(from_deep_copy()) free((void*)payload_);
  }

  inline const char* payload() const {
    return payload_;
  }

  inline uint32_t payload_size() const {
    return payload_size_;
  }

  inline void set_optional_size(uint32_t optional_size) {
    optional_size_ = optional_size;
  }

  inline uint32_t optional_size() const {
    return optional_size_;
  }

  inline uint16_t n_general_pts() const {
    return n_general_pts_;
  }

  inline uint32_t offset() const {
    return offset_;
  }

  inline const std::vector<KPTUtil>& kpts() const {
    return kpts_;
  }

  inline std::vector<KPTUtil>& kpts() {
    return kpts_;
  }

  inline const std::vector<NullableStringRef>& options() const {
    return options_;
  }

  inline std::vector<NullableStringRef>& options() {
    return options_;
  }

  inline void set_n_general_pts(uint16_t n) {
    n_general_pts_ = n;
  }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    context_copy = nullptr;
    auto ctxt = alloc_context<RecordInsertContext>(sizeof(RecordInsertContext));
    if(!ctxt.get()) return Status::OutOfMemory;
    char* res = (char*)malloc(payload_size_);
    memcpy(res, payload_, payload_size_);
    new(ctxt.get()) RecordInsertContext{ payload_, payload_size_,  kpts_, n_general_pts_, offset_, optional_size_ };
    context_copy = ctxt.release();
    return Status::Ok;
  }
 private:
  const char* payload_;
  uint32_t payload_size_;
  std::vector<KPTUtil> kpts_;
  std::vector<NullableStringRef> options_;
  uint16_t n_general_pts_;
  uint32_t offset_;
  uint32_t optional_size_;
};

/// FishStore's internal Insert() context.
/// An internal Insert() context that has gone async and lost its type information.
class AsyncPendingInsertContext : public PendingContext {
 protected:
  AsyncPendingInsertContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_)
    : PendingContext(OperationType::Insert, caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  AsyncPendingInsertContext(AsyncPendingInsertContext& other, IAsyncContext* caller_context)
    : PendingContext(other, caller_context) {
  }
 public:
  virtual const char* payload() const = 0;
  virtual uint32_t payload_size() const = 0;
  virtual uint16_t n_general_pts() const = 0;
  virtual const std::vector<KPTUtil>& kpts() const = 0;
  virtual uint32_t optional_size() const = 0;
  virtual const std::vector<NullableStringRef>& options() const = 0;
};

/// A synchronous Insert() context preserves its type information.
template <class IC>
class PendingInsertContext : public AsyncPendingInsertContext {
 public:
  typedef IC insert_context_t;
  typedef Record record_t;

  PendingInsertContext(insert_context_t& caller_context_, AsyncCallback caller_callback_)
    : AsyncPendingInsertContext(caller_context_, caller_callback_) {
  }
  /// The deep copy constructor.
  PendingInsertContext(PendingInsertContext& other, IAsyncContext* caller_context_)
    : AsyncPendingInsertContext(other, caller_context_) {
  }
 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext::caller_context,
                                            context_copy);
  }
 private:
  inline const insert_context_t& insert_context() const {
    return *static_cast<const insert_context_t*>(PendingContext::caller_context);
  }
  inline insert_context_t& insert_context() {
    return *static_cast<insert_context_t*>(PendingContext::caller_context);
  }
 public:
  inline uint32_t payload_size() const final {
    return insert_context().payload_size();
  }
  inline const char* payload() const final {
    return insert_context().payload();
  }
  inline uint32_t optional_size() const final {
    return insert_context().optional_size();
  }
  inline uint16_t n_general_pts() const final {
    return insert_context().n_general_pts();
  }
  inline const std::vector<NullableStringRef>& options() const final {
    return insert_context().options();
  }
  inline const std::vector<KPTUtil>& kpts() const final {
    return insert_context().kpts();
  }
};

/// FishStore's internal Scan() context.
/// An internal Scan() context that has gone async and lost its type information.
class AsyncPendingScanContext : public PendingContext {
 protected:
  AsyncPendingScanContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_)
    : PendingContext(OperationType::Scan, caller_context_, caller_callback_), io_level(0) {
  }

  AsyncPendingScanContext(AsyncPendingScanContext& other, IAsyncContext* caller_context_)
    : PendingContext(other, caller_context_), io_level(other.io_level) {
  }
 public:
  inline uint32_t scan_offset_bit() {
    // Get the scan offset bit for the current IO level.
    return Constants::address_offset_bit[io_level];
  }
  inline void setIOLevel(const uint64_t& gap) {
    // If we see a real gap that is bigger than our tolerance threshold.
    // We switch IO level back to 0. Otherwise, we keep observing more
    // and more locality, increase IO level to do more aggressive prefetching
    // so as to save the number of IOs.
    if(gap > Constants::gap_thres + Constants::avg_rec_size) {
      io_level = 0;
    } else if(io_level < Constants::kIoLevels - 1) io_level++;
  }

  virtual void Touch(const void* rec) = 0;
  virtual void Finalize() = 0;
  virtual bool check(const KeyPointer* ptr) = 0;
 private:
  uint8_t io_level;

};

template <class SC>
class PendingScanContext : public AsyncPendingScanContext {
 public:
  typedef SC scan_context_t;
  typedef Record record_t;

  PendingScanContext(scan_context_t& caller_context_, AsyncCallback caller_callback_)
    : AsyncPendingScanContext(caller_context_, caller_callback_) {
  }

  /// The deep copy constructor.
  PendingScanContext(PendingScanContext& other, IAsyncContext* caller_context_)
    : AsyncPendingScanContext(other, caller_context_) {
  }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext::caller_context, context_copy);
  }

 private:
  inline const scan_context_t& scan_context() const {
    return *static_cast<const scan_context_t*>(PendingContext::caller_context);
  }
  inline scan_context_t& scan_context() {
    return *static_cast<scan_context_t*>(PendingContext::caller_context);
  }

 public:
  inline void Touch(const void* rec) final {
    const record_t* record = reinterpret_cast<const record_t*>(rec);
    scan_context().Touch(record->payload(), record->payload_size());
  }

  inline void Finalize() final {
    scan_context().Finalize();
  }

  inline bool check(const KeyPointer* ptr) final {
    return scan_context().check(ptr);
  }

  inline KeyHash get_hash() const final {
    return scan_context().get_hash();
  }
};

/// FishStore's internal FullScan() context.
/// An internal FullScan() context that has gone async and lost its type information.
class AsyncPendingFullScanContext : public PendingContext {
 protected:
  AsyncPendingFullScanContext(IAsyncContext& caller_context_, AsyncCallback caller_callback_)
    : PendingContext(OperationType::FullScan, caller_context_, caller_callback_) {
  }

  AsyncPendingFullScanContext(AsyncPendingFullScanContext& other, IAsyncContext* caller_context_)
    : PendingContext(other, caller_context_) {
  }
 public:
  virtual void Touch(const void* rec) = 0;
  virtual void Finalize() = 0;
  virtual bool check(const char* payload, uint32_t payload_size) = 0;
};

template <class SC>
class PendingFullScanContext : public AsyncPendingFullScanContext {
 public:
  typedef SC scan_context_t;
  typedef Record record_t;

  PendingFullScanContext(scan_context_t& caller_context_, AsyncCallback caller_callback_)
    : AsyncPendingFullScanContext(caller_context_, caller_callback_) {
  }

  /// The deep copy constructor.
  PendingFullScanContext(PendingFullScanContext& other, IAsyncContext* caller_context_)
    : AsyncPendingFullScanContext(other, caller_context_) {
  }

 protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
    return IAsyncContext::DeepCopy_Internal(*this, PendingContext::caller_context, context_copy);
  }

 private:
  inline const scan_context_t& scan_context() const {
    return *static_cast<const scan_context_t*>(PendingContext::caller_context);
  }
  inline scan_context_t& scan_context() {
    return *static_cast<scan_context_t*>(PendingContext::caller_context);
  }

 public:
  inline void Touch(const void* rec) final {
    const record_t* record = reinterpret_cast<const record_t*>(rec);
    scan_context().Touch(record->payload(), record->payload_size());
  }

  inline void Finalize() final {
    scan_context().Finalize();
  }

  inline bool check(const char* payload, uint32_t payload_size) final {
    return scan_context().check(payload, payload_size);
  }
};


class AsyncIOContext;

/// Per-thread execution context. (Just the stuff that's checkpointed to disk.)
struct PersistentExecContext {
  PersistentExecContext()
    : serial_num{ 0 }
    , version{ 0 }
    , offset{ 0 }
    , guid{} {
  }

  void Initialize(uint32_t version_, const Guid& guid_, uint64_t serial_num_, uint32_t offset_ = 0) {
    serial_num = serial_num_;
    version = version_;
    guid = guid_;
    offset = offset_;
  }

  uint64_t serial_num;
  uint32_t offset;
  uint32_t version;
  /// Unique identifier for this session.
  Guid guid;
};
static_assert(sizeof(PersistentExecContext) == 32, "sizeof(PersistentExecContext) != 32");

/// Per-thread execution context. (Also includes state kept in-memory-only.)
struct ExecutionContext : public PersistentExecContext {
  /// Default constructor.
  ExecutionContext()
    : phase{ Phase::INVALID }
    , io_id{ 0 }
    , pending_io_cnt{ 0 } {
  }

  void Initialize(Phase phase_, uint32_t version_, const Guid& guid_, uint64_t serial_num_,
                  uint32_t offset) {
    assert(retry_requests.empty());
    assert(pending_ios.empty());
    assert(io_responses.empty());

    PersistentExecContext::Initialize(version_, guid_, serial_num_, offset);
    phase = phase_;
    retry_requests.clear();
    io_id = 0;
    pending_ios.clear();
    io_responses.clear();
    pending_io_cnt = 0;
  }

  Phase phase;

  /// Retry request contexts are stored inside the deque.
  std::deque<IAsyncContext*> retry_requests;
  /// Assign a unique ID to every I/O request.
  uint64_t io_id;
  /// For each pending I/O, maps io_id to the hash of the key being retrieved.
  std::unordered_map<uint64_t, KeyHash> pending_ios;
  uint32_t pending_io_cnt;

  /// The I/O completion thread hands the PendingContext back to the thread that issued the
  /// request.
  concurrent_queue<AsyncIOContext*> io_responses;
};

}
} // namespace fishstore::core
