// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cassert>
#include <cstdint>
#include "address.h"
#include "auto_ptr.h"

namespace fishstore {
namespace core {

/// Record header, internal to FishStore.
class RecordInfo {
 public:
  RecordInfo(uint16_t checkpoint_version_, bool final_bit_, bool tombstone_, bool invalid_,
             uint32_t ptr_cnt_, uint32_t payload_size_, uint32_t optional_size_)
    : checkpoint_version{ checkpoint_version_ }
    , final_bit{ final_bit_ }
    , tombstone{ tombstone_ }
    , invalid{ invalid_ }
    , ptr_cnt{ ptr_cnt_ }
    , payload_size{ payload_size_ }
    , optional_size{ optional_size_ } {
  }

  RecordInfo(const RecordInfo& other)
    : control_{ other.control_ } {
  }

  inline bool IsNull() const {
    return control_ == 0;
  }

  union {
      struct {
        uint64_t ptr_cnt : 15;
        uint64_t payload_size : 25;
        uint64_t optional_size : 8;
        uint64_t checkpoint_version : 13;
        uint64_t invalid : 1;
        uint64_t tombstone : 1;
        uint64_t final_bit : 1;
      };

      uint64_t control_;
      std::atomic_uint64_t atomic_control_;
    };
};
static_assert(sizeof(RecordInfo) == 8, "sizeof(RecordInfo) != 8");
static_assert(alignof(RecordInfo) == 8, "alignof(RecordInfo) != 8");

struct Record;

// Compare and swap utility that indicates the first 8 byte of key pointer.
union KPTCASUtil {
  KPTCASUtil(uint64_t other): control(other) {}

  struct {
    uint64_t mode : 1;
    uint64_t ptr_offset : 15;
    uint64_t prev_address : 48;
  };
  uint64_t control;
};

// KeyPointers are the basic structure on the hash chain. They live right next to the record header and
// the record payload so as to navigate to the right position correctly and efficiently.
// Note that pointers in the hash chain are pointed directly to the head of a KeyPointer rather than the
// head of the record so as to make insertion, scan more efficient. If the pointers in hash chain points
// to head of a record, we will have no choice but to scan through all key pointer to find the correct hash
// chain that we want to explore next.
struct KeyPointer {
  KeyPointer(uint16_t ptr_offset_, uint64_t prev_address_, uint16_t general_psf_id_,
             uint32_t value_offset_, uint32_t value_size_)
    : mode(0), ptr_offset{ptr_offset_}, prev_address(prev_address_), general_psf_id(general_psf_id_),
      value_offset(value_offset_), value_size(value_size_) {}

  KeyPointer(uint16_t ptr_offset_, uint64_t prev_address_, uint32_t inline_psf_id_, uint32_t value_)
    : mode(1), ptr_offset{ptr_offset_}, prev_address(prev_address_),
      inline_psf_id(inline_psf_id_), value(value_) {}

  inline Record* get_record() const {
    return reinterpret_cast<Record*>(reinterpret_cast<uintptr_t>(this)
                                     - sizeof(KeyPointer) * ptr_offset
                                     - sizeof(RecordInfo));
  }

  inline KeyHash get_hash() const {
    if(mode == 0) {
      return KeyHash{ Utility::HashBytesWithPSFID(general_psf_id, get_value(), value_size) };
    } else {
      assert(mode == 1);
      return KeyHash{ Utility::GetHashCode(inline_psf_id, value) };
    }
  }

  const char* get_value() const;

  // This function try to CAS the `prev_address` of current KeyPointer with an expected address.
  inline bool alter(Address new_address, KPTCASUtil& expected_cas) {
    KPTCASUtil updated{ expected_cas };
    updated.prev_address = new_address.control();
    auto expected = expected_cas.control;
    bool success = control.compare_exchange_strong(expected, updated.control);
    expected_cas.control = expected;
    return success;
  }

  // mode 0: field-based key pointer.
  // mode 1: predicate-based key pointer.
  // ptr_offset: the offset of current key pointer among all key pointers of the record.
  // prev_address: points to next key pointer in the hash chain.
  union {
    struct {
      uint64_t mode : 1;
      uint64_t ptr_offset : 15;
      uint64_t prev_address : 48;
    };
    std::atomic_uint64_t control;
  };

  // Mode 0
  // field_id: 14 bit field ID assigned by naming service.
  // field_offset: offset of the field value starting from the head of payload.
  // field_size: sizeo of the field value.
  // Mode 1
  // predicate_id: 64 bit predicate ID assigned by naming service.
  union {
    struct {
      uint64_t general_psf_id : 14;
      uint64_t value_offset : 25;
      uint64_t value_size : 25;
    };

    struct {
      uint32_t inline_psf_id;
      uint32_t value;
    };
  };
};

static_assert(sizeof(KeyPointer) == 16, "sizeof(KeyPointer) == 16");
static_assert(alignof(KeyPointer) == 8, "sizeof(KeyPointer) == 8");

// Record Layout:
// - Header: contains payload size, number of key pointers, checkpoint version, invalid bit (visible bit), etc.
// - KeyPointer Section: a bunch of key pointers in different hash chains which refer to the current record.
// - Payload Section: contains the semi-structured data payload.
// Starting from header we are able to get the size of record, navigate to the payload and all key pointers.
// Starting from any key pointer, we can navigate to the header, indirectly the payload head, and the position of a
// registered field (with a field-based key pointer).
// Note that all records are 8 byte aligned (since header and key pointers are 8 byte aligned and the payload is 1 byte aligned).
struct Record {
  Record(RecordInfo header_)
    : header(header_) {}

  inline static constexpr uint32_t size(uint32_t ptr_cnt, uint32_t payload_size,
                                        uint32_t optional_size) {
    return static_cast<uint32_t>(pad_alignment(
                                   sizeof(RecordInfo) + sizeof(KeyPointer) * ptr_cnt
                                   + payload_size + optional_size, alignof(RecordInfo)));
  }

  inline constexpr uint32_t size() const {
    return size(header.ptr_cnt, header.payload_size, header.optional_size);
  }

  inline KeyPointer* get_ptr(uint32_t offset) const {
    return reinterpret_cast<KeyPointer*>(reinterpret_cast<uintptr_t>(this)
                                         + sizeof(RecordInfo)
                                         + offset * sizeof(KeyPointer));
  }

  inline constexpr static uint32_t ptr_offset(uint16_t offset) {
    return sizeof(RecordInfo) + sizeof(KeyPointer) * offset;
  }

  inline const char* payload() const {
    return reinterpret_cast<const char*>(this) + sizeof(RecordInfo)
           + header.ptr_cnt * sizeof(KeyPointer);
  }

  inline char* payload() {
    return reinterpret_cast<char*>(this) + sizeof(RecordInfo)
           + header.ptr_cnt * sizeof(KeyPointer);
  }

  inline uint32_t payload_size() const {
    return header.payload_size;
  }

 public:
  RecordInfo header;
};

inline const char* KeyPointer::get_value() const {
  assert(mode == 0);
  return get_record()->payload() + value_offset;
}

}
} // namespace fishstore::core
