// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unordered_map>
#include <cstdint>
#include <cassert>
#include <vector>

#ifdef _MSC_VER
#define NOMINMAX
#endif
#include <simdjson/parsedjson.h>
#include <simdjson/jsonparser.h>
#include "adapters/common_utils.h"

using namespace simdjson;

namespace fishstore {
namespace adapter {

class SIMDJsonField {
public:
  SIMDJsonField(int64_t id_, const ParsedJson::Iterator& it_)
    : field_id(id_), iter(it_) {}

  inline int64_t FieldId() const {
    return field_id;
  }

  inline NullableBool GetAsBool() const {
    switch (iter.get_type()) {
    case 't':
      return NullableBool(true);
    case 'f':
      return NullableBool(false);
    default:
      return NullableBool();
    }
  }

  inline NullableInt GetAsInt() const {
    if (iter.is_integer()) {
      return NullableInt(static_cast<int32_t>(iter.get_integer()));
    } else return NullableInt();
  }

  inline NullableLong GetAsLong() const {
    if (iter.is_integer()) {
      return NullableLong(iter.get_integer());
    } else return NullableLong();
  }

  inline NullableFloat GetAsFloat() const {
    if (iter.is_double()) {
      return NullableFloat(static_cast<float>(iter.get_double()));
    } else return NullableFloat();
  }

  inline NullableDouble GetAsDouble() const {
    if (iter.is_double()) {
      return NullableDouble(iter.get_double());
    } else return NullableDouble();
  }

  inline NullableString GetAsString() const {
    if (iter.is_string()) {
      return NullableString(std::string(iter.get_string(), iter.get_string_length()));
    } else return NullableString();
  }

  inline NullableStringRef GetAsStringRef() const {
    if (iter.is_string()) {
      return NullableStringRef(StringRef(iter.get_string(), iter.get_string_length()));
    } else return NullableStringRef();
  }

private:
  int64_t field_id;
  ParsedJson::Iterator iter;
};

class SIMDJsonRecord {
public:
  friend class SIMDJsonParser;

  SIMDJsonRecord() : original(), fields() {}

  SIMDJsonRecord(const char* data, size_t length)
    : original(data, length) {
    fields.clear();
  }

  inline const std::vector<SIMDJsonField>& GetFields() const {
    return fields;
  }

  inline StringRef GetRawText() const {
    return original;
  }

public:
  StringRef original;
  std::vector<SIMDJsonField> fields;
};

class SIMDJsonParser {
public:
  SIMDJsonParser(const std::vector<std::string>& field_names, const size_t alloc_bytes = 1LL << 25)
  : fields(field_names) {
    auto success = pj.allocate_capacity(alloc_bytes);
    assert(success);
    has_next = false;
  }

  inline void Load(const char* buffer, size_t length) {
    record.original = StringRef(buffer, length);
    record.fields.clear();
    auto ok = json_parse(buffer, length, pj);
    if (ok != 0 || !pj.is_valid()) {
      printf("Parsing failed...\n");
      has_next = false;
    } else {
      has_next = true;
    }
  }

  inline bool HasNext() {
    return has_next;
  }

  inline const SIMDJsonRecord& NextRecord() {
    ParsedJson::Iterator it(pj);
    for (auto field_id = 0; field_id < fields.size(); ++field_id) {
      if (it.move_to(fields[field_id])) {
        record.fields.emplace_back(SIMDJsonField{field_id, it});
      }
    }
    has_next = false;
    return record;
  }

private:
  std::vector<std::string> fields;
  ParsedJson pj;
  SIMDJsonRecord record;
  bool has_next;
};

class SIMDJsonAdapter {
public:
  typedef SIMDJsonParser parser_t;
  typedef SIMDJsonField field_t;
  typedef SIMDJsonRecord record_t;

  inline static parser_t* NewParser(const std::vector<std::string>& fields) {
    return new parser_t{ fields };
  }

  inline static void Load(parser_t* const parser, const char* payload, size_t length, size_t offset = 0) {
    assert(offset <= length);
    parser->Load(payload + offset, length - offset);
  }

  inline static bool HasNext(parser_t* const parser) {
    return parser->HasNext();
  }

  inline static const record_t& NextRecord(parser_t* const parser) {
    return parser->NextRecord();
  }
};

}
}

