// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unordered_map>
#include <cstdint>
#include <cassert>
#include <vector>

#include <simdjson.h>
#include <adapters/common_utils.h>

const size_t DEFAULT_BATCH_SIZE = 1000000;

namespace fishstore {
namespace adapter {


class SIMDJsonField {
public:
  SIMDJsonField(int64_t id_, const simdjson::dom::element e)
    : field_id(id_), ele(e) {}

  inline int64_t FieldId() const {
    return field_id;
  }

  inline NullableBool GetAsBool() const {
    if (ele.is_bool()) {
      return NullableBool(ele.get_bool().value());
    }
    else {
      return NullableBool();
    }
  }

  inline NullableInt GetAsInt() const {
    if (ele.is_int64()) {
      return NullableInt(ele.get_int64().value());
    }
    else {
      return NullableInt();
    }
  }

  inline NullableLong GetAsLong() const {
    if (ele.is_int64()) {
      return NullableLong(ele.get_int64().value());
    }
    else {
      return NullableLong();
    }
  }

  inline NullableFloat GetAsFloat() const {
    if (ele.is_double()) {
      return NullableFloat(ele.get_double().value());
    }
    else {
      return NullableFloat();
    }
  }

  inline NullableDouble GetAsDouble() const {
    if (ele.is_double()) {
      return NullableDouble(ele.get_double().value());
    }
    else {
      return NullableDouble();
    }
  }

  inline NullableString GetAsString() const {
    if (ele.is_string()) {
      auto tmp = ele.get_string().value();
      return NullableString(std::string(tmp.data(), tmp.size()));
    }
    else return NullableString();
  }

  inline NullableStringRef GetAsStringRef() const {
    if (ele.is_string()) {
      auto tmp = ele.get_string().value();
      return NullableStringRef({ tmp.data(), tmp.size() });
    }
    else return NullableStringRef();
  }

private:
  int64_t field_id;
  const simdjson::dom::element ele;
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

  inline void clear() {
    original.ptr = NULL;
    original.size = 0;
    fields.clear();
  }

public:
  StringRef original;
  std::vector<SIMDJsonField> fields;
};

class SIMDJsonParser {
public:
  SIMDJsonParser(const std::vector<std::string>& field_names)
  : fields(field_names), parser_(), stream(), buffer_(NULL), len_(0), record_() {}

  inline void Load(const char* buffer, size_t length) {
    //XX: buffer is not padded, may have issue
    buffer_ = buffer;
    len_ = length;
    parser_.parse_many(buffer, length, DEFAULT_BATCH_SIZE).get(stream);
    it = stream.begin();
  }

  inline bool HasNext() {
    return it != stream.end();
  }

  inline const SIMDJsonRecord& NextRecord() {
    record_.clear();
    record_.original.ptr = buffer_ + it.current_index();
    auto last_index = it.current_index();   
    for (auto& field : fields) {
      record_.fields.emplace_back(SIMDJsonField(record_.fields.size(), (*it).at_pointer(field).value()));
    }
    ++it;
    record_.original.size = it != stream.end() ? it.current_index() - last_index : len_ - last_index;
    return record_;
  }

private:
  const char* buffer_;
  size_t len_;
  std::vector<std::string> fields;
  simdjson::dom::parser parser_;
  simdjson::dom::document_stream stream;
  simdjson::dom::document_stream::iterator it;
  SIMDJsonRecord record_;
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

