// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unordered_map>
#include <cstdint>
#include <cassert>
#include <vector>

#include <simdjson/parsedjson.h>
#include <simdjson/jsonparser.h>
#include "adaptors/common_utils.h"

namespace fishstore {
namespace adaptor {

struct TreeNode {
  TreeNode(): field_id(-1) {}
  TreeNode(int64_t id): field_id(id) {}
  ~TreeNode() {
    for (auto x: children) delete x.second;
  }

  int64_t field_id = -1;
  std::unordered_map<std::string, TreeNode*> children;
};

class SIMDJsonField {
public:
  SIMDJsonField(int64_t id_, const ParsedJson::iterator& it_)
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
  ParsedJson::iterator iter;
};

class SIMDJsonRecord {
public:
  friend class SIMDJsonParser;

  class Iterator : public std::iterator<std::input_iterator_tag, const SIMDJsonField> {
  public:
    Iterator(const SIMDJsonRecord* const rec_, size_t offset_ = 0)
      : rec(rec_), offset(offset_) {}

    inline const SIMDJsonField& operator*() const {
      return rec->fields[offset];
    }

    inline const SIMDJsonField* operator->() const {
      return &rec->fields[offset];
    }

    inline Iterator& operator++() {
      ++offset;
      return *this;
    }
    inline Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    inline bool operator==(const Iterator& rhs) {
      return rec == rhs.rec && offset == rhs.offset;
    }
    inline bool operator!=(const Iterator& rhs) {
      return !(*this == rhs);
    }
  private:
    const SIMDJsonRecord* const rec;
    size_t offset;
  };

  SIMDJsonRecord() : original(), fields() {}

  SIMDJsonRecord(const char* data, size_t length)
    : original(data, length) {
    fields.clear();
  }

  inline Iterator begin() const {
    return Iterator{ this };
  }

  inline Iterator end() const {
    return Iterator{ this, fields.size() };
  }

  inline StringRef GetAsRawTextRef() const {
    return original;
  }

public:
  StringRef original;
  std::vector<SIMDJsonField> fields;
};

class SIMDJsonRecords {
public:
  friend class SIMDJsonParser;

  class Iterator : public std::iterator<std::input_iterator_tag, const SIMDJsonRecord> {
  public:
    Iterator(const SIMDJsonRecords* const recs_, bool done_ = false)
      : recs(recs_), done(done_) {}

    inline const SIMDJsonRecord& operator*() const {
      return recs->record;
    }

    inline const SIMDJsonRecord* operator->() const {
      return &(recs->record);
    }

    inline Iterator& operator++() {
      done = true;
      return *this;
    }

    inline Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    inline bool operator==(const Iterator& rhs) {
      return recs == rhs.recs && done == rhs.done;
    }

    inline bool operator!=(const Iterator& rhs) {
      return !(*this == rhs);
    }

  private:
    const SIMDJsonRecords* const recs;
    bool done = false;
  };

  SIMDJsonRecords() : record() {
  }

  SIMDJsonRecords(const SIMDJsonRecord& rec) : record(rec) {}

  inline Iterator begin() const {
    return Iterator(this);
  }

  inline Iterator end() const {
    return Iterator(this, true);
  }
private:
  SIMDJsonRecord record;
};


class SIMDJsonParser {
public:
  SIMDJsonParser(const std::vector<std::string>& field_names, const size_t alloc_bytes = 1LL << 25) {
    fields = new TreeNode{};
    for (auto field_id = 0; field_id < field_names.size(); ++field_id) {
      auto& field_name = field_names[field_id];
      auto current_tree_node = fields;
      size_t start_pos = 0, pos = 0;
      while (std::string::npos != (pos = field_name.find('.', start_pos))) {
        auto slice = field_name.substr(start_pos, pos - start_pos);
        auto iter = current_tree_node->children.find(slice);
        if (iter != current_tree_node->children.end()) {
          current_tree_node = iter->second;
        }
        else {
          auto new_node = current_tree_node->children.emplace(std::make_pair(slice, new TreeNode{}));
          current_tree_node = new_node.first->second;
        }
        start_pos = pos + 1;
      }

      auto slice = field_name.substr(start_pos);
      auto iter = current_tree_node->children.find(slice);
      if (iter != current_tree_node->children.end()) {
        printf("Skipping field %s as seen before...\n", field_name.c_str());
      }
      else current_tree_node->children.emplace(std::make_pair(slice, new TreeNode{ field_id }));
    }
    auto success = pj.allocateCapacity(alloc_bytes);
    assert(success);
  }

  ~SIMDJsonParser() {
    delete fields;
  }

  inline SIMDJsonRecords Parse(const char* buffer, size_t length) {
    auto ok = json_parse(buffer, length, pj);
    if (ok != 0 || !pj.isValid()) {
      printf("Parsing failed...\n");
      return SIMDJsonRecords();
    }

    SIMDJsonRecord record(buffer, length);
    ParsedJson::iterator it(pj);
    Traverse(it, fields, record.fields);
    return SIMDJsonRecords{ record };
  }

private:
  void Traverse(ParsedJson::iterator& it, const TreeNode* current_tree_node,
    std::vector<SIMDJsonField>& parsed_fields) {

    if (current_tree_node->field_id != -1) {
      parsed_fields.emplace_back(SIMDJsonField(current_tree_node->field_id, it));
    }
    if (!current_tree_node->children.empty() && it.is_object()) {
      if (it.down()) {
        do {
          std::string slice(it.get_string());
          auto slice_iter = current_tree_node->children.find(slice);
          it.next();
          if (slice_iter != current_tree_node->children.end()) {
            Traverse(it, slice_iter->second, parsed_fields);
          }
        } while (it.next());
        it.up();
      }
    }
  }

  TreeNode* fields;
  ParsedJson pj;
};

class SIMDJsonAdaptor: public JsonAdaptor {
public:
  typedef SIMDJsonParser parser_t;
  typedef SIMDJsonRecord record_t;
  typedef SIMDJsonField field_t;

  inline static parser_t* NewParser(const std::vector<std::string>& fields) {
    return new parser_t{ fields };
  }

  inline static SIMDJsonRecords Parse(parser_t* const parser, const char* payload, size_t length, size_t offset = 0) {
    assert(offset <= length);
    return parser->Parse(payload + offset, length - offset);
  }
};

}
}

