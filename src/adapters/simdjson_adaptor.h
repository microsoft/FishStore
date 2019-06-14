// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unordered_map>
#include <cstdint>
#include <cassert>
#include <vector>

#include <simdjson/parsedjson.h>
#include <simdjson/jsonparser.h>
#include "adapters/common_utils.h"

namespace fishstore {
namespace adapter {

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
  SIMDJsonParser(const std::vector<std::string>& field_names, const size_t alloc_bytes = 1LL << 25) {
    root = new TreeNode{};
    for (auto field_id = 0; field_id < field_names.size(); ++field_id) {
      auto& field_name = field_names[field_id];
      auto current_tree_node = root;
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
    has_next = false;
  }

  ~SIMDJsonParser() {
    delete root;
  }

  inline void Load(const char* buffer, size_t length) {
    record.original = StringRef(buffer, length);
    record.fields.clear();
    auto ok = json_parse(buffer, length, pj);
    if (ok != 0 || !pj.isValid()) {
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
    ParsedJson::iterator it(pj);
    Traverse(it, root, record.fields);
    has_next = false;
    return record;
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

  TreeNode* root;
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

