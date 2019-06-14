#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "adapters/common_utils.h"

using namespace fishstore::adapter;

class FieldValue {
public:
  FieldValue& operator=(const FieldValue& other);

  int FieldId() const;
  bool IsMissing() const;
  
  NullableBool GetAsBool() const;
  NullableInt GetAsInt() const;
  NullableLong GetAsLong() const;
  NullableFloat GetAsFloat() const;
  NullableDouble GetAsDouble() const;
  NullableString GetAsString() const;
  NullableStringRef GetAsStringRef() const;
};

class Record {
public:
  StringRef GetRawText() const;
  const std::vector<FieldValue>& GetFields() const;
};

class Parser {
  Parser(const std::vector<std::string> &fields);
  void Load(const char* buffer, size_t length);
  bool HasNext();
  const Record& NextRecord();
};
