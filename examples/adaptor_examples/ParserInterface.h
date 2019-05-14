#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "adaptors/common_utils.h"

using namespace fishstore::adaptor;

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
  class Iterator: public std::iterator<std::input_iterator_tag, const FieldValue> {
    public:
      inline const FieldValue& operator*() const;
      inline const FieldValue* operator->() const;
      inline Iterator& operator++();
      inline Iterator operator++(int);
      inline bool operator==(const Iterator& rhs);
      inline bool operator!=(const Iterator& rhs);      
  };
    
  Iterator begin() const;
  inline Iterator end() const;
  StringRef GetAsRawTextRef() const;
};

class Records {
  class Iterator: public std::iterator<std::input_iterator_tag, const Record> {
  public:
    inline const Record& operator*() const;
    inline const Record* operator->() const;
    inline Iterator& operator++();
    inline Iterator operator++(int);
    inline bool operator==(const Iterator& rhs);
    inline bool operator!=(const Iterator& rhs);
  };
  
  Iterator begin() const;
  inline Iterator end() const;
};

class Parser {
  Parser(const std::vector<std::string> &fields);
  Records Parse(const char* buffer, size_t length, size_t offset);
};
