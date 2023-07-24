// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unordered_map>
#include <cstdint>
#include <cassert>
#include <utility>
#include <vector>

#ifdef _MSC_VER
#define NOMINMAX
#endif

#include <simdjson.h>
#include "adapters/common_utils.h"

constexpr size_t DEFAULT_BATCH_SIZE = 1 << 24;

using namespace simdjson;

namespace fishstore {
    namespace adapter {

        // Represents a SimdJson field
        class SIMDJsonField {
        public:
            // constructs a SimdJsonField with a given simdjson value
            SIMDJsonField(int64_t id_, const simdjson_result <ondemand::value> &value_)
                    : field_id(id_), simd_value(value_) {}

            inline int64_t FieldId() const {
                return field_id;
            }

            inline NullableBool GetAsBool() const {
                bool val;
                bool has_value = (simd_value.get(val) == error_code::SUCCESS);
                return {has_value, val};
            }

            inline NullableInt GetAsInt() const {
                int64_t val;
                bool has_value = (simd_value.get(val) == error_code::SUCCESS);
                return {has_value, static_cast<int32_t>(val)};
            }

            inline NullableLong GetAsLong() const {
                int64_t val;
                bool has_value = (simd_value.get(val) == error_code::SUCCESS);
                return {has_value, val};
            }

            inline NullableFloat GetAsFloat() const {
                double val;
                bool has_value = (simd_value.get(val) == error_code::SUCCESS);
                return {has_value, static_cast<float>(val)};
            }

            inline NullableDouble GetAsDouble() const {
                double val;
                bool has_value = (simd_value.get(val) == error_code::SUCCESS);
                return {has_value, val};
            }

            inline NullableString GetAsString() const {

                // the simdjson get method only supports std::string_view, so we must
                // turn it into a std::string after
                std::string_view temp{};
                bool has_value = (simd_value.get(temp) == error_code::SUCCESS);
                return {has_value, std::string(temp)};
            }

            inline NullableStringRef GetAsStringRef() const {
                std::string_view temp{};
                bool has_value = (simd_value.get(temp) == error_code::SUCCESS);
                StringRef str_ref{temp.data(), temp.length()};
                return {has_value, str_ref};
            }

        private:
            int64_t field_id;
            mutable simdjson_result <ondemand::value> simd_value;
        };

        // represents the type of field that will be looked up (object or array)
        enum class SIMDJsonFieldType {
            OBJECT,
            ARRAY
        };

        // represents the type of field that will be looked up and
        // the name or index that should be looked up
        // this is stored in a vector in SIMDJsonFieldLookup
        struct SIMDJsonFieldLookupElement {
            SIMDJsonFieldType type;
            union {
                std::string_view key_name;
                size_t array_index;
            };
        };

        // helper class used to look up simdjson fields
        class SIMDJsonFieldLookup {
        public:
            // creates a simdjson field from a string
            SIMDJsonFieldLookup() = default;

            explicit SIMDJsonFieldLookup(const std::string &lookup_str_) : lookup_str(
                    std::make_unique<std::string>(lookup_str_)) {
                char *start = lookup_str->data();
                char *end = lookup_str->data();
                while (true) {
                    if (*end == '.' || *end == '\0') { // end of  a field
                        size_t str_len = end - start;
                        if (str_len == 0) {
                            break;
                        }
                        std::string_view field = {start, str_len};
                        SIMDJsonFieldLookupElement item = {.type=SIMDJsonFieldType::OBJECT, .key_name=field};
                        lookups.push_back(item);

                        if (*end == '\0') {
                            break;
                        }


                        start = end + 1;
                    } else if (*start == '[') {
                        // strtol will move the end pointer right after the last number, so to the ']'.
                        size_t index = std::strtol(++start, &end, 10);
                        SIMDJsonFieldLookupElement item = {.type=SIMDJsonFieldType::ARRAY, .array_index=index};
                        lookups.push_back(item);
                        start = end + 1;
                    }
                    end++;
                }
            }

            simdjson_result <ondemand::value> find(ondemand::object source) const {
                auto fields_it = lookups.begin();

                // always starts with an object
                auto ret = source.find_field_unordered(fields_it->key_name);
                fields_it++;

                // iterate through the entire lookups
                while (fields_it != lookups.end()) {
                    // if object, or array find the correct value
                    if (fields_it->type == SIMDJsonFieldType::OBJECT) {
                        ret = ret.find_field_unordered(fields_it->key_name);
                    } else if (fields_it->type == SIMDJsonFieldType::ARRAY) {
                        ret = ret.at(fields_it->array_index);
                    }
                    fields_it++;
                }

                return ret;
            }

            std::string ToString() const {
                return *lookup_str;
            }

        private:
            std::vector<SIMDJsonFieldLookupElement> lookups;
            std::unique_ptr<std::string> lookup_str; // kept around for memory management
        };

        class SIMDJsonRecord {
        public:
            friend class SIMDJsonParser;

            SIMDJsonRecord() = default;

            SIMDJsonRecord(ondemand::document_reference doc, const std::vector<SIMDJsonFieldLookup> &lookups) {
                obj = doc.get_object();
                int i = 0;
                for (const auto &lookup: lookups) {
                    const auto value = lookup.find(obj);
                    // check the value was found if not, don't add to vector
                    if (value.error() == simdjson::SUCCESS) {
                        fields.emplace_back(i, value);
                    } else {
                        fprintf(stderr, "FIELD NOT FOUND: [%s]\n", lookup.ToString().c_str());
                    }
                    ++i;
                }
            }

            inline const std::vector<SIMDJsonField> &GetFields() const {
                return fields;
            }

            inline StringRef GetRawText() const {
                obj.reset();
                auto ref = obj.raw_json().value();
                return {ref.data(), ref.length()};
            }

        public:
            mutable ondemand::object obj;
            std::vector<SIMDJsonField> fields;
        };

        class SIMDJsonParser {
        public:
            SIMDJsonParser(const std::vector<std::string> &field_names) {
                field_lookups.reserve(field_names.size());
                for (const auto &item: field_names) {
                    field_lookups.emplace_back(item);
                }
            }

            inline void Load(const char *buffer, size_t length) {
                if (parser.iterate_many(buffer, length, DEFAULT_BATCH_SIZE).get(docs) != simdjson::SUCCESS)
                    return;
                docs_it = docs.begin();
            }

            inline bool HasNext() {
                return docs_it != docs.end();
            }

            inline const SIMDJsonRecord &NextRecord() {
                record = SIMDJsonRecord(*docs_it, field_lookups);
                ++docs_it;
                return record;
            }

        private:
            std::vector<SIMDJsonFieldLookup> field_lookups;

            // keep these around for memory safety reasons
            ondemand::parser parser;
            ondemand::document_stream docs;
            ondemand::document_stream::iterator docs_it;

            SIMDJsonRecord record;
        };

        class SIMDJsonAdapter {
        public:
            typedef SIMDJsonParser parser_t;
            typedef SIMDJsonField field_t;
            typedef SIMDJsonRecord record_t;

            inline static parser_t *NewParser(const std::vector<std::string> &fields) {
                return new parser_t{fields};
            }

            inline static void Load(parser_t *const parser, const char *payload, size_t length, size_t offset = 0) {
                assert(offset <= length);
                parser->Load(payload + offset, length - offset);
            }

            inline static bool HasNext(parser_t *const parser) {
                return parser->HasNext();
            }

            inline static const record_t &NextRecord(parser_t *const parser) {
                return parser->NextRecord();
            }
        };
    }
}

