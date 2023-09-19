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
            SIMDJsonField(int64_t id_, const dom::element &value_)
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
            dom::element simd_value;
        };

        class SIMDJsonRecord {
        public:
            friend class SIMDJsonParser;

            SIMDJsonRecord() = default;

            SIMDJsonRecord(dom::document_stream::iterator &doc, const std::vector<std::string> &lookups) {
                auto raw_text_sv = doc.source();
                raw_text = {raw_text_sv.data(), raw_text_sv.size()};
                obj = *doc;

                int i = 0;
                for (const auto &lookup: lookups) {
                    // check the value was found if not, don't add to vector
                    auto simd_res = obj.at_pointer("/" + lookup);
                    if (simd_res.error() == simdjson::SUCCESS) {
                        fields.emplace_back(i, simd_res.value());
                    }
                    ++i;
                }
            }

            inline const std::vector<SIMDJsonField> &GetFields() const {
                return fields;
            }

            inline StringRef GetRawText() const {
                return raw_text;
            }

        public:
            StringRef raw_text;
            dom::element obj;
            std::vector<SIMDJsonField> fields;
        };

        class SIMDJsonParser {
        public:
            SIMDJsonParser(std::vector<std::string> field_names_) : field_names(std::move(field_names_)) {
            }

            inline void Load(const char *buffer, size_t length) {
                if (parser.parse_many(buffer, length, DEFAULT_BATCH_SIZE).get(docs) != simdjson::SUCCESS)
                    return;

                docs_it = docs.begin();
                record = SIMDJsonRecord(docs_it, field_names);
                got_rec = false;
            }

            inline bool HasNext() {
                if (got_rec) {
                    ++docs_it;

                    // if at end, then just return false
                    if (!(docs_it != docs.end())) {
                        got_rec = false;
                        return false;
                    }

                    record = SIMDJsonRecord(docs_it, field_names);
                    got_rec = false;
                    return true;
                }
                return docs_it != docs.end();
            }

            inline const SIMDJsonRecord &NextRecord() {
                assert(docs_it != docs.end());
                got_rec = true;
                return record;
            }

        private:
            // keep these around for memory safety reasons
            bool got_rec = false;
            std::vector<std::string> field_names;


            dom::parser parser;
            dom::document_stream docs;
            dom::document_stream::iterator docs_it;

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

