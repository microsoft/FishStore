// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include <string>
#include <vector>
#include "gtest/gtest.h"

#define _NULL_DISK

#include "adapters/simdjson_adapter.h"
#include "core/fishstore.h"
#include "device/null_disk.h"

using namespace fishstore::core;
using adapter_t = fishstore::adapter::SIMDJsonAdapter;
using disk_t = fishstore::device::NullDisk;
using store_t = FishStore<disk_t, adapter_t>;

const size_t n_records = 1000000;
const size_t n_threads = 4;

const char* pattern =
  "{\"id\": \"%zu\", \"name\": \"name%zu\", \"gender\": \"%s\", \"school\": {\"id\": \"%zu\", \"name\": \"school%zu\"}}";

class JsonGeneralScanContext : public IAsyncContext {
public:
  JsonGeneralScanContext(uint16_t psf_id, const char* value, uint32_t expected)
    : hash_{ Utility::HashBytesWithPSFID(psf_id, value, strlen(value)) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(strlen(value)) },
    value_{ value },
    cnt{ 0 },
    expected{ expected } {
  }

  JsonGeneralScanContext(uint16_t psf_id, const char* value, size_t length, uint32_t expected)
    : hash_{ Utility::HashBytesWithPSFID(psf_id, value, length) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(length) },
    value_{ value },
    cnt{ 0 },
    expected{ expected } {
  }

  JsonGeneralScanContext(const JsonGeneralScanContext& other)
    : hash_{ other.hash_ },
    psf_id_{ other.psf_id_ },
    value_size_{ other.value_size_ },
    cnt{ other.cnt },
    expected{ other.expected } {
    set_from_deep_copy();
    char* res = (char*)malloc(value_size_);
    memcpy(res, other.value_, value_size_);
    value_ = res;
  }

  ~JsonGeneralScanContext() {
    if (from_deep_copy()) free((void*)value_);
  }

  inline void Touch(const char* payload, uint32_t payload_size) {
    ++cnt;
  }

  inline void Finalize() {
    ASSERT_EQ(cnt, expected);
  }

  inline KeyHash get_hash() const {
    return hash_;
  }

  inline bool check(const KeyPointer* kpt) {
    return kpt->mode == 0 && kpt->general_psf_id == psf_id_ &&
      kpt->value_size == value_size_ &&
      !memcmp(kpt->get_value(), value_, value_size_);
  }

protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

private:
  KeyHash hash_;
  uint16_t psf_id_;
  uint32_t value_size_;
  const char* value_;
  uint32_t cnt, expected;
};

class JsonFullScanContext : public IAsyncContext {
public:
  JsonFullScanContext(const std::vector<std::string>& field_names,
    const general_psf_t<adapter_t>& pred, const char* value, uint32_t expected)
    : parser{ adapter_t::NewParser(field_names) },
    eval_{ pred },
    field_cnt{ field_names.size() },
    cnt{ 0 },
    expected{ expected },
    value_{ value },
    value_size_{ static_cast<uint32_t>(strlen(value)) } {}

  JsonFullScanContext(const JsonFullScanContext& other)
    : parser{ other.parser },
    eval_{ other.eval_ },
    value_size_{ other.value_size_ },
    cnt{ other.cnt },
    expected{ other.expected } {
    set_from_deep_copy();
    char* res = (char*)malloc(value_size_);
    memcpy(res, other.value_, value_size_);
    value_ = res;
  }

  inline void Touch(const char* payload, uint32_t payload_size) {
    ++cnt;
  }

  inline void Finalize() {
    ASSERT_EQ(cnt, expected);
  }

  inline bool check(const char* payload, uint32_t payload_size) {
    adapter_t::Load(parser.get(), payload, payload_size);
    auto& record = adapter_t::NextRecord(parser.get());
    tsl::hopscotch_map<uint16_t, typename adapter_t::field_t> field_map(field_cnt);
    for (auto& field : record.GetFields()) {
      field_map.emplace(static_cast<int16_t>(field.FieldId()), field);
    }
    std::vector<adapter_t::field_t> args;
    args.reserve(field_cnt);
    for (uint16_t i = 0; i < field_cnt; ++i) {
      auto it = field_map.find(i);
      if (it == field_map.end()) return false;
      args.emplace_back(it->second);
    }
    auto res = eval_(args);
    if (res.is_null) return false;
    bool pass = (res.size == value_size_ && !strncmp(res.payload, value_, value_size_));
    if (res.need_free) delete res.payload;
    return pass;
  }

protected:
  Status DeepCopy_Internal(IAsyncContext * &context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

private:
  std::shared_ptr<adapter_t::parser_t> parser;
  general_psf_t<adapter_t> eval_;
  size_t field_cnt;
  uint32_t cnt, expected;
  uint32_t value_size_;
  const char* value_;
};

TEST(InMemFishStore, Ingest_Serial) {
  store_t store{ 1LL << 24, 1LL << 30, "" };
  store.StartSession();
  auto id_proj = store.MakeProjection("id");
  auto gender_proj = store.MakeProjection("gender");
  std::vector<ParserAction> actions;
  actions.push_back({ REGISTER_GENERAL_PSF, id_proj });
  actions.push_back({ REGISTER_GENERAL_PSF, gender_proj });
  uint64_t safe_register_address, safe_unregister_address;
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);
  size_t cnt = 0;
  size_t op_cnt = 0;
  char buf[1024];
  for (size_t i = 0; i < n_records; ++i) {
    auto n = sprintf(buf, pattern, i, i, (i % 2) ? "male" : "female", i % 10, i % 10);
    cnt += store.BatchInsert(buf, n, 0);
    ++op_cnt;
    if (op_cnt % 256 == 0) store.Refresh();
  }

  auto callback = [](IAsyncContext* ctxt, Status result) {
    ASSERT_EQ(result, Status::Ok);
  };

  JsonGeneralScanContext context1{ id_proj, "1234", 1 };
  auto res = store.Scan(context1, callback, 0);
  ASSERT_EQ(res, Status::Ok);
  store.CompletePending();

  JsonGeneralScanContext context2{ gender_proj, "male", n_records / 2 };
  res = store.Scan(context2, callback, 0);
  ASSERT_EQ(res, Status::Ok);
  store.CompletePending();

  actions.clear();
  actions.push_back({ DEREGISTER_GENERAL_PSF, id_proj });
  actions.push_back({ DEREGISTER_GENERAL_PSF, gender_proj });
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  store.StopSession();
  ASSERT_EQ(cnt, n_records);
}

TEST(InMemFishStore, Ingest_Concurrent) {
  store_t store{ 1LL << 24, 1LL << 30, "" };
  store.StartSession();
  auto id_proj = store.MakeProjection("id");
  auto gender_proj = store.MakeProjection("gender");
  std::vector<ParserAction> actions;
  actions.push_back({ REGISTER_GENERAL_PSF, id_proj });
  actions.push_back({ REGISTER_GENERAL_PSF, gender_proj });
  uint64_t safe_register_address, safe_unregister_address;
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  std::atomic_size_t cnt{ 0 };
  std::vector<std::thread> thds;
  for (size_t i = 0; i < n_threads; ++i) {
    thds.emplace_back([&store, &cnt](size_t start) {
      store.StartSession();
      char buf[1024];
      size_t op_cnt = 0;
      for (size_t i = start; i < n_records; i += n_threads) {
        auto n = sprintf(buf, pattern, i, i, (i % 2) ? "male" : "female", i % 10, i % 10);
        cnt += store.BatchInsert(buf, n, 0);
        ++op_cnt;
        if (op_cnt % 256 == 0) store.Refresh();
      }
      store.StopSession();
    }, i);
  }

  for (auto& thd : thds) {
    thd.join();
  }

  ASSERT_EQ(cnt.load(), n_records);

  auto callback = [](IAsyncContext * ctxt, Status result) {
    ASSERT_EQ(result, Status::Ok);
  };

  JsonGeneralScanContext context1{id_proj, "1234", 1};
  auto res = store.Scan(context1, callback, 0);
  ASSERT_EQ(res, Status::Ok);
  store.CompletePending();

  JsonGeneralScanContext context2{ gender_proj, "male", n_records / 2 };
  res = store.Scan(context2, callback, 0);
  ASSERT_EQ(res, Status::Ok);
  store.CompletePending();

  actions.clear();
  actions.push_back({ DEREGISTER_GENERAL_PSF, id_proj });
  actions.push_back({ DEREGISTER_GENERAL_PSF, gender_proj });
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  store.StopSession();
}

TEST(InMemFishStore, FullScan) {
  store_t store{ 1LL << 24, 1LL << 30, "" };
  store.StartSession();
  auto id_proj = store.MakeProjection("id");
  auto gender_proj = store.MakeProjection("gender");
  std::vector<ParserAction> actions;
  actions.push_back({ REGISTER_GENERAL_PSF, id_proj });
  actions.push_back({ REGISTER_GENERAL_PSF, gender_proj });
  uint64_t safe_register_address, safe_unregister_address;
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  std::atomic_size_t cnt{ 0 };
  std::vector<std::thread> thds;
  for (size_t i = 0; i < n_threads; ++i) {
    thds.emplace_back([&store, &cnt](size_t start) {
      store.StartSession();
      char buf[1024];
      size_t op_cnt = 0;
      for (size_t i = start; i < n_records; i += n_threads) {
        auto n = sprintf(buf, pattern, i, i, (i % 2) ? "male" : "female", i % 10, i % 10);
        cnt += store.BatchInsert(buf, n, 0);
        ++op_cnt;
        if (op_cnt % 256 == 0) store.Refresh();
      }
      store.StopSession();
    }, i);
  }

  for (auto& thd : thds) {
    thd.join();
  }

  ASSERT_EQ(cnt.load(), n_records);

  auto callback = [](IAsyncContext * ctxt, Status result) {
    ASSERT_EQ(result, Status::Ok);
  };

  JsonFullScanContext context1{ {"id"}, fishstore::core::projection<adapter_t>, "1234", 1 };
  auto res = store.FullScan(context1, callback, 0);
  ASSERT_EQ(res, Status::Ok);
  store.CompletePending();

  JsonFullScanContext context2{ {"gender"}, fishstore::core::projection<adapter_t>, "male", n_records / 2 };
  res = store.FullScan(context2, callback, 0);
  ASSERT_EQ(res, Status::Ok);
  store.CompletePending();

  actions.clear();
  actions.push_back({ DEREGISTER_GENERAL_PSF, id_proj });
  actions.push_back({ DEREGISTER_GENERAL_PSF, gender_proj });
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  store.StopSession();
}

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}  
