// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdint>
#include <string>
#include <vector>
#include <experimental/filesystem>
#include "gtest/gtest.h"

#include "adapters/simdjson_adapter.h"
#include <device/file_system_disk.h>
#include "core/fishstore.h"

using handler_t = fishstore::environment::QueueIoHandler;

using namespace fishstore::core;
using adapter_t = fishstore::adapter::SIMDJsonAdapter;
using disk_t = fishstore::device::FileSystemDisk<handler_t, 33554432L>;
using store_t = FishStore<disk_t, adapter_t>;

const size_t n_records = 1500000;
const size_t n_threads = 4;
const char* pattern =
  "{\"id\": \"%zu\", \"name\": \"name%zu\", \"gender\": \"%s\", \"school\": {\"id\": \"%zu\", \"name\": \"school%zu\"}}";

static uint32_t expected_value = 0;

class JsonGeneralScanContext : public IAsyncContext {
public:
  JsonGeneralScanContext(uint16_t psf_id, const char* value)
    : hash_{ Utility::HashBytesWithPSFID(psf_id, value, strlen(value)) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(strlen(value)) },
    value_{ value },
    cnt{ new uint32_t{ 0 } } {
  }

  JsonGeneralScanContext(uint16_t psf_id, const char* value, size_t length)
    : hash_{ Utility::HashBytesWithPSFID(psf_id, value, length) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(length) },
    value_{ value },
    cnt{ new uint32_t{ 0 } } {
  }

  JsonGeneralScanContext(const JsonGeneralScanContext& other)
    : hash_{ other.hash_ },
    psf_id_{ other.psf_id_ },
    value_size_{ other.value_size_ },
    cnt{ other.cnt } {
    set_from_deep_copy();
    char* res = (char*)malloc(value_size_);
    memcpy(res, other.value_, value_size_);
    value_ = res;
  }

  ~JsonGeneralScanContext() {
    if (from_deep_copy()) free((void*)value_);
    delete cnt;
  }

  inline void Touch(const char* payload, uint32_t payload_size) {
    ++(*cnt);
  }

  inline void Finalize() {
  }

  inline uint32_t get_cnt() {
    return *cnt;
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
  uint32_t* cnt;
};

class JsonFullScanContext : public IAsyncContext {
public:
  JsonFullScanContext(const std::vector<std::string>& field_names,
    const general_psf_t<adapter_t>& pred, const char* value)
    : eval_{ pred },
    cnt{ new uint32_t{0} },
    value_{ value },
    value_size_{ static_cast<uint32_t>(strlen(value)) },
    field_names{field_names},
    parser{field_names} {
  }

  JsonFullScanContext(const JsonFullScanContext& other)
    : field_names{ other.field_names },
    eval_{ other.eval_ },
    value_size_{ other.value_size_ },
    cnt{ other.cnt },
    parser{other.field_names} {
    set_from_deep_copy();
    char* res = (char*)malloc(value_size_);
    memcpy(res, other.value_, value_size_);
    value_ = res;
  }

  ~JsonFullScanContext() {
    if (from_deep_copy()) {
      free((void*)value_);
    }
    delete cnt;
  }

  inline void Touch(const char* payload, uint32_t payload_size) {
    ++(*cnt);
  }

  inline void Finalize() {
  }

  inline uint32_t get_cnt() {
    return *cnt;
  }

  inline bool check(const char* payload, uint32_t payload_size) {
    parser.Load(payload, payload_size);
    auto& record = parser.NextRecord();
    tsl::hopscotch_map<uint16_t, typename adapter_t::field_t> field_map(field_names.size());
    for (auto& field : record.GetFields()) {
      field_map.emplace(static_cast<int16_t>(field.FieldId()), field);
    }
    std::vector<adapter_t::field_t> args;
    args.reserve(field_names.size());
    for (uint16_t i = 0; i < field_names.size(); ++i) {
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
  adapter_t::parser_t parser;
  std::vector<std::string> field_names;
  general_psf_t<adapter_t> eval_;
  uint32_t* cnt;
  uint32_t value_size_;
  const char* value_;
};

TEST(Registration, Register_Concurrent) {
  std::experimental::filesystem::remove_all("test");
  std::experimental::filesystem::create_directories("test");
  store_t store{ 8192, 201326592, "test" };
  store.StartSession();
  auto school_id_proj = store.MakeProjection("school.id");
  std::vector<ParserAction> actions;
  actions.push_back({ REGISTER_GENERAL_PSF, school_id_proj });
  
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

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  uint64_t safe_register_address, safe_unregister_address;
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
    safe_register_address = safe_address;
  });

  store.CompleteAction(true);

  for (auto& thd : thds) {
    thd.join();
  }

  ASSERT_EQ(cnt, n_records);

  auto callback = [](IAsyncContext * ctxt, Status result) {
    ASSERT_EQ(result, Status::Ok);
  };

  JsonGeneralScanContext context1{school_id_proj, "1"};
  auto res = store.Scan(context1, callback, 0, safe_register_address);
  store.CompletePending(true);

  JsonFullScanContext context2{ {"school.id"}, fishstore::core::projection<adapter_t>, "1" };
  res = store.FullScan(context2, callback, 0, safe_register_address);
  store.CompletePending(true);

  ASSERT_EQ(context1.get_cnt(), context2.get_cnt());

  actions.clear();
  actions.push_back({ DEREGISTER_GENERAL_PSF, school_id_proj });
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
      safe_register_address = safe_address;
    });

  store.CompleteAction(true);

  store.StopSession();
}

TEST(Registration, Deregister_Concurrent) {
  std::experimental::filesystem::remove_all("test");
  std::experimental::filesystem::create_directories("test");
  store_t store{ 8192, 201326592, "test" };
  store.StartSession();
  auto school_id_proj = store.MakeProjection("school.id");
  std::vector<ParserAction> actions;
  actions.push_back({ REGISTER_GENERAL_PSF, school_id_proj });
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

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  actions.clear();
  actions.push_back({ DEREGISTER_GENERAL_PSF, school_id_proj });
  safe_unregister_address = store.ApplyParserShift(
    actions, [&safe_register_address](uint64_t safe_address) {
    safe_register_address = safe_address;
  });
  store.CompleteAction(true);
  

  for (auto& thd : thds) {
    thd.join();
  }

  ASSERT_EQ(cnt, n_records);

  auto callback = [](IAsyncContext* ctxt, Status result) {
    ASSERT_EQ(result, Status::Ok);
  };

  JsonGeneralScanContext context1{ school_id_proj, "1" };
  auto res = store.Scan(context1, callback, 0, 0, safe_unregister_address);
  store.CompletePending(true);

  JsonFullScanContext context2{ {"school.id"}, fishstore::core::projection<adapter_t>, "1" };
  res = store.FullScan(context2, callback, 0, 0, safe_unregister_address);
  store.CompletePending(true);

  ASSERT_EQ(context1.get_cnt(), context2.get_cnt());

  store.StopSession();
}


int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
