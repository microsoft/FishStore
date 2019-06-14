// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdio>
#include <experimental/filesystem>
#include <fstream>

#define _NULL_DISK

#include "adapters/simdjson_adapter.h"
#include "device/null_disk.h"
#include "core/fishstore.h"

typedef fishstore::environment::QueueIoHandler handler_t;
typedef fishstore::device::NullDisk disk_t;
typedef fishstore::adapter::SIMDJsonAdapter adapter_t;
using store_t = fishstore::core::FishStore<disk_t, adapter_t>;

std::unordered_map<std::string, uint16_t> field_ids;

class JsonGeneralScanContext : public IAsyncContext {
public:
  JsonGeneralScanContext(uint16_t psf_id, const char* value)
    : hash_{ fishstore::core::Utility::HashBytesWithPSFID(psf_id, value, strlen(value)) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(strlen(value)) },
    value_{ value },
    cnt{ 0 } {
  }

  JsonGeneralScanContext(uint16_t psf_id, const std::string& value)
    :hash_{ fishstore::core::Utility::HashBytesWithPSFID(psf_id, value.c_str(), value.length()) },
    psf_id_{ psf_id },
    value_size_{ static_cast<uint32_t>(value.length()) },
    value_{ value.c_str() },
    cnt{ 0 }{}

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
  }

  inline void Touch(const char* payload, uint32_t payload_size) {
    // printf("Record Hit: %.*s\n", payload_size, payload);
    ++cnt;
  }

  inline void Finalize() {
    printf("%u record has been touched...\n", cnt);
  }

  inline fishstore::core::KeyHash get_hash() const {
    return hash_;
  }

  inline bool check(const fishstore::core::KeyPointer* kpt) {
    return kpt->mode == 0 && kpt->general_psf_id == psf_id_ &&
      kpt->value_size == value_size_ &&
      !memcmp(kpt->get_value(), value_, value_size_);
  }

protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

private:
  fishstore::core::KeyHash hash_;
  uint16_t psf_id_;
  uint32_t value_size_;
  const char* value_;
  uint32_t cnt;
};

class JsonInlineScanContext : public IAsyncContext {
public:
  JsonInlineScanContext(uint32_t psf_id, int32_t value)
    : psf_id_(psf_id), value_(value), cnt(0) {}

  inline void Touch(const char* payload, uint32_t payload_size) {
    // printf("Record Hit: %.*s\n", payload_size, payload);
    ++cnt;
  }

  inline void Finalize() {
    printf("%u record has been touched...\n", cnt);
  }

  inline fishstore::core::KeyHash get_hash() const {
    return fishstore::core::KeyHash{ fishstore::core::Utility::GetHashCode(psf_id_, value_) };
  }

  inline bool check(const fishstore::core::KeyPointer* kpt) {
    return kpt->mode == 1 && kpt->inline_psf_id == psf_id_ && kpt->value == value_;
  }

protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) {
    return IAsyncContext::DeepCopy_Internal(*this, context_copy);
  }

private:
  uint32_t psf_id_;
  int32_t value_;
  uint32_t cnt;
};

void SetThreadAffinity(size_t core) {
  // Assume 28 cores.
  constexpr size_t kCoreCount = 36;  // JDH DEBUG
#ifdef _WIN32
  HANDLE thread_handle = ::GetCurrentThread();
  ::SetThreadAffinityMask(thread_handle, (uint64_t)0x1 << core);
#else
  // On MSR-SANDBOX-049, we see CPU 0, Core 0 assigned to 0, 28;
  //                            CPU 1, Core 0 assigned to 1, 29; etc.
  cpu_set_t mask;
  CPU_ZERO(&mask);
#ifdef NUMA
  switch (core % 4) {
  case 0:
    // 0 |-> 0
    // 4 |-> 2
    // 8 |-> 4
    core = core / 2;
    break;
  case 1:
    // 1 |-> 28
    // 5 |-> 30
    // 9 |-> 32
    core = kCoreCount + (core - 1) / 2;
    break;
  case 2:
    // 2  |-> 1
    // 6  |-> 3
    // 10 |-> 5
    core = core / 2;
    break;
  case 3:
    // 3  |-> 29
    // 7  |-> 31
    // 11 |-> 33
    core = kCoreCount + (core - 1) / 2;
    break;
  }
#else
  switch (core % 2) {
  case 0:
    // 0 |-> 0
    // 2 |-> 2
    // 4 |-> 4
    core = core;
    break;
  case 1:
    // 1 |-> 28
    // 3 |-> 30
    // 5 |-> 32
    core = (core - 1) + kCoreCount;
    break;
  }
#endif
  CPU_SET(core, &mask);

  ::sched_setaffinity(0, sizeof(mask), &mask);
#endif
}

int main(int argc, char* argv[]) {
  if (argc != 5) {
    printf("Usage: ./online_demo <input_file> <n_threads> <memory_buget> <store_target>\n");
    return -1;
  }

  int n_threads = atoi(argv[2]);
  std::ifstream fin(argv[1]);
  std::vector<std::string> batches;
  const uint32_t json_batch_size = 1;
  uint32_t json_batch_cnt = 0;
  size_t line_cnt = 0;
  size_t record_cnt = 0;
  std::string line;
  while (std::getline(fin, line)) {
    if (json_batch_cnt == 0 || line_cnt == json_batch_size) {
      line_cnt = 1;
      ++json_batch_cnt;
      batches.push_back(line);
    }
    else {
      batches.back() += line;
      line_cnt++;
    }
    ++record_cnt;
  }

  uint32_t batch_size = json_batch_cnt / n_threads + 1;

  printf("Finish loading %u batches (%zu records) of json into the memory....\n",
    json_batch_cnt, record_cnt);

  std::experimental::filesystem::create_directory(argv[4]);
  size_t store_size = 1LL << atoi(argv[3]);
  store_t store{ (1L << 24), store_size, argv[4] };

  printf("Finish initializing FishStore, starting the demo...\n");
  store.StartSession();

  bool finish = false;
  std::vector<std::thread> thds;
  std::atomic_uint64_t bytes_ingested{ 0 };
  std::atomic_uint32_t record_ingested{ 0 };
  auto worker_thd = [&](int thread_no) {
    SetThreadAffinity(thread_no);
    store.StartSession();
    size_t begin_line = thread_no;
    size_t batch_end = batches.size();
    auto callback = [](IAsyncContext * ctxt, Status result) {
      assert(false);
    };
    size_t cnt = 0;
    while (!finish) {
      for (size_t i = begin_line; i < batch_end; i += n_threads) {
        auto res = store.BatchInsert(batches[i], 1);
        cnt += res;
        if (cnt % 256 == 0) {
          store.Refresh();
          cnt = 0;
        }
        bytes_ingested.fetch_add(batches[i].size());
        record_ingested.fetch_add(res);
        if (finish) break;
      }
    }

    store.CompletePending(true);
    store.StopSession();
  };

  for (int i = 0; i < n_threads; ++i) {
    thds.emplace_back(std::thread(worker_thd, i));
  }

  double current_throughput = 0.0;
  std::thread timer([&n_threads, &finish, &bytes_ingested, &record_ingested, &current_throughput]() {
    SetThreadAffinity(n_threads + 1);
    uint64_t last_bytes = 0;
    uint32_t last_records = 0;
    while (!finish) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      uint64_t current_bytes = bytes_ingested.load();
      uint32_t current_records = record_ingested.load();
      current_throughput = (double)(current_bytes - last_bytes) / 1024.0 / 1024.0;
      last_bytes = current_bytes;
      last_records = current_records;
    }
  });

  static auto callback = [](IAsyncContext * ctxt, Status result) {
    assert(result == Status::Ok);
  };

  while (true) {
    SetThreadAffinity(n_threads);
    std::vector<ParserAction> parser_actions;
    uint64_t safe_register_address, safe_unregister_address;

    printf(">>");
    std::string op;
    std::cin >> op;
    if (op == "reg-field") {
      std::string field_name;
      std::cin >> field_name;
      auto it = field_ids.find(field_name);
      uint16_t field_id;
      if (it == field_ids.end()) {
        field_id = store.MakeProjection(field_name);
        field_ids[field_name] = field_id;
      }
      else field_id = it->second;
      parser_actions.clear();
      parser_actions.push_back({ REGISTER_GENERAL_PSF, field_id });
      safe_unregister_address = store.ApplyParserShift(
        parser_actions, [&safe_register_address](uint64_t safe_address) {
          safe_register_address = safe_address;
        });
      store.CompleteAction(true);
      printf("Finish registering field projection PSF on field `%s`, start indexing from address %zu...\n",
        field_name.c_str(), safe_register_address);
    }
    else if (op == "dereg-field") {
      std::string field_name;
      std::cin >> field_name;
      auto it = field_ids.find(field_name);
      if (it == field_ids.end()) {
        printf("FIeld %s has never been registered...\n", field_name.c_str());
        continue;
      }
      uint16_t field_id = it->second;
      parser_actions.clear();
      parser_actions.push_back({ DEREGISTER_GENERAL_PSF, field_id });
      safe_unregister_address = store.ApplyParserShift(
        parser_actions, [&safe_register_address](uint64_t safe_address) {
          safe_register_address = safe_address;
        });
      store.CompleteAction(true);
      printf("Finish deregistering field projection PSF on field %s, stop indexing from address %zu...\n",
        field_name.c_str(), safe_unregister_address);
    }
    else if (op == "load-lib") {
      std::string lib_path;
      std::cin >> lib_path;
      size_t lib_id = store.LoadPSFLibrary(lib_path);
      if (lib_id != -1) {
        printf("Finish Loading PSF library from path %s with LibraryID %zu...\n", lib_path.c_str(), lib_id);
      }
    }
    else if (op == "reg-filter") {
      std::string func_name;
      size_t lib_id, field_cnt;
      std::cin >> lib_id >> func_name >> field_cnt;
      std::vector<std::string> fields;
      for (size_t i = 0; i < field_cnt; ++i) {
        std::string field_name;
        std::cin >> field_name;
        fields.emplace_back(field_name);
      }
      uint32_t psf_id = store.MakeInlinePSF(fields, lib_id, func_name);
      if (psf_id == -1) continue;
      parser_actions.clear();
      parser_actions.push_back({ REGISTER_INLINE_PSF, psf_id });
      safe_unregister_address = store.ApplyParserShift(
        parser_actions, [&safe_register_address](uint64_t safe_address) {
          safe_register_address = safe_address;
        });
      store.CompleteAction(true);
      printf("Finish registering Inline PSF `%s` from library %zu with filter ID %u, start indexing from address %zu...\n",
        func_name.c_str(), lib_id, psf_id, safe_register_address);
    }
    else if (op == "rereg-filter") {
      uint32_t psf_id;
      std::cin >> psf_id;
      parser_actions.clear();
      parser_actions.push_back({ REGISTER_INLINE_PSF, psf_id });
      safe_unregister_address = store.ApplyParserShift(
        parser_actions, [&safe_register_address](uint64_t safe_address) {
        safe_register_address = safe_address;
      });
      store.CompleteAction(true);
      printf("Finish registering filter PSF %u, start indexing from address %zu...\n",
        psf_id, safe_unregister_address);
    }
    else if (op == "dereg-filter") {
      uint32_t psf_id;
      std::cin >> psf_id;
      parser_actions.clear();
      parser_actions.push_back({ DEREGISTER_INLINE_PSF, psf_id });
      safe_unregister_address = store.ApplyParserShift(
        parser_actions, [&safe_register_address](uint64_t safe_address) {
          safe_register_address = safe_address;
        });
      store.CompleteAction(true);
      printf("Finish deregistering filter PSF %u, stop indexing from address %zu...\n",
        psf_id, safe_unregister_address);
    }
    else if (op == "scan") {
      std::string type;
      std::cin >> type;
      if (type == "filter") {
        uint32_t psf_id;
        std::cin >> psf_id;
        JsonInlineScanContext context{ psf_id, 1 };
        auto begin = std::chrono::high_resolution_clock::now();
        auto status = store.Scan(context, callback, 1);
        store.CompletePending(true);
        auto end = std::chrono::high_resolution_clock::now();
        printf("Finish index scan on filter #%u in %.6f sec...\n", psf_id,
          std::chrono::duration<double>(end - begin).count());
      }
      else if (type == "field") {
        std::string field_name, value;
        std::cin >> field_name >> value;
        auto it = field_ids.find(field_name);
        if (it == field_ids.end()) {
          printf("FIeld %s has never been registered...\n", field_name.c_str());
          continue;
        }
        uint16_t field_id = it->second;
        JsonGeneralScanContext context{ field_id, value };
        auto begin = std::chrono::high_resolution_clock::now();
        auto status = store.Scan(context, callback, 1);
        store.CompletePending(true);
        auto end = std::chrono::high_resolution_clock::now();
        printf("Finish index scan on field `%s == %s` in %.6f sec...\n",
          field_name.c_str(), value.c_str(), std::chrono::duration<double>(end - begin).count());
      }
      else {
        printf("Scan type need to be either field or filter...\n");
        std::string line;
        std::getline(std::cin, line);
      }
    }
    else if (op == "print-throughput") {
      printf("Current ingestion throughput is %.6f MB/s\n", current_throughput);
    }
    else if (op == "print-reg") {
      store.PrintRegistration();
    }
    else if (op == "exit") {
      printf("Stopping the demo...\n");
      break;
    }
    else if (op == "help") {
      printf(
        "Possible Commands:\n"
        "reg-field <field_name>                                       Register a field projection PSF on <field_name>.\n"
        "dereg-field <field_name>                                     Deregister a field projection PSF on <field_name>.\n"
        "load-lib <path>                                              Load a PSF Library from <path>.\n"
        "reg-filter <lib_id> <func_name> <n_fields> <fields>...       Register a filter PSF <func_name> in Library <lib_id> defined over <fields>.\n"
        "rereg-filter <filter_id>                                     Reregister a filter PSF <func_name> in Library <lib_id> defined over <fields>.\n"
        "dereg-filter <filter_id>                                     Deregister the filter PSF with ID <filter_id>.\n"
        "scan filter <filter_id>                                      Do an index scan over Filter PSF #<filter_id>.\n"
        "scan field <field_name> <value>                              Do an index scan over field <field_name> (need to be registered) over <value>.\n"
        "print-throughput                                             Print the current ingestion throughput of FishStore.\n"
        "print-reg                                                    Print the current FishStore registration.\n"
        "exit                                                         Stop the demo.\n"
        "help                                                         Show this message\n");
    }
    else {
      printf("Invalid command %s...\n", op.c_str());
      std::getline(std::cin, line);
    }
  }

  finish = true;
  for (int i = 0; i < n_threads; ++i) {
    thds[i].join();
  }
  timer.join();

  store.StopSession();

  return 0;
}