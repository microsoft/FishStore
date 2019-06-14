# FishStore Tutorial

This document provides a tutorial of basic concepts and core API of FishStore. For running examples, please refer to code in [examples](examples) and [unit tests](tests).

# Predicated Subset Functions (PSFs)

A central concept in FishStore is *predicated subset function (PSF)*, which logically groups records with similar properties for later retrieval. Technically, given a data source of records in R, a PSF is a function f: R -> D that maps valid records in R, based on a set of *field of interest* in R to a specific value in domain D.

For example, the field projection function π<sub>C</sub>(r) is a valid PSF that maps a record r to the value of its field C. If r does not contain field C or its value for field C is `null`, we have π<sub>C</sub>(r) = `null`.

Given a set of PSFs, a particular record may satisfy (i.e., have a non-`null` value for) serveral of them. We call these the properies of the record. Formally, a record r ∈ R is said to have property (f, v), where f is a PSF mapping R to D and f(r) = v ∈ D.

PSFs are implemented by users as functions with specific signatures inside dynamic link libraries (DLLs). For details, please refer to [this document](examples/lib_examples/README.md) and [PSF library examples](examples/lib_examples).

# Construct FishStore

To construct fishstore, the user needs to specify several template arguments and store parameters. Specifically, a valid FishStore instance is of type:

```cpp
fishstore::core::FishStore<class disk_t, class adapter_t>;
```

`disk_t` specifies what underlying I/O utilities FishStore will use. In our current version, we support two types of disks:
- `fishstore::device::NullDisk` will discard any data that spills out of memory, thus no data will be persisted onto disk. We use it mainly for test purposes.

- `fishstore::device::FileSystemDisk<class handler_t, uint64_t size>` will persist all data to a folder in the file system. Template argument `handler_t` indicates what I/O handler FishStore uses: we currently support a queue I/O handler for Linux/Windows, and a threadPool I/O handler for Windows. Tempalte argument `size` is the number of bytes FishStore will bundled in each log file.

`adapter_t` specifies which parser adapter FishStore will use. A parser adapter helps FishStore work with a specific parser so as to parse raw input text to fields. For more details about how to implement a parser adapter, please refer to [this document](src/adapters/README.md).

**In our current implementation, we provide a parser adapter based on [simdjson](https://github.com/lemire/simdjson) to handle general JSON ingestion. However, it is not perfect. We list known limitations [here](src/adapters/README.md#Example).**

Below is an example for constructing a FishStore instance:

```cpp
typedef fishstore::environment::QueueIoHandler handler_t;
typedef fishstore::device::FileSystemDisk<handler_t, 1073741824LL> disk_t;
typedef fishstore::adapter::SIMDJsonAdapter adapter_t;
using store_t = fishstore::core::FishStore<disk_t, adapter_t>;

// FishStore constructor:
// FishStore(size_t hash_table_size, size_t in_mem_buffer_size, const std::string& store_dst);
store_t store {1LL << 24, 1LL << 31, "fishstore_data"};
```

It constructs a FishStore instance which has an initial hash table size of 2<sup>24</sup> hash entries, 2GB of in-memory buffer, using simdjson for parsing, and persisting data to the directory `"fishstore_data"` using a queue I/O handler and bundling each 1GB of persisted data into a file.

Once the FishStore instance is constructed, the user can use the following interfaces to start or stop a session on a thread:

```cpp
Guid session_id = store.StartSession();
store.StopSession();
```

**FishStore will only provide guarantees on threads registered as a session. So, please make sure to start a session before interacting with a FishStore instance on a thread.**

# PSF Loading and De/registration

Before registering a PSF in FishStore, the user needs to load the corresponding PSF library and ask FishStore to assign the PSF an ID using its naming service. Specifically, the user can load a set of PSFs from a dynamic link library (DLL) as below:

```cpp
uint64_t lib_id = store.LoadPSFLibrary("library_path");
```

If the library loads successfully, FishStore will allocate a unique library ID for further reference. Once a PSF library is loaded, we can register PSFs into FishStore's naming service as follows:

```cpp
uint16_t general_psf_id = store.MakeGeneralPSF({"field1", "field2"}, lib_id, "foo1");
uint32_t inline_psf_id = store.MakeInlinePSF({"field3", "field4"}, lib_id, "foo2");
```

User need to specify which fields you want to pass to the PSF, the library ID in which the PSF resides and its function name. For example, in the code example above, we registered a general PSF `foo1` defined in PSF library with ID `lib_id` over `field1` and `field2`. Similarly, we also registered an inline PSF `foo2` defined in the same library over `field3` and `field4`.

We also provide a shortcut for registering a field projection PSF (as a general PSF):

```cpp
uint16_t projection_psf_id = store.MakeProjection("proj_field");
```

The return value of a general PSF can be any size, while an inline PSF has a return value of 32 bit integer. Users need to ensure that the function signature of a given PSF matches the API they call. For more details about composing PSF library, please refer to [this document](examples/lib_examples/README.md).

*Note that general PSFs and inline PSFs have separate name spaces, and should not be confused. Currently, we support up to 2<sup>32</sup> - 1 inline PSFs and 2<sup>16</sup> - 1 general PSFs. Furthermore, FishStore will NOT recycle deregistered PSF IDs or do any deduplications.*

With the PSF ID allocated, user can register and deregister PSFs in batches using the following interface:

```cpp
std::vector<ParserAction> parser_actions;
parser_actions.push_back({ REGISTER_GENERAL_PSF, id_proj });
parser_actions.push_back({ REGISTER_GENERAL_PSF, actor_id_proj });
parser_actions.push_back({ DEREGISTER_GENERAL_PSF, repo_id_proj });
parser_actions.push_back({ REGISTER_GENERAL_PSF, type_proj });
parser_actions.push_back({ REGISTER_INLINE_PSF, predicate1_id });
parser_actions.push_back({ DEREGISTER_INLINE_PSF, predicate2_id });

uint64_t safe_register_address, safe_unregister_address;
safe_unregister_address = store.ApplyParserShift(
parser_actions, [&safe_register_address](uint64_t safe_address) {
    safe_register_address = safe_address;
});

store.CompleteAction(true);
```

User can push all the de/registration actions in a single vector and call `ApplyParserShift` to apply all of them at once. `ApplyParserShift` will return a safe unregister address synchronously indicating the address up to which FishStore gurantees records are still fully indexed on requested PSF deregistrations. The safe register boundary is provided asynchronously through the callback, which provides the starting address where FishStore started indexing fully on requested PSF registrations.

`store.CompleteAction(true)` will stall the issuing thread until the requested actions are done. In contrast, `store.CompleteAction()` will check if the request is ongoing. It will return `false` if the action is still pending, and otherwise return `true`.

*Both the naming service and the PSF de/registration service are thread safe. Users can call them on any valid FishStore session with the guarantees being propagated to all sessions.*

# Data Ingestion

User can ingest a batch of records from raw text as follows:

```cpp
FishStore<D, A>::BatchInsert(const char* payload, size_t length, uint64_t monotomic_serial_num, uint32_t offset = 0);
FishStore<D, A>::BatchInsert(const std::string& payload, uint64_t monotomic_serial_num, uint32_t offset = 0);
```

In particular, the user can pass in a batch of records in raw text, provide a thread-local monotomic serial number for checkpointing purpose, and a offset indicating the offset in the payload where parser should start parsing. To make sure FishStore synchronizes multiple sessions properly, the user also need to periodically refresh its own session using `store.Refresh()`.

For example, user can ingest all raw-text-form records in a vector of string `batches` as below:

```cpp
uint64_t serial_num = 0;
for (const std::string& batch : batches) {
    store.BatchInsert(batch, serial_num);
    ++serial_num;
    if (serial_num % 256 == 0) store.Refresh();
}
```

# Subset Retrieval

Users are provided with two methods for retrieving a subset from ingested data, namely, index scan and full scan.


## Index Scan

For index scan, given a property $(f, v)$ over a registered PSF $f$, FishStore will touch all indexed record within an optional address range. In order to define customized logic, user need to define a scan context fulfilling the following interface:

```cpp
class ScanContext : public IAsyncContext {
public:
  ScanContext(const ScanContext& other);
  inline void Touch(const char* payload, uint32_t payload_size);
  inline void Finalize();
  inline fishstore::core::KeyHash get_hash() const;
  inline bool check(const fishstore::core::KeyPointer* kpt);
protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final; 
};
```

In particular, `get_hash()` returns the hash signature of a property. For a property based on a general PSF with ID `psf_id` and value `value` of length `value_size`, its hash signature is evaluated as:
```cpp
fishstore::core::Utility::HashBytesWithPSFID(psf_id, value, value_size);
```

For a property based on a inline PSF with ID `psf_id` and an integer value `value`, its hash signature is evaluated as:
```cpp
fishstore::core::Utility::GetHashCode(psf_id, value);
```

Function `check` is used to double check if a record is visited because of a hash collision. The standard implementation for a general PSF based property is:

```cpp
inline bool check(const core::KeyPointer* kpt) {
    return kpt->mode == 0 && kpt->general_psf_id == psf_id &&
           kpt->value_size == value_size &&
           !memcmp(kpt->get_value(), value, value_size);
}
```

while the standard implementation for an inline PSF based property is:

```cpp
inline bool check(const core::KeyPointer* kpt) {
    return kpt->mode == 1 &&
           kpt->inline_psf_id == psf_id &&
           kpt->value == value;
}
```

FishStore will call the `Touch()` funcion on each satisfying record, and call `Finalize()` function upon fishishing the scan.

*Contexts passed to FishStore will be deep copied using their copy constructor when corresponding operations go aysnc.* Thus, user is responsible to implement a `DeepCopy_Internal()` function. Usually, user can use a common pattern as below if the copy constructor is properly defined:

```cpp
Status DeepCopy_Internal(IAsyncContext*& context_copy) final {
  return IAsyncContext::DeepCopy_Internal(*this, context_copy);
}
```

To avoid memory management issues, please be careful when implementing copy constructor and deconstrutors. Further, use can call `from_deep_copy()` interited from IAsyncContext to determin whether the current instance is constucted from a deep copy.


To lauch a scan on FishStore, user should use the following API:

```cpp
Status res = store.Scan(context, callback, serial_num, start_address, end_address);
```

In particular, `context` is an instance of scan context class described as above, `serial_num` is a thread-local monotical serial number (similar as that in `BatchInsert`). `callback` is the function to be called after all asyncrous I/Os handing back the async context and ending status. It should be defined in the following type:

```cpp
typedef void(*AsyncCallback)(IAsyncContext* ctxt, Status result);
```

FishStore will only return records reside on its log between `start_address` and `end_address`. If these addresses are not specified, FishStore will scan the whole log. As return value, `Scan()` will return `Status::Ok` if the scan ends before landing on disk, otherwise `Status::Pending` is returned.

User can call `store.CompletePending(true)` to stall the current thread to wait until all pending requets to complete. Otherwise, user can call `store.CompletePending()` to check if all pending requests are completed.

## Full Scan

The other type of record retrieval FishStore supports is full scan. A full scan will check all record one by one wihtin the search range against a given check function. Specifically, user need to define a full scan context fulfilling the following interface:

```cpp
class FullScanContext : public IAsyncContext {
public:
  FullScanContext(const FullScanContext& other);
  inline void Touch(const char* payload, uint32_t payload_size);
  inline void Finalize();
  inline bool check(const char* payload, uint32_t payload_size);
protected:
  Status DeepCopy_Internal(IAsyncContext*& context_copy) final; 
};
```

The only difference is that `check()` function is directly apply on a payload rather than `fishstore::core::KeyPointer`. To lauch a full scan, FishStore provides a similar API as index scan:

```cpp
Status res = store.FullScan(context, callback, serial_num, start_address, end_address);
```

FishStore is equipped with adaptive prefetching. User can fine tune their IO levels and adaptive threshold in [`src/core/constants.h`](src/core/constants.h). Another important parameter is the average size of each record, which can be set dynamically using:

```cpp
fishstore::core::Constants::SetAvgRecordSize(avg_record_size);
```

# Checkpoint and Recovery

FishStore supports a simplified version of the concurrent prefix recovery (CPR) model used in FASTER. For more details on the CPR model, please refer to [this paper](https://www.microsoft.com/en-us/research/uploads/prod/2019/01/cpr-sigmod19.pdf).

FishStore can checkpoint its hash table and log together or separately. Checkpointing is done asynchronously with a callback and uniquely identify with a Guid. Below are checkpointing examples for log, index (hash table), and both together:

```cpp
auto index_checkpoint_callback = [](Status result) {
      if (result == Status::Ok) {
        printf("Index Checkpoint successful!!\n");
      } else
        printf("Index Checkpoint failed...\n");
    };

auto log_persistence_callback =
  [](Status result, uint64_t persistent_serial_num, uint32_t persistent_offset) {
      if (result == Status::Ok) { 
        printf("Thread %u finish checkpointing with serial_num %zu offset %u...\n",
                Thread::id(), persistent_serial_num, persistent_offset);
      } else printf("Thread %u failed in log checkpointing...\n");
    };

Guid index_token;
store.CheckpointIndex(index_checkpoint_callback, index_token);
store.CompleteAction(true);

Guid log_token;
store.CheckpointHybridLog(log_persistence_callback, log_token);
store.CompleteAction(true);

Guid unified_token;
store.Checkpoint(index_checkpoint_callback, log_persistent_callback, unified_token);
store.ComleteAction(true);
```

`index_checkpoint_callback` is the callback that FishStore calls after finish checkpointing its hash index, while `log_persistence_callback` is the callback that each running FishStore session calls after the log checkpointing. When the log checkpointing is done, FishStore will returns a serial number $s$ and an offset $o$ to each running session, indicating up to location $o$ of raw data in operation $s$ is successfully persisted.

When recover from a checkpoint, FishStore asks user to supply two tokens: one for the log and one for the index. To ensure the revoery is correct, user need to ensure the provided index checkpoint happened no later than the log checkpoint. The recovery of a FishStore instance can be done by calling:

```cpp
std::vector<Guid> recovered_session_ids;
uint32_t version
Status res = store.Recover(index_token, log_token, version, recovered_session_ids);
```

Other than the recover status, `Recover()` also returns the internal version number and the running session IDs when the log checkpoint is performed. To recover an individual session, user can use the following interface:

```cpp
std::tie(serial_num, offset) = store.ContinueSession(session_id);
```

By continuing a session, user can retrieve the serial number and ingestion offset at checkpointing time. With the help of this information, user can continue a running session from a consistent point without causing any duplications or data loss.
