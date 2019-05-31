# FishStore Tutorial

This document provides a tutorial of basic concepts and core API of FishStore. For running examples, please refer to code in [examples](examples) and [unit tests](tests).

# Predicated Subset Functions (PSFs)

A central concept in FishStore is *predicated subset function (PSF)*, which logically groups records withsimilar properties for later retrieval. Technically, given a data source of records in $R$, a PSF is a function $f: R \rightarrow D$ that maps valid records in $R$, based on a set of *field of interest* in $R$ to a specific value in domain D.

For example, the field projection function $\Pi_C(r)$ is a valid PSF that maps a record $r$ to the value of its field C. If $r$ does not contain field $C$ or its value for field C is `null`, we have $\Pi_C(r) =$ `null`.

Given a set of PSFs, a particular record may satisfy (i.e., have a non-`null` value for) serveral of them. We call thee the properies of the record. Formally, a record $r \in R$ is said to have property $(f, v)$, where $f$ is a PSF mapping $R$ to $D$ and $f(r) = v \in D$.

PSFs are implemented by users as functions with specific signatures inside dynamic linking libraries. For more details, please refer to [this document](examples/lib_examples/README.md) and [PSF library examples](examples/lib_examples).

# Construct FishStore

To construct fishstore, user need to specify several template arguments and store parameter. Specifically, a valid FishStore instance is of type:

```cpp
fishstore::core::FishStore<class disk_t, class adaptor_t>;
```

`disk_t` specifies what underlying I/O utilities FishStore will use. In our current version, we support two types of them:
- `fishstore::device::NullDisk` will discard any data spill out of memory, thus no data will be persisted onto disk. We use it mainly for test purpose.
- `fishstore::device::FileSystemDisk<class handler_t, uint64_t size>` will persist all data to a folder in the file system. Tempalte argument `handler_t` indicates what I/O handler FishStore ueses: we currently support a queue I/O handler for Linux/Windows, and a threadPool I/O handler for Windows. Tempalte argument `size` is the number of bytes FishStore will bundled in each log file.

`adaptor_t` specifies which parser adaptor FishStore will use. A parser adaptor help FishStore work with a specific parser so as to parse raw input text to fields. For more details about how to implement a parser adaptor, please refer to [this document](examples/adaptor_examples/README.md).

Below is an example for constructing a FishStore instance:

```cpp
typedef fishstore::environment::QueueIoHandler handler_t;
typedef fishstore::device::FileSystemDisk<handler_t, 1073741824LL> disk_t;
typedef fishstore::adaptor::SIMDJsonAdaptor adaptor_t;
using store_t = fishstore::core::FishStore<disk_t, adaptor_t>;

// FishStore constructor:
// FishStore(size_t hash_table_size, size_t in_mem_buffer_size, const std::string& store_dst);
store_t store {1LL << 24, 1LL << 31, "fishstore_data"};
```

It construct a FishStore instance which has a initial hash table size of $2^{24}$ hash entries, 2GB of in memory buffer, and persiting data to the directory `"fishstore_data"` using a queue I/O handler and bundling each 1GB persisted data into a file.

# PSF Loading and De/registration

In FishStore, user can load a set of PSFs from a dynamic linking library as below:

```cpp
uint64_t lib_id = store.LoadPSFLibrary("library_path");
```

If the library load successfully, FishStore will allocate a library ID for further reference. Once a PSF library loaded, we can register a PSF into FishStore's naming service by:

```cpp
uint16_t general_psf_id = store.MakeGeneralPSF({"field1", "field2"}, lib_id, "foo1");
uint32_t inline_psf_id = store.MakeInlinePSF({"field3", "field4"}, lib_id, "foo2");
```

User need to specify which fields you want to pass to the PSF, the library ID in which the PSF resides and its function name. For example, in the code example above, we registered a general PSF `foo1` defined in PSF library with ID `lib_id` over `field1` and `field2`. Similarly, we also registered an inline PSF `foo2` defiend in the same library over `field3` and `field4`.

The return value of a general PSF can be any size, while an inline PSF has a return value of 32 bit integer. Users need to guarantee the function signature of given PSF matches the API they call. For more details about composing PSF library, please refer to [this document](examples/lib_examples/README.md).

*Note that general PSFs and inline PSFs have separate name spaces, please do not confuse. Currently, we support up to $2^{32} - 1$ inline PSFs and $2^{16} - 1$ general PSFs.*

*Furthermore, FishStore will NOT recycle deregistered PSF IDs or do any deduplications.*

# Data Ingestion

# Subset Retrieval

# Checkpoint and Recovery