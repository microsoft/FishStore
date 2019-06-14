# Composing PSF Libraries

FishStore allows user to compose their own PSF libraries, register and deregister them on the fly. A PSF is a function that maps records to a value domain D. For a record r, we say it satisfy property (f, v) if and only if f(r) = v. For each registered PSF, FishStore will index all records whose f(r) does not evaluate to `null`.

To compose PSF libraries, user need to define their own PSF functions and compile them in a **dynamic linking library** (i.e., `dll` in Windows and `.so` in Linux/Unix). Generally, FishStore supports two types of PSFs, namely, General PSFs and Inline PSFs. This document will cover how user should define them in C++.

# General PSFs
A general PSF maps a set of record fields to a bag of bytes which is represented  as `NullableStringRef` under `fishstore::core` scope:

```cpp
struct NullableStringRef {
  bool is_null = true;
  bool need_free = false;
  uint32_t size = 0;
  const char* payload = nullptr;
};
```
Specifically, `is_null` indicates if this is a `null` value, and `need_free` tells FishStore whether it requires be freed to avoid memory leakage.

Given a particular parser adapter `adapter_t`, a general PSF should be defined in the following function signature:

```cpp
fishstore::core::NullableStringRef foo(const std::vector<typename adapter_t::field_t>& fields)}
```

For more details about parser adapters, please refer to [this](../adapter_examples/README.md).

# Inline PSFs
FishStore optimize specifically for PSFs with small fixed-size return values (to avoid allocating additional space in optional region). If a PSF returns a value less than 4 types, user can define it as:

```cpp
fishstore::core::NullableInt foo(const std::vector<typename adapter_t::field_t>& fields)}
```

where `fishstore::core::NullableInt` is defined as below:

```cpp
struct NullableInt {
  bool is_null;
  int32_t value;
};
```

# Examples
In this folder, we provide two examples for PSF libraries:
- [Github](github_lib.cc)
- [Twitter](twitter_lib.cc)
