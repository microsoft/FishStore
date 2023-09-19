# Extending FishStore with Parser Adapters

FishStore is a general data storage system independent from input data format and should work with any data parsers. To make your favoriate parser work with FishStore, you can implement a parser adapter to match up our internal interface. In specific, with a properly implemented parser adapter, you can simply plug in your own parser by specifying the parser adapter as a template argument:

```cpp
using store_t = fishstore::core::FishStore<FooAdapter, disk_t>;
```

# General Parser Interface
[`parser_api.h`](parser_api.h) provides a general parser interface which extension developer should comply on. Generally speaking, a parser should be able to construct with a given list of field names, parse a batch of documents (or a single document) returning an interator to iterate through all the records and all required fields. For each parsed field, user should be able to get the value in its corresponding format through interfaces like `GetAsInt()` or `GetAsDouble()`. Such functions return values in `NullableInt` and `NullableDouble` defined under `fishstore::adapter` scope.

**Note that `NullableInt` and `NullableStringRef` defined under `fishstore::core` scope has different interfaces than that in `fishstore::adapter`. Please do not be confused with them.**

# Parser Adapter

With a parser complying with our general parser interface, user should define a parser adapter as the following:

```cpp
class FooAdapter {
public:
  typedef FooParser parser_t;
  typedef FooField field_t;
  typedef FooRecord record_t;

  static parser_t* NewParser(const std::vector<std::string>& fields);
  static void Load(parser_t* const parser, const char* payload, size_t length, size_t offset = 0);
  inline static bool HasNext(parser_t* const parser);
  inline static const record_t& NextRecord(parser_t* const parser);
};
```

User should explicitly define the parser type in `parser_t`, parsed field type in `field_t`, and new parser allocation function `NewParser`. Furthermore, user also need to define a load function for the parser to process a `payload` starting from `offset`. To ensure all parsed records with successfully parsed fields can be accessed by FishStore, user also need to define an iterator-like API over the parser to retrieve next available record in the bach currently loaded.

# Example
[simdjson_adapter.h](simdjson_adapter.h) provides a full encapsulation of [simdjson](https://github.com/lemire/simdjson) to comply FishStore parser interface and corresponding parser adapter.

There are a few known limitations with simdjson parser wrapper and adapter:

- Note that simdjson currently only supports parsing one JSON record at a time. Thus, users can only feed one record in raw text to `BatchInsert()` at a time. As a result, user need to implement their own logic to delimit record boundaries within a batch in application level.
- `SIMDJsonParser` and `SIMDJsonAdapter` only supports object-based field names (e.g., `actor.id`, `payload.action.type`), and arrays (like `a[0].b`), although all field names must start with an object (`[0].xyz` is not allowed). Wildcards `a.*.b` are not supported.
