# Extending FishStore with Parser Adaptors

FishStore is a general data storage system independent from input data format and should work with any data parsers. To make your favoriate parser work with FishStore, you can implement a parser adaptor to match up our internal interface. In specific, with a properly implemented parser adaptor, you can simply plug in your own parser by specifying the parser adaptor as a template argument:

```cpp
using store_t = fishstore::core::FishStore<FooAdaptor, disk_t>;
```

# General Parser Interface
[ParserInterface.h](ParserInterface.h) provides a general parser interface which extension developer should comply on. Generally speaking, Fthe parser should be able to construct with a given list of field names, parse a batch of documents (or a single document) returning an interator to iterate through all the records and all required fields. For each parsed field, user should be able to get the value in its corresponding format through interfaces like `GetAsInt()` or `GetAsDouble()`. Such functions return values in `NullableInt` and `NullableDouble` defined under `fishstore::adaptor` scope.

**Note that `NullableInt` and `NullableStringRef` defined under `fishstore::core` scope has different interfaces thatn that in `fishstore::adaptor`. Please do not be confused with them.**

# Parser Adaptor

With a parser complying with our general parser interface, user should define a parser adaptor as the following:

```cpp
class FooAdaptor {
public:
  typedef FooParser parser_t;
  typedef FooField field_t;

  static parser_t* NewParser(const std::vector<std::string>& fields);

  static SIMDJsonRecords Parse(parser_t* const parser, const char* payload, size_t length, size_t offset = 0);
};
```

User should explicitly define the parser type in `parser_t`, parsed field type in `field_t`, new parser allocation function `NewParser` and the actually parsing function `Parse` (which parse the `payload` starting from `offset`).

FishStore internally also define two traits `JsonAdaptor` and `CsvAdaptor` to support vairous functionality (basically a difference between fixed/flexible schema) in FishStore. Please make sure to inherient from them when connecting to a Json/CSV parser.

*In the future, we will also supporting other format specific feature and provide more parser adaptor traits.*

# Example
[simdjson_adaptor.h](simdjson_adaptor.h) provides a full encapsulation of [simdjson](https://github.com/lemire/simdjson) to comply FishStore parser interface and corresponding parser adaptor.
