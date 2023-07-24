// A basic testing framework for simdjson seperate from FishStore
//
// Created by Max Norfolk on 7/23/23.

#include "gtest/gtest.h"
#include "adapters/simdjson_adapter.h"

using adapter_t = fishstore::adapter::SIMDJsonAdapter;
using parser_t = fishstore::adapter::SIMDJsonParser;

std::string raw_json = R"XX(
{"id":3, "school":{"id":6}, "random":"garbage13"}
{"id":6, "school":{"id":7}, "random":"garbage24"}
{"id":100, "school":{"id":3}, "random":"garbage35"}
)XX";

TEST(SimdJsonTests, BasicTest1) {
    parser_t parser({"id", "school.id"});
    parser.Load(raw_json.data(), raw_json.length());
    ASSERT_TRUE(parser.HasNext());
    auto rec = parser.NextRecord();
    auto &fields = rec.GetFields();
    auto raw_text = rec.GetRawText();
    std::string str = {raw_text.Data(), raw_text.Length()};
    printf("Raw String: %s\n", str.data());
    ASSERT_EQ(fields.size(), 2);
    ASSERT_TRUE(fields[0].GetAsInt().HasValue());
}


int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
