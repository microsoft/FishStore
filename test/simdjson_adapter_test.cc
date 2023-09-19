// A basic testing framework for simdjson seperate from FishStore
//
// Created by Max Norfolk on 7/23/23.

#include "gtest/gtest.h"
#include "adapters/simdjson_adapter.h"

using adapter_t = fishstore::adapter::SIMDJsonAdapter;
using parser_t = fishstore::adapter::SIMDJsonParser;

std::string raw_json = R"XX(
{"id":3, "school":{"id":6}, "random":"garbage13"}
{ "school":{"id":7}, "id":6, "random":"garbage24"}
{"id":100, "school":{"id":3}, "random":"garbage35"}
)XX";

TEST(SimdJsonTests, BasicTest1) {
    parser_t parser({"id", "school.id"});
    parser.Load(raw_json.data(), raw_json.length());

    ASSERT_TRUE(parser.HasNext());

    auto rec = parser.NextRecord();
    auto &fields = rec.GetFields();
    ASSERT_EQ(fields.size(), 2);


    // get raw text first
    auto raw_text = rec.GetRawText();
    std::string raw_text_str = {raw_text.Data(), raw_text.Length()};

    // verify raw text
    const std::string correct_text = R"({"id":3, "school":{"id":6}, "random":"garbage13"})";
    ASSERT_EQ(raw_text_str, correct_text);

    // check int
    auto n = fields[0].GetAsInt();
    ASSERT_TRUE(n.HasValue());
    ASSERT_EQ(n.Value(), 3);

    // get raw again
    raw_text = rec.GetRawText();
    raw_text_str = {raw_text.Data(), raw_text.Length()};
    ASSERT_EQ(raw_text_str, correct_text);

}


int main(int argc, char *argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
