// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <cstdio>
#include <iostream>
#include <vector>
#ifdef _WIN32
#define LIBEXPORT_MACROS extern "C" __declspec(dllexport)
#else
#define LIBEXPORT_MACROS extern "C"
#endif
#include "adapters/simdjson_adapter.h"
#include "core/psf.h"

typedef fishstore::adapter::SIMDJsonAdapter adapter_t;
using namespace fishstore::core;

LIBEXPORT_MACROS NullableInt jp_high_follower(std::vector<adapter_t::field_t>& fields) {
  auto user_lang = fields[0].GetAsStringRef();
  auto follower_count = fields[1].GetAsLong();
  if(user_lang.HasValue() && follower_count.HasValue() &&
     strcmp(user_lang.Value().Data(),  "ja") == 0 && follower_count.Value() > 3000) {
    return NullableInt{ false, 1 };
  } else return NullableInt { true, 0 };
}

LIBEXPORT_MACROS NullableInt sensitive_reply_to_trump(
  const std::vector<adapter_t::field_t>& fields) {
  auto screen_name = fields[0].GetAsStringRef();
  auto sensitive = fields[1].GetAsBool();
  if(screen_name.HasValue() && strcmp(screen_name.Value().Data(), "realDonaldTrump") == 0 &&
      sensitive.HasValue() && sensitive.Value()) {
    return NullableInt{ false, 1 };
  } else return NullableInt { true, 0 };
}
