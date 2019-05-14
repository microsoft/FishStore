add_library(github_lib SHARED github_lib.cc)
target_link_libraries(github_lib simdjson)

add_library(twitter_lib SHARED twitter_lib.cc)
target_link_libraries(twitter_lib simdjson)
