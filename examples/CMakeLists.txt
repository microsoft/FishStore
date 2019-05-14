set (FISHSTORE_SIMDJSON_ADAPTOR_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/adaptor_examples/simdjson_adaptor.h)
include_directories (${CMAKE_CURRENT_SOURCE_DIR}/adaptor_examples)

if(WIN32)
  set (FISHSTORE_BENCHMARK_LINK_LIBS fishstore rpcrt4 wsock32 Ws2_32)
else()
  set (FISHSTORE_BENCHMARK_LINK_LIBS fishstore stdc++fs uuid tbb gcc aio m stdc++ pthread dl)
endif()

#Function to automate building benchmark binaries
FUNCTION(ADD_FISHSTORE_BENCHMARK BENCHMARK_NAME)
  add_executable(${BENCHMARK_NAME} ${BENCHMARK_HEADERS} ${FISHSTORE_SIMDJSON_ADAPTOR_HEADERS} ${BENCHMARK_NAME}.cc)
  target_link_libraries(${BENCHMARK_NAME} ${FISHSTORE_BENCHMARK_LINK_LIBS} simdjson)
ENDFUNCTION()

add_subdirectory(lib_examples)
add_subdirectory(github_query-dir)
add_subdirectory(online_demo-dir)
add_subdirectory(online_demo_disk-dir)
add_subdirectory(checkpoint_recovery-dir)
