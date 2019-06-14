# Build the FISHSTORE library.
set (FISHSTORE_HEADERS
  adapters/common_utils.h
  adapters/simdjson_adapter.h
  core/address.h
  core/alloc.h
  core/async.h
  core/async_result_types.h
  core/auto_ptr.h
  core/checkpoint_locks.h
  core/checkpoint_state.h
  core/constants.h
  core/fishstore.h
  core/gc_state.h
  core/grow_state.h
  core/guid.h
  core/hash_bucket.h
  core/hash_table.h
  core/internal_contexts.h
  core/key_hash.h
  core/light_epoch.h
  core/lss_allocator.h
  core/malloc_fixed_page_size.h
  core/native_buffer_pool.h
  core/persistent_memory_malloc.h
  core/phase.h
  core/record.h
  core/recovery_status.h
  core/state_transitions.h
  core/status.h
  core/thread.h
  core/utility.h
  core/psf.h
  device/file_system_disk.h
  device/null_disk.h
  environment/file.h
  environment/file_common.h
  adapters/common_utils.h
)

if (MSVC)
set (FISHSTORE_HEADERS ${FISHSTORE_HEADERS}
  environment/file_windows.h
)
else()
set (FISHSTORE_HEADERS ${FISHSTORE_HEADERS}
  environment/file_linux.h
)
endif() 

set (FISHSTORE_SOURCES
  core/address.cc
  core/lss_allocator.cc
  core/thread.cc
)

if (MSVC)
set (FISHSTORE_SOURCES ${FISHSTORE_SOURCES}
  environment/file_windows.cc
)
else()
set (FISHSTORE_SOURCES ${FISHSTORE_SOURCES}
  environment/file_linux.cc
)
endif()

add_library(fishstore STATIC ${FISHSTORE_SOURCES} ${FISHSTORE_HEADERS})
