cmake_minimum_required(VERSION 3.9) # CMP0069 NEW

enable_testing()

include(ExternalProject)
project(FishStore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /Gm- /W3 /WX /EHsc /GS /fp:precise /permissive- /Zc:wchar_t /Zc:forScope /Zc:inline /Gd /TP")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /D_CRT_SECURE_NO_WARNINGS /FC /wd4244 /wd4267")

    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /MDd")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /O2 /Oi /Gy- /MD")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG /OPT:REF /OPT:NOICF /INCREMENTAL:NO")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG /OPT:REF /OPT:NOICF /INCREMENTAL:NO")
else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Og -g -D_DEBUG -fPIC")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -fPIC")
endif()

#Always set _DEBUG compiler directive when compiling bits regardless of target OS
set_directory_properties(PROPERTIES COMPILE_DEFINITIONS_DEBUG "_DEBUG")

##### BEGIN GOOGLE TEST INSTALLATION #####
# Copied from http://google.github.io/googletest/quickstart-cmake.html
# Download and unpack googletest at configure time
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

##### END GOOGLE TEST INSTALLATION #####

add_subdirectory(third_party/simdjson EXCLUDE_FROM_ALL)

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/third_party/hopscotch-map/include)

# Set the directory targets when build in libs and binaries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Build each subdirectory
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)

