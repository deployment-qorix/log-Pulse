cmake_minimum_required(VERSION 3.10)
project(logpulse LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_TESTS "Build tests" ON)

find_package(Threads REQUIRED)

# Collect all .cpp files under src
file(GLOB SRC_ALL src/*.cpp)

# Remove main.cpp from the core-sources so tests can define their own main()
set(SRC_CORE "")
foreach(f ${SRC_ALL})
  get_filename_component(fname ${f} NAME)
  if (NOT fname STREQUAL "main.cpp")
    list(APPEND SRC_CORE ${f})
  endif()
endforeach()

# Core library (everything except main.cpp)
add_library(logpulse_core STATIC ${SRC_CORE})
target_include_directories(logpulse_core PUBLIC ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(logpulse_core PRIVATE Threads::Threads)

if(WIN32)
  target_link_libraries(logpulse_core PRIVATE ws2_32)
endif()

# Main executable (links against core)
add_executable(logpulse src/main.cpp)
target_link_libraries(logpulse PRIVATE logpulse_core)

# Tests
if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
