# tests/CMakeLists.txt
cmake_minimum_required(VERSION 3.10)
project(logpulse_tests)

# Make an object library containing global test symbols
add_library(test_globals OBJECT test_globals.cpp)
target_include_directories(test_globals PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR}/include)

# Helper macro to create tests linking the globals object library
macro(add_my_test name src)
  add_executable(${name} ${src})
  target_link_libraries(${name} PRIVATE logpulse_core Threads::Threads $<TARGET_OBJECTS:test_globals>)
  add_test(NAME ${name} COMMAND ${name})
endmacro()

# Example tests (adjust file names as you have them)
add_my_test(test_parser    test_parser.cpp)
add_my_test(test_aggregator test_aggregator.cpp)
add_my_test(test_spacesaving test_spacesaving.cpp)
add_my_test(test_bounded_queue test_bounded_queue.cpp)
add_my_test(test_worker_pool test_worker_pool.cpp)
add_my_test(test_checkpoint test_checkpoint.cpp)
add_my_test(test_sliding_window test_sliding_window.cpp)

enable_testing()
