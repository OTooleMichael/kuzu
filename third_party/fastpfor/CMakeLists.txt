add_library(fastpfor
        STATIC
        fastpfor/bitpacking.cpp fastpfor/bitpackingaligned.cpp)

target_include_directories(fastpfor PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR})
