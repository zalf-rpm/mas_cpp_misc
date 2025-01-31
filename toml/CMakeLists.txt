cmake_minimum_required(VERSION 3.12)
project(MAS-infrastructure-json11)

message(STATUS "-> mas_cpp_misc_toml")

add_library(toml_lib 
    STATIC 
    toml.hpp 

)
target_link_libraries(toml_lib
    PUBLIC 

)
target_include_directories(toml_lib 
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}    # for #include "toml.hpp"
    ${CMAKE_CURRENT_SOURCE_DIR}/.. # for #include "toml/toml.hpp"
)
if(MSVC AND MT_RUNTIME_LIB)
    target_compile_options(toml_lib PRIVATE "/MT$<$<CONFIG:Debug>:d>")
endif()

message(STATUS "<- mas_cpp_misc_toml")
