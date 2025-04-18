cmake_minimum_required(VERSION 3.12)
project(MAS-infrastructure-json11)

message(STATUS "-> MAS-infrastructure-json11")

add_compile_definitions(CAPNPROTO_SERIALIZATION_SUPPORT)
set(CAPNPROTO_SERIALIZATION_SUPPORT 1)

if(NOT TARGET date_lib)
    message(STATUS "target: date_lib")
    add_subdirectory(../tools/date date)
endif()

if(NOT TARGET helpers_lib)
    message(STATUS "target: helpers_lib")
    add_subdirectory(../tools/helpers helpers)
endif()

add_library(json11_lib 
    STATIC 
    json11.hpp 
    json11.cpp 
    json11-helper.h 
    json11-helper.cpp
)
target_link_libraries(json11_lib
    PUBLIC 
    date_lib 
    helpers_lib
)
target_include_directories(json11_lib 
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}    # for #include "json11.hpp"
    ${CMAKE_CURRENT_SOURCE_DIR}/.. # for #include "json11/json11.hpp"
)
if(MSVC AND MT_RUNTIME_LIB)
    target_compile_options(json11_lib PRIVATE "/MT$<$<CONFIG:Debug>:d>")
endif()

message(STATUS "<- MAS-infrastructure-json11")