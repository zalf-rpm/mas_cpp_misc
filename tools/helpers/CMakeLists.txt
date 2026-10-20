cmake_minimum_required(VERSION 3.22)
project(Util-tools-helpers)

message(STATUS "-> MAS-infrastructure-tools-helpers")

if(NOT TARGET cr_math_lib)
	message(STATUS "target: cr_math_lib")
	add_subdirectory(../../cr_math cr_math)
endif()

add_library(helpers_lib 
	STATIC 
	../helper.h 
	../helper.cpp 
	../algorithms.h 
	../algorithms.cpp 
	../datastructures.h
)
target_link_libraries(helpers_lib PUBLIC cr_math_lib)

target_include_directories(helpers_lib 
	PUBLIC 
	${CMAKE_CURRENT_SOURCE_DIR}/..    # for #include "helper.h"
	${CMAKE_CURRENT_SOURCE_DIR}/../.. # for #include "tools/helper.h"
)

if(MSVC AND MT_RUNTIME_LIB)
	target_compile_options(helpers_lib PRIVATE "/MT$<$<CONFIG:Debug>:d>")
endif()

message(STATUS "<- MAS-infrastructure-tools-helpers")
