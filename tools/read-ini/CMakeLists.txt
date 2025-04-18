cmake_minimum_required(VERSION 3.12)
project(MAS-infrastructure-tools-read_ini)

message(STATUS "-> MAS-infrastructure-tools-read_ini")

if(NOT TARGET date_lib)
  message(STATUS "target: date_lib")
	add_subdirectory(../date date)
endif()

if(NOT TARGET helpers_lib)
	message(STATUS "target: helpers_lib")
	add_subdirectory(../helpers helpers)
endif()

add_library(read_ini_lib 
	STATIC 
	../read-ini.h 
	../read-ini.cpp
)
target_link_libraries(read_ini_lib 
	PUBLIC 
	date_lib
	helpers_lib
)
target_include_directories(read_ini_lib 
	PUBLIC 
	${CMAKE_CURRENT_SOURCE_DIR}/..    # for #include "read-ini.h"
	${CMAKE_CURRENT_SOURCE_DIR}/../.. # for #include "tools/read-ini.h"
)

if(MSVC AND MT_RUNTIME_LIB)
	target_compile_options(read_ini_lib PRIVATE "/MT$<$<CONFIG:Debug>:d>")
endif()

message(STATUS "<- MAS-infrastructure-tools-read_ini")