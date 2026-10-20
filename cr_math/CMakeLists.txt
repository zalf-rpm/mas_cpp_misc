cmake_minimum_required(VERSION 3.22)
project(MAS-infrastructure-cr-math C)

message(STATUS "-> MAS-infrastructure-cr-math")

# The CORE-MATH sources need GCC or Clang (GNU C: __int128, x86intrin.h).
set(CORE_MATH_FUNCTIONS exp log log2 log10 pow sin cos tan asin acos atan)
set(CORE_MATH_SOURCES)
foreach(f ${CORE_MATH_FUNCTIONS})
	list(APPEND CORE_MATH_SOURCES core-math/src/binary64/${f}/${f}.c)
endforeach()

add_library(cr_math_lib STATIC cr_math.h ${CORE_MATH_SOURCES})

target_include_directories(cr_math_lib
	PUBLIC
	${CMAKE_CURRENT_SOURCE_DIR}    # for #include "cr_math.h"
)

set_target_properties(cr_math_lib PROPERTIES C_STANDARD 11 C_EXTENSIONS ON)
# CORE-MATH assumes FMA contraction everywhere or nowhere (its README); off
# makes it independent of compiler defaults and target CPU flags.
target_compile_options(cr_math_lib PRIVATE -ffp-contract=off)

message(STATUS "<- MAS-infrastructure-cr-math")
