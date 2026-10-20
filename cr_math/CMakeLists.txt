cmake_minimum_required(VERSION 3.22)
project(MAS-infrastructure-cr-math C)

message(STATUS "-> MAS-infrastructure-cr-math")

# FMA instructions instead of the software fma CORE-MATH falls back to
# make MONICA about 30% faster, with identical results - but the executables
# then need an x86-64 CPU with FMA (Intel since 2013, AMD since 2012). See README.md.
option(CR_MATH_FMA "cr_math: use FMA instructions (CPUs since ~2013, see README.md)" ON)

set(CORE_MATH_FUNCTIONS exp log log2 log10 pow sin cos tan asin acos atan)

# CORE-MATH assumes FMA contraction everywhere or nowhere (its README); off
# makes it independent of compiler defaults.
set(CR_MATH_FLAGS -ffp-contract=off)
if(CR_MATH_FMA)
	list(APPEND CR_MATH_FLAGS -mfma)
endif()

if(MSVC)
	# The sources are GNU C (__int128, x86intrin.h), which MSVC can't compile:
	# clang-cl compiles them into MSVC-compatible objects instead. /Zl leaves
	# the choice of the C runtime (/MT or /MD) to the code linking them.
	find_program(CLANG_CL clang-cl REQUIRED
		HINTS "$ENV{ProgramFiles}/LLVM/bin"
		      "${CMAKE_GENERATOR_INSTANCE}/VC/Tools/Llvm/x64/bin")
	message(STATUS "cr_math: compiling CORE-MATH with ${CLANG_CL}")
	set(CR_MATH_OBJECTS)
	foreach(f ${CORE_MATH_FUNCTIONS})
		set(src ${CMAKE_CURRENT_SOURCE_DIR}/core-math/src/binary64/${f}/${f}.c)
		set(obj ${CMAKE_CURRENT_BINARY_DIR}/core-math/${f}.obj)
		file(GLOB headers ${CMAKE_CURRENT_SOURCE_DIR}/core-math/src/binary64/${f}/*.h)
		list(TRANSFORM CR_MATH_FLAGS PREPEND /clang: OUTPUT_VARIABLE flags)
		add_custom_command(OUTPUT ${obj}
			COMMAND ${CLANG_CL} /nologo /c /O2 /Zl /std:c11 ${flags} /Fo${obj} ${src}
			DEPENDS ${src} ${headers}
			COMMENT "clang-cl ${f}.c"
			VERBATIM)
		list(APPEND CR_MATH_OBJECTS ${obj})
	endforeach()
	set_source_files_properties(${CR_MATH_OBJECTS} PROPERTIES EXTERNAL_OBJECT TRUE GENERATED TRUE)
	add_library(cr_math_lib STATIC cr_math.h ${CR_MATH_OBJECTS})
	set_target_properties(cr_math_lib PROPERTIES LINKER_LANGUAGE C)
else()
	set(CR_MATH_SOURCES)
	foreach(f ${CORE_MATH_FUNCTIONS})
		list(APPEND CR_MATH_SOURCES core-math/src/binary64/${f}/${f}.c)
	endforeach()
	add_library(cr_math_lib STATIC cr_math.h ${CR_MATH_SOURCES})
	set_target_properties(cr_math_lib PROPERTIES C_STANDARD 11 C_EXTENSIONS ON)
	target_compile_options(cr_math_lib PRIVATE ${CR_MATH_FLAGS})
endif()

target_include_directories(cr_math_lib
	PUBLIC
	${CMAKE_CURRENT_SOURCE_DIR}    # for #include "cr_math.h"
)

message(STATUS "<- MAS-infrastructure-cr-math")
