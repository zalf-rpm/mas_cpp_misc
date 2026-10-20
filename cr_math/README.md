# cr_math - correctly rounded math functions

`exp`, `log`, `log2`, `log10`, `pow`, `sin`, `cos`, `tan`, `asin`, `acos` and
`atan` from [CORE-MATH](https://core-math.gitlabpages.inria.fr/) (MIT license,
`core-math/LICENSE`), as the static library `cr_math_lib` with the header
`cr_math.h` (`cr::exp(x)` in C++, `cr_exp(x)` in C or via FFI).

Correctly rounded means each function returns the double nearest to the exact
result. The system math libraries don't guarantee that (glibc and Microsoft's
UCRT are within about one unit in the last place, each in its own way), so the
same model gives different results on Linux and Windows. With these functions
the results only depend on the code, not on the platform's math library.

## Source

`core-math/` holds the files for these functions from
https://gitlab.inria.fr/core-math/core-math, unchanged, at commit
`7ebab73913c5a33a25402ee51c96ec06b3a18095` (2026-10-20), in upstream's layout.
To update: copy the same files from a newer checkout and record its commit
here. Another function: add its directory the same way and the name to
`CORE_MATH_FUNCTIONS` in `CMakeLists.txt` and to `cr_math.h`.

## Building

The sources are GNU C (`__int128`, `x86intrin.h`): GCC or Clang on Linux; on
Windows (MSVC) `CMakeLists.txt` compiles them with `clang-cl` into
MSVC-compatible objects, so the code using them stays with MSVC. They are
built with `-ffp-contract=off`, as CORE-MATH expects FMA contraction to be
used either everywhere or nowhere.

`CR_MATH_FMA` (default `ON`) builds them with `-mfma`: the explicit fma calls
then become single instructions instead of calls to a software fma (MONICA
as a whole: about 30% faster). Results are identical either way (fma is exact), but the executables
need an x86-64 CPU with FMA (Intel since 2013, AMD since 2012). Without
SSE4.1, clang lowers `__builtin_roundeven` to a `roundeven()` call, which
Microsoft's C runtime lacks - so `CR_MATH_FMA=OFF` on Windows may need
`-msse4.1` added (untested).

On Windows `long` is 32 bits. The one place in these sources where that
matters (`asin.c`, the hard-case test) can only flag extra arguments for the
lookup in its table of exceptions, which returns the result unchanged for any
argument not in it - so it costs time, never accuracy.
