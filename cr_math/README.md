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

The sources are GNU C (`__int128`, `x86intrin.h`): GCC or Clang on Linux,
`clang-cl` on Windows - MSVC itself can't compile them. They are built with
`-ffp-contract=off`, as CORE-MATH expects FMA contraction to be used either
everywhere or nowhere.
