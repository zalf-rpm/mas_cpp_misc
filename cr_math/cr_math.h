/* Correctly rounded math functions (CORE-MATH, see README.md).

A correctly rounded function returns the double nearest to the exact
mathematical result, so it gives bit-identical results on every platform and
with every compiler - unlike the system math libraries (glibc, Microsoft's
UCRT), which may be off by one in the last bit, each for different arguments.
Use these instead of std::exp etc. wherever results must not depend on the
platform:

    #include "cr_math.h"
    double y = cr::exp(x);

Exact operations (sqrt, fabs, floor, fmod, ...) are the same everywhere and
stay with <cmath>. The plain C functions (cr_exp, ...) can also be called from
C or via FFI, e.g. from Odin. */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

double cr_exp(double x);
double cr_log(double x);
double cr_log2(double x);
double cr_log10(double x);
double cr_pow(double x, double y);
double cr_sin(double x);
double cr_cos(double x);
double cr_tan(double x);
double cr_asin(double x);
double cr_acos(double x);
double cr_atan(double x);

#ifdef __cplusplus
}

namespace cr {
inline double exp(double x) { return cr_exp(x); }
inline double log(double x) { return cr_log(x); }
inline double log2(double x) { return cr_log2(x); }
inline double log10(double x) { return cr_log10(x); }
inline double pow(double x, double y) { return cr_pow(x, y); }
inline double sin(double x) { return cr_sin(x); }
inline double cos(double x) { return cr_cos(x); }
inline double tan(double x) { return cr_tan(x); }
inline double asin(double x) { return cr_asin(x); }
inline double acos(double x) { return cr_acos(x); }
inline double atan(double x) { return cr_atan(x); }
} // namespace cr
#endif
