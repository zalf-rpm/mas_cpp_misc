project('date', 'cpp',
  version : '1.0',
  default_options : [
    'cpp_std=c++17',
    'default_library=static',
  ],
)

# Sources live one level up (tools/), not in this subdirectory.
# Cap'n Proto serialization support is not enabled here (matches the
# json11 CMakeLists.txt which leaves CAPNPROTO_SERIALIZATION_SUPPORT unset).
date_lib = static_library('date',
  sources : files('../date.cpp'),
  include_directories : include_directories('..', '../..'),
)

date_dep = declare_dependency(
  link_with : date_lib,
  include_directories : include_directories('..', '../..'),
)
