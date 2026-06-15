project('helpers', 'cpp',
  version : '1.0',
  default_options : [
    'cpp_std=c++17',
    'default_library=static',
  ],
)

# Sources live one level up (tools/), not in this subdirectory.
helpers_lib = static_library('helpers',
  sources : files(
    '../helper.cpp',
    '../algorithms.cpp',
  ),
  include_directories : include_directories('..', '../..'),
)

helpers_dep = declare_dependency(
  link_with : helpers_lib,
  include_directories : include_directories('..', '../..'),
)
