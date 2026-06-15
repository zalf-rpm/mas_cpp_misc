project('sqlite3', 'c',
  version : '1.0',
  default_options : ['default_library=static'],
)

cc = meson.get_compiler('c')
dl_dep = cc.find_library('dl', required : false)

sqlite3_lib = static_library('sqlite3',
  sources : files('sqlite3.c'),
  include_directories : include_directories('.', '..'),
  dependencies : dl_dep,
)

sqlite3_dep = declare_dependency(
  link_with : sqlite3_lib,
  include_directories : include_directories('.', '..'),
  dependencies : dl_dep,
)
