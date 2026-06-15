project('mas_cpp_misc_common', 'cpp',
  version : '1.0',
  default_options : [
    'cpp_std=c++17',
    'default_library=static',
  ]
)

# ---------------------------------------------------------------------------
# Sub-projects / sibling directories
# (mirror the add_subdirectory() calls in CMakeLists.txt)
# ---------------------------------------------------------------------------

capnp_schemas_proj = subproject('mas_capnproto_schemas',
  required : true,
)
capnp_schemas_dep = capnp_schemas_proj.get_variable('capnp_schemas_dep')

helpers_proj = subproject('helpers',
  required : true,
)
helpers_dep = helpers_proj.get_variable('helpers_dep')

json11_proj = subproject('json11',
  required : true,
)
json11_dep = json11_proj.get_variable('json11_dep')

sqlite3_proj = subproject('sqlite3',
  required : true,
)
sqlite3_dep = sqlite3_proj.get_variable('sqlite3_dep')

# ---------------------------------------------------------------------------
# System / pkg-config dependencies
# ---------------------------------------------------------------------------

threads_dep = dependency('threads')
dl_dep      = meson.get_compiler('cpp').find_library('dl', required : false)

sodium_dep  = dependency('libsodium', required : true)

# ---------------------------------------------------------------------------
# Global compile definitions
# (mirror add_compile_definitions() in CMakeLists.txt)
# ---------------------------------------------------------------------------

add_project_arguments(
  '-D_USE_MATH_DEFINES',
  '-DNO_MYSQL',
  '-DSODIUM_STATIC',
  language : 'cpp',
)

# MSVC-specific: security-warning suppression and static runtime (/MT) are
# handled via Meson's b_vscrt option (e.g. -Db_vscrt=mt) and the built-in
# MSVC warning flags rather than add_project_arguments, so they are omitted
# here and left to the build-directory configuration step.

# ---------------------------------------------------------------------------
# common_lib  (static library)
# ---------------------------------------------------------------------------

common_lib_sources = files(
  'channel.cpp',
  'PortConnector.cpp',
  'common.cpp',
  'restorer.cpp',
  'restorable-service-main.cpp',
  'rpc-connection-manager.cpp',
)

common_lib_headers = files(
  'channel.h',
  'PortConnector.h',
  'common.h',
  'restorer.h',
  'restorable-service-main.h',
  'rpc-connection-manager.h',
  'dll-exports.h',
  'sole.hpp',
)

common_lib = static_library('common',
  sources : common_lib_sources + common_lib_headers,
  include_directories : include_directories('.', '..'),
  dependencies : [
    threads_dep,
    dl_dep,
    capnp_schemas_dep,
    helpers_dep,
    json11_dep,
    sodium_dep,
  ],
)

# Dependency object that downstream targets (and the executables below) use
common_dep = declare_dependency(
  link_with : common_lib,
  include_directories : include_directories('.', '..'),
  dependencies : [
    threads_dep,
    dl_dep,
    capnp_schemas_dep,
    helpers_dep,
    json11_dep,
    sodium_dep,
  ],
)

# ---------------------------------------------------------------------------
# Executable: channel
# ---------------------------------------------------------------------------

executable('channel',
  sources : files('channel-main.cpp'),
  dependencies : common_dep,
)

# ---------------------------------------------------------------------------
# Executable: storage-service
# ---------------------------------------------------------------------------

executable('storage-service',
  sources : files(
    'storage-service.cpp',
    'storage-service-main.cpp',
  ),
  dependencies : [sqlite3_dep, common_dep],
)

# ---------------------------------------------------------------------------
# Executable: host-port-resolver-service
# ---------------------------------------------------------------------------

executable('host-port-resolver-service',
  sources : files(
    'host-port-resolver.cpp',
    'host-port-resolver-main.cpp',
  ),
  dependencies : common_dep,
)

# ---------------------------------------------------------------------------
# Executable: gateway-service
# ---------------------------------------------------------------------------

executable('gateway-service',
  sources : files(
    'gateway.cpp',
    'gateway-main.cpp',
  ),
  dependencies : common_dep,
)
