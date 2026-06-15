project('json11', 'cpp',
  version : '1.0',
  default_options : [
    'cpp_std=c++17',
    'default_library=static',
  ],
)

add_project_arguments('-DCAPNPROTO_SERIALIZATION_SUPPORT', language : 'cpp')

# When used as a subproject of common, Meson resolves these from the
# top-level project's subprojects/ directory, so symlinks for 'helpers'
# and 'date' must exist there alongside 'json11'.
date_proj    = subproject('date',    required : true)
date_dep     = date_proj.get_variable('date_dep')

helpers_proj = subproject('helpers', required : true)
helpers_dep  = helpers_proj.get_variable('helpers_dep')

json11_lib = static_library('json11',
  sources : files(
    'json11.cpp',
    'json11-helper.cpp',
  ),
  include_directories : include_directories('.', '..'),
  dependencies : [date_dep, helpers_dep],
)

json11_dep = declare_dependency(
  link_with : json11_lib,
  include_directories : include_directories('.', '..'),
  dependencies : [date_dep, helpers_dep],
)
