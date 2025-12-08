find_package(pybind11 REQUIRED)

collect_sources(PYTHON_SRCS)
collect_headers(PYTHON_HEADERS)

pybind11_add_module(example_py example_py.cc)
set_target_properties(example_py PROPERTIES PREFIX "")
target_link_libraries(example_py PRIVATE cpp)
