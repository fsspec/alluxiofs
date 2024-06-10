include(FetchContent)

FetchContent_Declare(
  pybind11
  GIT_REPOSITORY "https://github.com/pybind/pybind11.git"
  GIT_TAG "v2.12.0"
)
FetchContent_MakeAvailable(pybind11)

# Helper function to remove elements from a variable
function(remove TARGET INPUT)
  foreach(ITEM ${ARGN})
    list(REMOVE_ITEM INPUT "${ITEM}")
  endforeach()
  set(${TARGET} ${INPUT} PARENT_SCOPE)
endfunction(remove)

# Collect headers in the current directory
macro(collect_headers HEADERS_GROUP)
  cmake_parse_arguments(
    COLLECT_HEADERS
    "PARENT_SCOPE;INCLUDE_TEST"
    ""
    ""
    ${ARGV})

  file(GLOB collect_headers_tmp *.h *.hpp *.cuh *.inl)
  set(${HEADERS_GROUP} ${${HEADERS_GROUP}} ${collect_headers_tmp})

  # We remove filenames containing substring "test"
  if(NOT COLLECT_HEADERS_INCLUDE_TEST)
    file(GLOB collect_headers_tmp *test*)
    remove(${HEADERS_GROUP} "${${HEADERS_GROUP}}" ${collect_headers_tmp})
  endif()

  if(COLLECT_HEADERS_PARENT_SCOPE)
    set(${HEADERS_GROUP} ${${HEADERS_GROUP}} PARENT_SCOPE)
  endif()
endmacro(collect_headers)

# Collect sources in the current directory
macro(collect_sources SRCS_GROUP)
  cmake_parse_arguments(
    COLLECT_SOURCES
    "PARENT_SCOPE"
    ""
    ""
    ${ARGV})

  file(GLOB collect_sources_tmp *.cc *.cu *.c)
  file(GLOB collect_sources_tmp_test *_test.cc *_test.cu *_test.c)
  remove(collect_sources_tmp "${collect_sources_tmp}" ${collect_sources_tmp_test})
  set(${SRCS_GROUP} ${${SRCS_GROUP}} ${collect_sources_tmp})

  if (COLLECT_SOURCES_PARENT_SCOPE)
    set(${SRCS_GROUP} ${${SRCS_GROUP}} PARENT_SCOPE)
  endif()
endmacro(collect_sources)
