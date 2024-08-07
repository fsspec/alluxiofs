# Collect sources and headers for the cpp library
collect_sources(CORE_SRCS PARENT_SCOPE)
collect_headers(CORE_HDRS PARENT_SCOPE)

## Why here is core, can it be others
add_library(cpp SHARED ${CORE_SRCS})
set_target_properties(cpp PROPERTIES PREFIX "")
target_include_directories(cpp PRIVATE ${CMAKE_SOURCE_DIR}/cpp)
