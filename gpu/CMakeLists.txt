cmake_minimum_required(VERSION 3.18)

project(alluxioGPU)

include(cmake/Utils.cmake)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include directories
include_directories(
  "${PROJECT_SOURCE_DIR}"
  "${PROJECT_SOURCE_DIR}/cpp"
  "${PROJECT_BINARY_DIR}")

################################################
# Build Alluxio GPU libraries
################################################
add_subdirectory(cpp)
add_subdirectory(python)
