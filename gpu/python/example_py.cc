#include <pybind11/pybind11.h>
#include "example.h"

namespace py = pybind11;

PYBIND11_MODULE(example_py, m) {
    m.def("add", &add, "A function that adds two numbers");
}
