project(PythonExt)

find_package(Arrow REQUIRED)
find_package(pybind11 REQUIRED)

pybind11_add_module(simple simple.cc)
# add_executable(simple simple.cc)
target_link_libraries(simple PRIVATE Arrow::arrow_shared pybind11::pybind11)