#include <arrow/api.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <iostream>
#include <memory>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <tuple>

namespace py = pybind11;

class SimpleAllocator {
public:
  SimpleAllocator(const std::vector<int> &els) {
    arrow::Int64Builder builder;
    for (auto it = els.begin(); it != els.end(); it++) {
      arrow::Status status = builder.Append(*it);
      if (!status.ok()) {
        std::cout << "Failed to append element: " << *it << std::endl;
        // Handle the error accordingly
      }
    }

    auto res = builder.Finish();
    if (res.ok()) {
      this->arr = res.ValueOrDie();
      auto status = arrow::ExportArray(*this->arr, &this->data, &this->schema);
      std::cout << "Export ok? " << status.ok() << std::endl;
    } else {
      std::cout << "failed" << std::endl;
    }
  };

  std::tuple<intptr_t, intptr_t> address_of() {
    return std::make_tuple(reinterpret_cast<intptr_t>(&(this->data)),
                           reinterpret_cast<intptr_t>(&(this->schema)));
  }

  int at(int i) {
    auto int64_arr = std::static_pointer_cast<arrow::Int64Array>(this->arr);

    return int64_arr->Value(i);
  }

  std::shared_ptr<arrow::Array> arr;
  ArrowArray data;
  ArrowSchema schema;
};

void test_func(intptr_t address1, intptr_t address2) {
  auto data_ptr = reinterpret_cast<ArrowArray *>(address1);
  auto schema_ptr = reinterpret_cast<ArrowSchema *>(address2);
  auto status = arrow::ImportArray(data_ptr, schema_ptr);
  if (status.ok()) {
    auto int64_arr =
        std::static_pointer_cast<arrow::Int64Array>(status.ValueOrDie());

    for (int i = 0; i < int64_arr->length(); i++) {
      std::cout << int64_arr->Value(i) << std::endl;
    }
  } else {
    std::cout << "failed" << std::endl;
    ;
  }
}

PYBIND11_MODULE(simple, simple) {
  py::class_<SimpleAllocator>(simple, "SimpleAllocator")
      .def(py::init<const std::vector<int> &>(), py::arg("els"))
      .def("at", &SimpleAllocator::at)
      .def("address_of", &SimpleAllocator::address_of);

  simple.def("test", &test_func);
}

// int main() {
//   SimpleAllocator alloc;
//   // auto res = arrow::ImportArray(&alloc.data, &alloc.schema);
//   // std::cout << "res.ok = " << res.ok() << std::endl;

//   intptr_t data, schema;
//   // std::tie(data, schema) = alloc.address_of();
//   data = reinterpret_cast<intptr_t>(&alloc.data);
//   schema = reinterpret_cast<intptr_t>(&alloc.schema);
//   auto data_ptr = reinterpret_cast<ArrowArray *>(data);
//   auto schema_ptr = reinterpret_cast<ArrowSchema *>(schema);
//   auto status = arrow::ImportArray(data_ptr, schema_ptr);
//   std::cout << "&data = " << data << std::endl;
//   std::cout << "&schema = " << schema << std::endl;
//   std::cout << "status.ok = " << status.ok() << std::endl;

//   test_func(data, schema);
// }