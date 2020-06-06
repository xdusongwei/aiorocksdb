#include "rocks.h"


PYBIND11_MODULE(rocks_wrap, m) {
    py::class_<RocksStatus>(m, "RocksStatus", py::dynamic_attr())
    .def(py::init())
    .def("getCode", &RocksStatus::getCode)
    .def("getSubCode", &RocksStatus::getSubCode)
    .def("getSeverity", &RocksStatus::getSeverity)
    .def("setCode", &RocksStatus::setCode)
    .def("setSubCode", &RocksStatus::setSubCode)
    .def("setSeverity", &RocksStatus::setSeverity);

    py::class_<RocksIterator>(m, "RocksIterator", py::dynamic_attr())
    .def(py::init());

    py::class_<RocksSnapshot>(m, "RocksSnapshot", py::dynamic_attr())
    .def(py::init());

    py::class_<RocksColumnFamily>(m, "RocksColumnFamily", py::dynamic_attr())
    .def(py::init<const std::string &>())
    .def("getName", &RocksColumnFamily::getName);

    py::class_<Rocks>(m, "Rocks", py::dynamic_attr())
    .def(py::init<const std::string &, const bool>())
    .def("open_db", &Rocks::open_db)
    .def("open_transaction_db", &Rocks::open_transaction_db)
    .def("open_optimistic_transaction_db", &Rocks::open_optimistic_transaction_db)
    .def("destroyColumnFamily", &Rocks::destroyColumnFamily)
    .def("dropColumnFamily", &Rocks::destroyColumnFamily)
    .def("createColumnFamily", &Rocks::createColumnFamily)
    .def("createSnapshot", &Rocks::createSnapshot)
    .def("releaseSnapshot", &Rocks::releaseSnapshot)
    .def("createIterator", &Rocks::createIterator)
    .def("destroyIterator", &Rocks::destroyIterator)
    .def("close", &Rocks::close);
}
