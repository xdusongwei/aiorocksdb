// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pybind11/pybind11.h>
namespace py = pybind11;


#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"


using namespace rocksdb;


class RocksDbBatch{
     public:
        RocksDbBatch(){
        }

        void put(const std::string& key, const std::string& value){
            batch.Put(key, value);
        }

        void del(const std::string& key){
            batch.Delete(key);
        }

        WriteBatch& getBatch(){
            return batch;
        }

     private:
        WriteBatch    batch;
};


class RocksDb {
    public:
        RocksDb(const std::string &path, const bool create_if_missing)
            { this->path = path; this->create_if_missing = create_if_missing; }

        bool open(){
            py::gil_scoped_release release;
            bool is_ok = false;
            try {
                Options options;
                options.IncreaseParallelism();
                options.OptimizeLevelStyleCompaction();
                options.create_if_missing = create_if_missing;
                Status s = DB::Open(options, path, &db);
                is_ok = s.ok();
            }
            catch (...) {  }
            py::gil_scoped_acquire acquire;
            return is_ok;
        }

        bool close(){
            py::gil_scoped_release release;
            try {
                delete db;
            }
            catch (...) {  }
            py::gil_scoped_acquire acquire;
            return true;
        }

        py::object get(const std::string& key){
            py::gil_scoped_release release;
            std::string value;
            py::object result = py::cast<py::none>(Py_None);
            try {
                Status s = db->Get(ReadOptions(), key, &value);
                if(s.IsNotFound()){
                    result = py::cast<py::none>(Py_None);
                }
                else if(!s.ok()){
                    result = py::cast<py::none>(Py_None);
                }
                else{
                    result = py::bytes(value);
                }
            }
            catch (...) {  }
            py::gil_scoped_acquire acquire;
            return result;
        }

        bool put(const std::string& key, const std::string& value){
            py::gil_scoped_release release;
            bool is_ok = false;
            try {
                Status s = db->Put(WriteOptions(), key, value);
                is_ok = s.ok();
            }
            catch (...) {  }
            py::gil_scoped_acquire acquire;
            return is_ok;
        }

        bool del(const std::string& key){
            py::gil_scoped_release release;
            bool is_ok = false;
            try {
                Status s = db->Delete(WriteOptions(), key);
                is_ok = s.ok();
            }
            catch (...) {  }
            py::gil_scoped_acquire acquire;
            return is_ok;
        }

        bool execute(RocksDbBatch& batch){
            py::gil_scoped_release release;
            bool is_ok = false;
            try {
                Status s = db->Write(WriteOptions(), &batch.getBatch());
                is_ok = s.ok();
            }
            catch (...) {  }
            py::gil_scoped_acquire acquire;
            return is_ok;
        }

    private:
        std::string    path;
        bool           create_if_missing;
        DB*            db;
};


PYBIND11_MODULE(rocksdbapi, m) {
    py::class_<RocksDb>(m, "RocksDb", py::dynamic_attr())
    .def(py::init<const std::string &, const bool>())
    .def("open", &RocksDb::open)
    .def("close", &RocksDb::close)
    .def("get", &RocksDb::get)
    .def("put", &RocksDb::put)
    .def("execute", &RocksDb::execute)
    .def("delete", &RocksDb::del);

    py::class_<RocksDbBatch>(m, "RocksDbBatch", py::dynamic_attr())
    .def(py::init<>())
    .def("put", &RocksDbBatch::put)
    .def("delete", &RocksDbBatch::del);
}


