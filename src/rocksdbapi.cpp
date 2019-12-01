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

std::string kDBPath = "/tmp/rocksdb_simple_example";


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
            Options options;
            // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
            options.IncreaseParallelism();
            options.OptimizeLevelStyleCompaction();
            // create the DB if it's not already present
            options.create_if_missing = create_if_missing;

            // open DB
            Status s = DB::Open(options, path, &db);
            return s.ok();
        }

        bool close(){
            delete db;
            return true;
        }

        py::object get(const std::string& key){
            std::string value;
            Status s = db->Get(ReadOptions(), key, &value);
            if(s.IsNotFound()){
                return py::cast<py::none>(Py_None);
            }
            else if(!s.ok()){
                return py::cast<py::none>(Py_None);
            }
            else{
                return py::cast(value);
            }
        }

        bool put(const std::string& key, const std::string& value){
            Status s = db->Put(WriteOptions(), key, value);
            return s.ok();
        }

        bool del(const std::string& key){
            Status s = db->Delete(WriteOptions(), key);
            return s.ok();
        }

        bool execute(RocksDbBatch& batch){
            Status s = db->Write(WriteOptions(), &batch.getBatch());
            return s.ok();
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


