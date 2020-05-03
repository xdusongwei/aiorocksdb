#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>
namespace py = pybind11;


#include <cstdio>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"


using namespace rocksdb;


class RocksIterator{
    public:
        RocksIterator(){
            iter = nullptr;
        }

        void setIterator(Iterator* iter){
            this->iter = iter;
        }

        void close(DB* db){
            if(iter){
                delete iter;
                iter = nullptr;
            }
        }

    private:
        Iterator* iter;
};


class RocksSnapshot{
    public:
        RocksSnapshot(){
            snapshot = nullptr;
        }

        void setSnapshot(const Snapshot* snapshot){
            this->snapshot = snapshot;
        }

        const Snapshot* getSnapshot(){
            return snapshot;
        }

        void release(DB* db){
            if(snapshot){
                db->ReleaseSnapshot(snapshot);
                snapshot = nullptr;
            }
        }

    private:
        const Snapshot* snapshot;
};


class ColumnFamily{
    public:
        ColumnFamily(const std::string &name){
            this->name = name;
        }

        void setHandle(ColumnFamilyHandle *cf){
            this->cf = cf;
        }

        ColumnFamilyHandle* getHandle(){
            return cf;
        }

        std::string& getName(){
            return name;
        }

        void drop(DB* db){
            if(cf){
                db->DropColumnFamily(cf);
                cf = nullptr;
            }
        }

        void close(DB* db){
            if(cf){
                db->DestroyColumnFamilyHandle(cf);
                cf = nullptr;
            }
        }

    private:
        std::string name;
        ColumnFamilyHandle* cf;
};


class RocksDb {
    public:
        RocksDb(const std::string &path, const bool create_if_missing)
            { this->path = path; this->create_if_missing = create_if_missing; }

        bool open(std::vector<ColumnFamily>& column_family_list){
            bool is_ok = false;
            try {
                std::vector<ColumnFamilyDescriptor> column_families;
                std::vector<ColumnFamilyHandle*> handles;
                for(ColumnFamily i : column_family_list) {
                    column_families.push_back(ColumnFamilyDescriptor(i.getName(), ColumnFamilyOptions()));
                }
                Options options;
                options.IncreaseParallelism();
                options.OptimizeLevelStyleCompaction();
                options.create_if_missing = create_if_missing;
                Status s = DB::Open(options, path, column_families, &handles, &db);
                for(auto i = 0u;i<column_family_list.size();i++) {
                    column_family_list[i].setHandle(handles[i]);
                }
                handles.clear();
                is_ok = s.ok();
            }
            catch (...) {  }
            return is_ok;
        }

        void dropColumnFamily(ColumnFamily& cf){
            if(db){
                cf.drop(db);
            }
        }

        void destroyColumnFamily(ColumnFamily& cf){
            if(db){
                cf.close(db);
            }
        }

        void createColumnFamily(ColumnFamily& cf){
            std::vector<std::string> result;
            ColumnFamilyHandle* handle;
            db->CreateColumnFamily(ColumnFamilyOptions(), cf.getName(), &handle);
            cf.setHandle(handle);
        }

        void createSnapshot(RocksSnapshot& ss){
            ss.setSnapshot(db->GetSnapshot());
        }

        void releaseSnapshot(RocksSnapshot& snapshot){
            if(db){
                snapshot.release(db);
            }
        }

        void createIterator(RocksIterator& iter, RocksSnapshot& snapshot){
            ReadOptions options;
            options.snapshot = snapshot.getSnapshot();
            Iterator* newIter = db->NewIterator(options);
            iter.setIterator(newIter);
        }

        void destroyIterator(RocksIterator& iter){
            if(db){
                iter.close(db);
            }
        }

        void close(){
            try {
                if(db){
                    delete db;
                    db = nullptr;
                }
            }
            catch (...) {  }
        }

    private:
        std::string    path;
        bool           create_if_missing;
        DB*            db;
};


PYBIND11_MODULE(wrap, m) {
    py::class_<RocksIterator>(m, "RocksIterator", py::dynamic_attr())
    .def(py::init());

    py::class_<RocksSnapshot>(m, "RocksSnapshot", py::dynamic_attr())
    .def(py::init());

    py::class_<ColumnFamily>(m, "ColumnFamily", py::dynamic_attr())
    .def(py::init<const std::string &>())
    .def("getName", &ColumnFamily::getName);

    py::class_<RocksDb>(m, "RocksDb", py::dynamic_attr())
    .def(py::init<const std::string &, const bool>())
    .def("open", &RocksDb::open)
    .def("destroyColumnFamily", &RocksDb::destroyColumnFamily)
    .def("dropColumnFamily", &RocksDb::destroyColumnFamily)
    .def("createColumnFamily", &RocksDb::createColumnFamily)
    .def("createSnapshot", &RocksDb::createSnapshot)
    .def("releaseSnapshot", &RocksDb::releaseSnapshot)
    .def("createIterator", &RocksDb::createIterator)
    .def("destroyIterator", &RocksDb::destroyIterator)
    .def("close", &RocksDb::close);
}
