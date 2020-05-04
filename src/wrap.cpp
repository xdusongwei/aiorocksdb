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


class RocksStatus{
    public:
        RocksStatus(){
            this->code = 0U;
            this->subCode = 0U;
            this->severity = 0U;
        }

        void fromStatus(Status& s){
            this->code = s.code();
            this->subCode = s.subcode();
            this->severity = 0U;
        }

        bool isOk(){
            return code == Status::Code::kOk;
        }

        void setCode(unsigned char code){
            this->code = code;
        }

        void setSubCode(unsigned char subCode){
            this->subCode = subCode;
        }

        void setSeverity(unsigned char severity){
            this->severity = severity;
        }

        unsigned char getCode(){
            return code;
        }

        unsigned char getSubCode(){
            return subCode;
        }

        unsigned char getSeverity(){
            return severity;
        }

    private:
        unsigned char code;
        unsigned char subCode;
        unsigned char severity;
};


class RocksIterator{
    public:
        RocksIterator(){
            iter = nullptr;
        }

        void setIterator(Iterator* iter){
            this->iter = iter;
        }

        void close(DB* db){
            if(iter != nullptr){
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
            if(snapshot != nullptr){
                db->ReleaseSnapshot(snapshot);
                snapshot = nullptr;
            }
        }

    private:
        const Snapshot* snapshot;
};


class RocksColumnFamily{
    public:
        RocksColumnFamily(const std::string &name){
            this->name = name;
            this->cf = nullptr;
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
            if(cf != nullptr){
                db->DropColumnFamily(cf);
                cf = nullptr;
            }
        }

        void close(DB* db){
            if(cf != nullptr){
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
            { this->path = path; this->create_if_missing = create_if_missing; this->db = nullptr; }

        RocksStatus open(std::vector<RocksColumnFamily>& column_family_list){
            std::vector<ColumnFamilyDescriptor> column_families;
            std::vector<ColumnFamilyHandle*> handles;
            for(RocksColumnFamily i : column_family_list) {
                column_families.push_back(ColumnFamilyDescriptor(i.getName(), ColumnFamilyOptions()));
            }
            Options options;
            options.IncreaseParallelism();
            options.OptimizeLevelStyleCompaction();
            options.create_if_missing = create_if_missing;
            Status s = DB::Open(options, path, column_families, &handles, &db);
            if(s.ok()){
                for(auto i = 0U;i<column_family_list.size();i++) {
                    column_family_list[i].setHandle(handles[i]);
                }
            }
            handles.clear();
            RocksStatus status;
            status.fromStatus(s);
            return status;
        }

        void dropColumnFamily(RocksColumnFamily& cf){
            if(db != nullptr){
                cf.drop(db);
            }
        }

        void destroyColumnFamily(RocksColumnFamily& cf){
            if(db != nullptr){
                cf.close(db);
            }
        }

        void createColumnFamily(RocksColumnFamily& cf){
            std::vector<std::string> result;
            ColumnFamilyHandle* handle;
            db->CreateColumnFamily(ColumnFamilyOptions(), cf.getName(), &handle);
            cf.setHandle(handle);
        }

        void createSnapshot(RocksSnapshot& snapshot){
            if(db != nullptr){
                snapshot.setSnapshot(db->GetSnapshot());
            }
        }

        void releaseSnapshot(RocksSnapshot& snapshot){
            if(db != nullptr){
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
            if(db != nullptr){
                iter.close(db);
            }
        }

        void close(){
            try {
                if(db != nullptr){
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
    py::class_<Status>(m, "Status", py::dynamic_attr())
    .def("code", &Status::code)
    .def("subCode", &Status::subcode);

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
