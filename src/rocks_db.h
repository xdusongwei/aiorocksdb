#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#include <rocksdb/utilities/stackable_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/backupable_db.h>
#include <rocksdb/utilities/db_ttl.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>

namespace py = pybind11;
using namespace ROCKSDB_NAMESPACE;

#include "rocks_column_family.h"
#include "rocks_status.h"
#include "rocks_iterator.h"
#include "rocks_snapshot.h"
#include "rocks_batch.h"
#include "rocks_transaction.h"
#include "rocks_backup.h"
#include "rocks_sst_writer.h"
#include "rocks_sst_reader.h"
#include "rocks_options.h"


class RDb {
    public:
        RDb(const std::string &path, const Options& options)
        { this->path = path; this->options = options; this->db = nullptr; }

        Status openDb(std::vector<RColumnFamily*>& column_family_list);

        Status openDbForReadonly(std::vector<RColumnFamily*>& column_family_list, bool error_if_log_file_exist);

        Status openTransactionDb(std::vector<RColumnFamily*>& column_family_list);

        Status openOptimisticTransactionDb(std::vector<RColumnFamily*>& column_family_list);

        Status openDbWithTTL(std::vector<RColumnFamily*>& column_family_list, std::vector<int32_t> ttls, bool read_only);

        Status dropColumnFamily(RColumnFamily& cf);

        Status destroyColumnFamily(RColumnFamily& cf);

        Status createColumnFamily(const ColumnFamilyOptions& cf_options, RColumnFamily& cf, int32_t ttl);

        void createSnapshot(RSnapshot& snapshot);

        void releaseSnapshot(RSnapshot& snapshot);

        void createIterator(RIterator& iter, ReadOptions &options, RColumnFamily &columnFamily);

        void close();

        Status put(const WriteOptions &options, RColumnFamily &columnFamily, const std::string &key, const std::string &value);

        ComplexStatus get(const ReadOptions &options, RColumnFamily &columnFamily, const std::string &key);

        Status deleteKey(const WriteOptions &options, RColumnFamily &columnFamily, const std::string &key);

        Status deleteRange(const WriteOptions &options, RColumnFamily &columnFamily, const std::string &from, const std::string &to);

        ComplexStatus multiGet(const ReadOptions &options, const std::vector<RColumnFamily*>& column_family_list, const std::vector<std::string> keys);

        Status ingestExternalFile(const std::vector<std::string> &files, RColumnFamily &columnFamily, IngestExternalFileOptions &ifo);

        Status flush(const FlushOptions &options, std::vector<RColumnFamily*>& column_family_list);

        Status write(const WriteOptions &options, RBatch& batch);

        void beginTransaction(const WriteOptions &options, TransactionOptions& txn_options, RTransaction &txn);

        void beginOptimisticTransaction(const WriteOptions &options, OptimisticTransactionOptions& txn_options, RTransaction &txn);

        void releaseTransaction(RTransaction &txn);

        Status createBackup(RBackup& backup);

        void setTTL(int32_t ttl);

        void setTTL(RColumnFamily &columnFamily, int32_t ttl);

    private:
        std::string    path;
        RDB*           db;
        Options        options;
        bool           is_transaction_db;
        bool           is_optimistic_transaction_db;
        bool           is_ttl_db;
};

Status RDb::openDb(std::vector<RColumnFamily*>& column_family_list){
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        column_families.push_back(ColumnFamilyDescriptor(i->getName(), ColumnFamilyOptions()));
    }
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = RDB::Open(options, path, column_families, &handles, &db);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    if(s.ok()){
        for(auto i = 0U;i<column_family_list.size();i++) {
            column_family_list[i]->setHandle(handles[i]);
        }
    }
    handles.clear();
    return s;
}

Status RDb::openDbForReadonly(std::vector<RColumnFamily*>& column_family_list, bool error_if_log_file_exist = false){
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        column_families.push_back(ColumnFamilyDescriptor(i->getName(), ColumnFamilyOptions()));
    }
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = RDB::OpenForReadOnly(options, path, column_families, &handles, &db, error_if_log_file_exist);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    if(s.ok()){
        for(auto i = 0U;i<column_family_list.size();i++) {
            column_family_list[i]->setHandle(handles[i]);
        }
    }
    handles.clear();
    return s;
}

Status RDb::openTransactionDb(std::vector<RColumnFamily*>& column_family_list){
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        column_families.push_back(ColumnFamilyDescriptor(i->getName(), ColumnFamilyOptions()));
    }
    TransactionDBOptions txn_db_options;
    TransactionDB* txn_db;
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = TransactionDB::Open(options, txn_db_options, path, column_families, &handles, &txn_db);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    if(s.ok()){
        for(auto i = 0U;i<column_family_list.size();i++) {
            column_family_list[i]->setHandle(handles[i]);
        }
        db = (RDB*)txn_db;
    }
    handles.clear();
    is_transaction_db = true;
    return s;
}

Status RDb::openOptimisticTransactionDb(std::vector<RColumnFamily*>& column_family_list){
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        column_families.push_back(ColumnFamilyDescriptor(i->getName(), ColumnFamilyOptions()));
    }
    OptimisticTransactionDB* txn_db;
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = OptimisticTransactionDB::Open(options, path, column_families, &handles, &txn_db);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    if(s.ok()){
        for(auto i = 0U;i<column_family_list.size();i++) {
            column_family_list[i]->setHandle(handles[i]);
        }
        db = (RDB*)txn_db;
    }
    handles.clear();
    is_optimistic_transaction_db = true;
    return s;
}

Status RDb::openDbWithTTL(std::vector<RColumnFamily*>& column_family_list, std::vector<int32_t> ttls, bool read_only){
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        column_families.push_back(ColumnFamilyDescriptor(i->getName(), ColumnFamilyOptions()));
    }
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    DBWithTTL* ttl_db;
    Status s = DBWithTTL::Open(options, path, column_families, &handles, &ttl_db, ttls, read_only);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    if(s.ok()){
        for(auto i = 0U;i<column_family_list.size();i++) {
            column_family_list[i]->setHandle(handles[i]);
        }
        db = (RDB*)ttl_db;
    }
    handles.clear();
    is_ttl_db = true;
    return s;
}

Status RDb::dropColumnFamily(RColumnFamily& cf){
    return cf.drop(db);
}

Status RDb::destroyColumnFamily(RColumnFamily& cf){
    return cf.close(db);
}

Status RDb::createColumnFamily(const ColumnFamilyOptions& cf_options, RColumnFamily& cf, int32_t ttl = 0){
    std::vector<std::string> result;
    ColumnFamilyHandle* handle;
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s;
    if(is_ttl_db){
        DBWithTTL* ttl_db = (DBWithTTL*)db;
        s = ttl_db->CreateColumnFamilyWithTtl(cf_options, cf.getName(), &handle, ttl);
    }
    else{
        s = db->CreateColumnFamily(cf_options, cf.getName(), &handle);
    }
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    cf.setHandle(handle);
    return s;
}

void RDb::createSnapshot(RSnapshot& snapshot){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    if(db != nullptr){
        snapshot.setSnapshot(db->GetSnapshot());
    }
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
}

void RDb::releaseSnapshot(RSnapshot& snapshot){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    if(db != nullptr){
        snapshot.release(db);
    }
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
}

void RDb::createIterator(RIterator& iter, ReadOptions &options, RColumnFamily &columnFamily){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Iterator* newIter = db->NewIterator(options, columnFamily.getHandle());
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    iter.setIterator(newIter);
}


void RDb::close(){
    if(db != nullptr){
        delete db;
        db = nullptr;
    }
}


ComplexStatus RDb::get(const ReadOptions &options, RColumnFamily &columnFamily, const std::string &key){
    ComplexStatus result;
    {
        std::string str;
        #ifndef USE_GIL
        py::gil_scoped_release release;
        #endif
        Status s = db->Get(options, columnFamily.getHandle(), key, &str);
        #ifndef USE_GIL
        py::gil_scoped_acquire acquire;
        #endif
        result.status = s;
        if(s.ok()){
            result.value = py::bytes(str);
        }
    }
    return result;
}


Status RDb::put(const WriteOptions &options, RColumnFamily &columnFamily, const std::string &key, const std::string &value){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = db->Put(options, columnFamily.getHandle(), key, value);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return s;
}


Status RDb::deleteKey(const WriteOptions &options, RColumnFamily &columnFamily, const std::string &key){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = db->Delete(options, columnFamily.getHandle(), key);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return s;
}


Status RDb::deleteRange(const WriteOptions &options, RColumnFamily &columnFamily, const std::string &from, const std::string &to){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = db->DeleteRange(options, columnFamily.getHandle(), from, to);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return s;
}


ComplexStatus RDb::multiGet(const ReadOptions &options, const std::vector<RColumnFamily*>& column_family_list, const std::vector<std::string> keys){
    std::vector<Slice> slice_keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        handles.push_back(i->getHandle());
    }
    for(std::string i : keys) {
        slice_keys.push_back(Slice(i));
    }
    ComplexStatus result;
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    result.statusList = db->MultiGet(options, handles, slice_keys, &values);
    result.valueList = values;
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return result;
}


Status RDb::ingestExternalFile(const std::vector<std::string> &files, RColumnFamily &columnFamily, IngestExternalFileOptions &ifo){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = db->IngestExternalFile(files, ifo);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return s;
}


Status RDb::flush(const FlushOptions &options, std::vector<RColumnFamily*>& column_family_list){
    std::vector<ColumnFamilyHandle*> handles;
    for(RColumnFamily* i : column_family_list) {
        handles.push_back(i->getHandle());
    }
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = db->Flush(options, handles);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return s;
}


Status RDb::write(const WriteOptions &options, RBatch& batch){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = db->Write(options, &batch);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    return s;
}


void RDb::beginTransaction(const WriteOptions &options, TransactionOptions& txn_options, RTransaction &txn){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    if((TransactionDB*)db){
        Transaction* t = ((TransactionDB*)db)->BeginTransaction(options, txn_options);
        txn.setTransaction(t);
    }
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
}


void RDb::beginOptimisticTransaction(const WriteOptions &options, OptimisticTransactionOptions& txn_options, RTransaction &txn){
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    if((OptimisticTransactionDB*)db){
        Transaction* t = ((OptimisticTransactionDB*)db)->BeginTransaction(options, txn_options);
        txn.setTransaction(t);
    }
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
}


void RDb::releaseTransaction(RTransaction &txn){
    txn.close();
}


Status RDb::createBackup(RBackup& backup){
    Status s = backup.createBackup(db);
    return s;
}

void RDb::setTTL(int32_t ttl){
    if(!is_ttl_db){
        return;
    }
    DBWithTTL* ttl_db = (DBWithTTL*)db;
    ttl_db->SetTtl(ttl);
}

void RDb::setTTL(RColumnFamily &columnFamily, int32_t ttl){
    if(!is_ttl_db){
        return;
    }
    DBWithTTL* ttl_db = (DBWithTTL*)db;
    ttl_db->SetTtl(columnFamily.getHandle(), ttl);
}