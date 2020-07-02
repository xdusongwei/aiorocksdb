#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#include <rocksdb/utilities/stackable_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/backupable_db.h>
using namespace ROCKSDB_NAMESPACE;

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>

namespace py = pybind11;

#define RDB DB
#include "rocks_db.h"


namespace pybind11 { namespace detail {
template <> struct type_caster<Slice> {
    public:
        PYBIND11_TYPE_CASTER(Slice, _("Slice"));

        bool load(handle src, bool) {
            PyObject *source = src.ptr();
            PyObject *tmp = PyObject_Bytes(source);
            if (!tmp){
                return false;
            }
            else{
                value = PyBytes_AsString(tmp);
            }
            return !PyErr_Occurred();
        }

        static handle cast(Slice src, return_value_policy, handle) {
            return PyBytes_FromString(src.ToString().c_str());
        }
    };
}}


PYBIND11_MODULE(db_api, m) {
    py::class_<ComplexStatus>(m, "ComplexStatus")
    .def(py::init())
    .def_readonly("status", &ComplexStatus::status)
    .def_readonly("status_list", &ComplexStatus::statusList)
    .def_readonly("value_list", &ComplexStatus::valueList)
    .def_readonly("value", &ComplexStatus::value)
    ;

    py::class_<LoadOptionsStatus>(m, "LoadOptionsStatus")
    .def(py::init())
    .def_readonly("status", &LoadOptionsStatus::status)
    .def_readonly("column_family_list", &LoadOptionsStatus::columnFamilyList)
    .def_readonly("options", &LoadOptionsStatus::options)
    ;

    py::class_<RTransaction>(m, "RTransaction")
    .def(py::init())
    .def("put", &RTransaction::put)
    .def("delete_key", &RTransaction::deleteKey)
    .def("commit", &RTransaction::commit)
    .def("set_save_point", &RTransaction::setSavePoint)
    .def("multi_get", &RTransaction::multiGet)
    .def("get_for_update", &RTransaction::getForUpdate)
    .def("set_snapshot", &RTransaction::setSnapshot)
    .def("rollback", &RTransaction::rollback)
    .def("clear_snapshot", &RTransaction::clearSnapshot)
    .def("prepare", &RTransaction::prepare)
    .def("rollback_to_save_point", &RTransaction::rollbackToSavePoint)
    .def("pop_save_point", &RTransaction::popSavePoint)
    ;

    py::class_<RIterator>(m, "RIterator")
    .def(py::init())
    .def("seek", &RIterator::seek)
    .def("seek_for_prev", &RIterator::seekForPrev)
    .def("seek_to_first", &RIterator::seekToFirst)
    .def("seek_to_last", &RIterator::seekToLast)
    .def("valid", &RIterator::valid)
    .def("prev", &RIterator::prev)
    .def("next", &RIterator::next)
    .def("key", &RIterator::key)
    .def("value", &RIterator::value)
    .def("status", &RIterator::status)
    .def("close", &RIterator::close)
    ;

    py::class_<RSnapshot>(m, "RSnapshot")
    .def(py::init())
    .def("set_read_options", &RSnapshot::setReadOptions)
    .def("clear_read_options", &RSnapshot::clearReadOptions)
    ;

    py::class_<RColumnFamily>(m, "RColumnFamily")
    .def(py::init<const std::string&>())
    .def(py::init<const std::string&, const ColumnFamilyOptions&>())
    .def("get_name", &RColumnFamily::getName)
    .def("is_loaded", &RColumnFamily::isLoaded)
    .def("get_column_family_descriptor", &RColumnFamily::getColumnFamilyDescriptor)
    .def("__repr__",
        [](RColumnFamily &a) {
            return "<RColumnFamily name:" + a.getName() + " handle:" + (a.getHandle() ? "loaded": "empty") + ">";
        }
    )
    ;

    py::class_<RBatch>(m, "RBatch")
    .def(py::init())
    .def("put", &RBatch::put)
    .def("delete_key", &RBatch::deleteKey)
    ;

    py::class_<RBackup>(m, "RBackup")
    .def(py::init())
    .def("open", &RBackup::open)
    .def("close", &RBackup::close)
    .def("purge_old_backups", &RBackup::purgeOldBackups)
    .def("delete_backup", &RBackup::deleteBackup)
    .def("restore_db_from_backup", &RBackup::restoreDbFromBackup)
    .def("verify_backup", &RBackup::verifyBackup)
    .def("get_backup_info", &RBackup::getBackupInfo)
    ;

    py::class_<RBackupReadonly>(m, "RBackupReadonly")
    .def(py::init())
    .def("open", &RBackupReadonly::open)
    .def("close", &RBackupReadonly::close)
    .def("restore_db_from_backup", &RBackupReadonly::restoreDbFromBackup)
    .def("verify_backup", &RBackupReadonly::verifyBackup)
    .def("get_backup_info", &RBackupReadonly::getBackupInfo)
    ;

    py::class_<RSstFileWriter>(m, "RSstFileWriter")
    .def(py::init<const EnvOptions &, const Options &>())
    .def("put", &RSstFileWriter::put)
    .def("delete", &RSstFileWriter::deleteKey)
    .def("delete_range", &RSstFileWriter::deleteRange)
    .def("open", &RSstFileWriter::open)
    .def("finish", &RSstFileWriter::finish)
    ;

    py::class_<RSstFileReader>(m, "RSstFileReader")
    .def(py::init<const Options &>())
    .def("open", &RSstFileReader::open_sst)
    .def("verify_checksum", &RSstFileReader::verifyChecksum)
    .def("create_iterator", &RSstFileReader::createIterator)
    ;

    py::class_<RDb>(m, "RDb")
    .def(py::init<const std::string &, const Options&>())
    .def("open_db", &RDb::openDb)
    .def("open_db_for_readonly", &RDb::openDbForReadonly)
    .def("open_transaction_db", &RDb::openTransactionDb)
    .def("open_optimistic_transaction_db", &RDb::openOptimisticTransactionDb)
    .def("open_db_with_ttl", &RDb::openDbWithTTL)
    .def("destroy_column_family", &RDb::destroyColumnFamily)
    .def("drop_column_family", &RDb::dropColumnFamily)
    .def("create_column_family", &RDb::createColumnFamily)
    .def("create_snapshot", &RDb::createSnapshot)
    .def("release_snapshot", &RDb::releaseSnapshot)
    .def("create_iterator", &RDb::createIterator)
    .def("get", &RDb::get)
    .def("put", &RDb::put)
    .def("delete_key", &RDb::deleteKey)
    .def("delete_range", &RDb::deleteRange)
    .def("multi_get", &RDb::multiGet)
    .def("ingest_external_file", &RDb::ingestExternalFile)
    .def("flush", &RDb::flush)
    .def("write", &RDb::write)
    .def("begin_transaction", &RDb::beginTransaction)
    .def("begin_optimistic_transaction", &RDb::beginOptimisticTransaction)
    .def("release_transaction", &RDb::releaseTransaction)
    .def("create_backup", &RDb::createBackup)
    .def("close", &RDb::close)
    .def("set_ttl", py::overload_cast<int32_t>(&RDb::setTTL))
    .def("set_ttl", py::overload_cast<RColumnFamily&, int32_t>(&RDb::setTTL))
    ;

    m.def("load_latest_options", py::overload_cast<const ConfigOptions&, const std::string&>(&LoadLatestOptions));
}
