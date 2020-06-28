#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#include <rocksdb/utilities/stackable_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/utilities/backupable_db.h>
#include <rocksdb/utilities/options_util.h>
using namespace ROCKSDB_NAMESPACE;

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>

namespace py = pybind11;


struct BackupableDBOptionsWrapper: BackupableDBOptions{
    public:
        explicit BackupableDBOptionsWrapper(const std::string& _backup_dir): BackupableDBOptions(_backup_dir){}
};


PYBIND11_MODULE(db_native, m) {
    py::class_<DbPath>(m, "DbPath")
    .def(py::init<const std::string &, uint64_t>());

    py::class_<Options>(m, "Options")
    .def(py::init())
    .def("IncreaseParallelism",
        [](Options &a, int total_threads) {
            a.IncreaseParallelism(total_threads);
        },
        py::arg("total_threads") = 16
    )
    .def("OptimizeLevelStyleCompaction",
        [](Options &a, uint64_t memtable_memory_budget) {
            a.OptimizeLevelStyleCompaction(memtable_memory_budget);
        },
        py::arg("memtable_memory_budget") = 512 * 1024 * 1024
    )
    .def_readwrite("advise_random_on_open", &Options::advise_random_on_open)
    .def_readwrite("atomic_flush ", &Options::atomic_flush)
    .def_readwrite("max_log_file_size", &Options::max_log_file_size)
    .def_readwrite("max_background_flushes", &Options::max_background_flushes)
    .def_readwrite("max_subcompactions", &Options::max_subcompactions)
    .def_readwrite("max_background_compactions", &Options::max_background_compactions)
    .def_readwrite("base_background_compactions", &Options::base_background_compactions)
    .def_readwrite("max_background_jobs", &Options::max_background_jobs)
    .def_readwrite("delete_obsolete_files_period_micros", &Options::delete_obsolete_files_period_micros)
    .def_readwrite("wal_dir", &Options::wal_dir)
    .def_readwrite("db_log_dir", &Options::db_log_dir)
    .def_readwrite("use_fsync", &Options::use_fsync)
    .def_readwrite("max_total_wal_size", &Options::max_total_wal_size)
    .def_readwrite("max_file_opening_threads", &Options::max_file_opening_threads)
    .def_readwrite("max_open_files", &Options::max_open_files)
    .def_readwrite("paranoid_checks", &Options::paranoid_checks)
    .def_readwrite("error_if_exists", &Options::error_if_exists)
    .def_readwrite("create_missing_column_families", &Options::create_missing_column_families)
    .def_readwrite("create_if_missing", &Options::create_if_missing)
    ;

    py::class_<Status>(m, "Status", py::dynamic_attr())
    .def(py::init())
    .def("code",
        [](Status &a) {
            return (unsigned char)a.code();
        }
    )
    .def("subcode",
        [](Status &a) {
            return (unsigned char)a.subcode();
        }
    )
    .def("ok", &Status::ok)
    .def("ToString", &Status::ToString)
    .def("__repr__",
        [](Status &a) {
            return "<Status code:" + std::to_string(a.code()) + " subCode:" + std::to_string(a.subcode()) + " " + a.ToString() + ">";
        }
    )
    ;

    py::class_<ReadOptions>(m, "ReadOptions")
    .def_readwrite("tailing", &ReadOptions::tailing)
    .def(py::init())
    ;

    py::class_<WriteOptions>(m, "WriteOptions")
    .def_readwrite("low_pri", &WriteOptions::low_pri)
    .def_readwrite("disableWAL ", &WriteOptions::disableWAL)
    .def(py::init())
    ;

    py::class_<EnvOptions>(m, "EnvOptions")
    .def(py::init())
    ;

    py::class_<FlushOptions>(m, "FlushOptions")
    .def(py::init())
    ;

    py::class_<IngestExternalFileOptions>(m, "IngestExternalFileOptions")
    .def_readwrite("write_global_seqno", &IngestExternalFileOptions::write_global_seqno)
    .def(py::init())
    ;

    py::class_<TransactionOptions>(m, "TransactionOptions")
    .def(py::init())
    ;

    py::class_<OptimisticTransactionOptions>(m, "OptimisticTransactionOptions")
    .def(py::init())
    ;

    py::class_<BackupableDBOptions>(m, "BackupableDBOptionsNative")
    .def(py::init<const std::string&>())
    ;

    py::class_<RestoreOptions>(m, "RestoreOptions")
    .def(py::init())
    ;

    py::class_<ColumnFamilyOptions>(m, "ColumnFamilyOptions")
    .def(py::init())
    ;

    py::class_<ColumnFamilyDescriptor>(m, "ColumnFamilyDescriptor")
    .def_readonly("name", &ColumnFamilyDescriptor::name)
    .def_readonly("options", &ColumnFamilyDescriptor::options)
    ;

    py::class_<BackupInfo>(m, "BackupInfo", py::dynamic_attr())
    .def_readonly("backup_id", &BackupInfo::backup_id)
    .def_readonly("timestamp", &BackupInfo::timestamp)
    .def_readonly("size", &BackupInfo::size)
    .def_readonly("number_files", &BackupInfo::number_files)
    .def("__repr__",
        [](BackupInfo &a) {
            return "<BackupInfo id:" + std::to_string(a.backup_id) + " timestamp:" + std::to_string(a.timestamp) + ">";
        }
    )
    ;

    m
    .def("destroy_db",
        [](const std::string& dbname, const Options& options) {
            py::gil_scoped_release release;
            Status status = DestroyDB(dbname, options);
            py::gil_scoped_acquire acquire;
            return status;
        },
        "Destroy the contents of the specified database."
    );

    m.def("repair_db",
        [](const std::string& dbname, const Options& options) {
            py::gil_scoped_release release;
            Status status = RepairDB(dbname, options);
            py::gil_scoped_acquire acquire;
            return status;
        },
        "repair database"
    );

    m.def("BackupableDBOptions",
        [](const std::string& _backup_dir) {
            BackupableDBOptions options = BackupableDBOptions(_backup_dir);
            return options;
        },
        ""
    );

    py::class_<ConfigOptions>(m, "ConfigOptions")
    .def(py::init())
    .def_readwrite("ignore_unknown_options", &ConfigOptions::ignore_unknown_options)
    .def_readwrite("input_strings_escaped", &ConfigOptions::input_strings_escaped)
    .def_readwrite("file_readahead_size", &ConfigOptions::file_readahead_size)
    .def("is_shallow", &ConfigOptions::IsShallow)
    .def("is_detailed", &ConfigOptions::IsDetailed)
    .def("is_check_disabled", &ConfigOptions::IsCheckDisabled)
    .def("is_check_enabled", &ConfigOptions::IsCheckEnabled)
    ;
}
