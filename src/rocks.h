#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>
#include <rocksdb/comparator.h>
#include <rocksdb/utilities/stackable_db.h>
#include <rocksdb/utilities/transaction.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction_db.h>

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/complex.h>

namespace py = pybind11;
using namespace ROCKSDB_NAMESPACE;

#define RDB DB
#include "rocks_status.h"
#include "rocks_iterator.h"
#include "rocks_snapshot.h"
#include "rocks_column_family.h"


class Rocks {
    public:
        Rocks(const std::string &path, const bool create_if_missing);

        RocksStatus open_db(std::vector<RocksColumnFamily>& column_family_list);

        RocksStatus open_transaction_db(std::vector<RocksColumnFamily>& column_family_list);

        RocksStatus open_optimistic_transaction_db(std::vector<RocksColumnFamily>& column_family_list);

        void dropColumnFamily(RocksColumnFamily& cf);

        void destroyColumnFamily(RocksColumnFamily& cf);

        void createColumnFamily(RocksColumnFamily& cf);

        void createSnapshot(RocksSnapshot& snapshot);

        void releaseSnapshot(RocksSnapshot& snapshot);

        void createIterator(RocksIterator& iter, RocksSnapshot& snapshot);

        void destroyIterator(RocksIterator& iter);

        void close();
};
