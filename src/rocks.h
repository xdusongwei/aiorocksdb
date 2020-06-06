

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
