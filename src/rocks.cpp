

class Rocks {
    public:
        Rocks(const std::string &path, const bool create_if_missing)
            { this->path = path; this->create_if_missing = create_if_missing; this->db = nullptr; }

        RocksStatus open_db(std::vector<RocksColumnFamily>& column_family_list){
            std::vector<ColumnFamilyDescriptor> column_families;
            std::vector<ColumnFamilyHandle*> handles;
            for(RocksColumnFamily i : column_family_list) {
                column_families.push_back(ColumnFamilyDescriptor(i.getName(), ColumnFamilyOptions()));
            }
            Options options;
            options.IncreaseParallelism();
            options.OptimizeLevelStyleCompaction();
            options.create_if_missing = create_if_missing;
            Status s = RDB::Open(options, path, column_families, &handles, &db);
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

        RocksStatus open_transaction_db(std::vector<RocksColumnFamily>& column_family_list){
            std::vector<ColumnFamilyDescriptor> column_families;
            std::vector<ColumnFamilyHandle*> handles;
            for(RocksColumnFamily i : column_family_list) {
                column_families.push_back(ColumnFamilyDescriptor(i.getName(), ColumnFamilyOptions()));
            }
            Options options;
            options.IncreaseParallelism();
            options.OptimizeLevelStyleCompaction();
            options.create_if_missing = create_if_missing;
            TransactionDBOptions txn_db_options;
            TransactionDB* txn_db;
            Status s = TransactionDB::Open(options, txn_db_options, path, column_families, &handles, &txn_db);
            if(s.ok()){
                for(auto i = 0U;i<column_family_list.size();i++) {
                    column_family_list[i].setHandle(handles[i]);
                }
                db = (RDB*)txn_db;
            }
            handles.clear();
            RocksStatus status;
            status.fromStatus(s);
            return status;
        }

        RocksStatus open_optimistic_transaction_db(std::vector<RocksColumnFamily>& column_family_list){
            std::vector<ColumnFamilyDescriptor> column_families;
            std::vector<ColumnFamilyHandle*> handles;
            for(RocksColumnFamily i : column_family_list) {
                column_families.push_back(ColumnFamilyDescriptor(i.getName(), ColumnFamilyOptions()));
            }
            Options options;
            options.IncreaseParallelism();
            options.OptimizeLevelStyleCompaction();
            options.create_if_missing = create_if_missing;
            OptimisticTransactionDB* txn_db;
            Status s = OptimisticTransactionDB::Open(options, path, column_families, &handles, &txn_db);
            if(s.ok()){
                for(auto i = 0U;i<column_family_list.size();i++) {
                    column_family_list[i].setHandle(handles[i]);
                }
                db = (RDB*)txn_db;
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
        RDB*            db;
};
