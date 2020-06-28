
#ifdef RDB
class RTransaction{
     public:
        RTransaction(){
            transaction = nullptr;
        }

        void setTransaction(Transaction* transaction){
            this->transaction = transaction;
        }

        Transaction* getTransaction(){
            return transaction;
        }

        void close(){
            py::gil_scoped_release release;
            if(transaction){
                delete transaction;
                transaction = nullptr;
            }
            py::gil_scoped_acquire acquire;
        }

        Status put(RColumnFamily &columnFamily, const std::string& key, const std::string& value){
            py::gil_scoped_release release;
            Status s = transaction->Put(columnFamily.getHandle(), key, value);
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status deleteKey(RColumnFamily &columnFamily, const std::string& key){
            py::gil_scoped_release release;
            Status s = transaction->Delete(columnFamily.getHandle(), key);
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status commit(){
            py::gil_scoped_release release;
            Status s = transaction->Commit();
            py::gil_scoped_acquire acquire;
            return s;
        }

        void setSavePoint(){
            py::gil_scoped_release release;
            transaction->SetSavePoint();
            py::gil_scoped_acquire acquire;
        }

        Status rollback(){
            py::gil_scoped_release release;
            Status s = transaction->Rollback();
            py::gil_scoped_acquire acquire;
            return s;
        }

        void clearSnapshot(){
            py::gil_scoped_release release;
            transaction->ClearSnapshot();
            py::gil_scoped_acquire acquire;
        }

        Status prepare(){
            py::gil_scoped_release release;
            Status s = transaction->Prepare();
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status rollbackToSavePoint(){
            py::gil_scoped_release release;
            Status s = transaction->RollbackToSavePoint();
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status popSavePoint(){
            py::gil_scoped_release release;
            Status s = transaction->PopSavePoint();
            py::gil_scoped_acquire acquire;
            return s;
        }

        ComplexStatus multiGet(const ReadOptions &options, std::vector<RColumnFamily*>& column_family_list, std::vector<Slice> keys){
            std::vector<std::string> values;
            std::vector<ColumnFamilyHandle*> handles;
            for(RColumnFamily* i : column_family_list) {
                handles.push_back(i->getHandle());
            }
            ComplexStatus result;
            py::gil_scoped_release release;
            result.statusList = transaction->MultiGet(options, handles, keys, &values);
            result.valueList = values;
            py::gil_scoped_acquire acquire;
            return result;
        }

        ComplexStatus getForUpdate(const ReadOptions &options, RColumnFamily &columnFamily, const std::string &key){
            ComplexStatus result;
            {
                std::string str;
                PinnableSlice pinnable_val(&str);
                py::gil_scoped_release release;
                Status s = transaction->GetForUpdate(options, columnFamily.getHandle(), key, &pinnable_val);
                py::gil_scoped_acquire acquire;
                result.status = s;
                if(s.ok()){
                    result.value = py::bytes(str);
                }
            }
            return result;
        }

        void setSnapshot(){
            py::gil_scoped_release release;
            transaction->SetSnapshot();
            py::gil_scoped_acquire acquire;
        }

     private:
        Transaction*   transaction;
};
#endif
