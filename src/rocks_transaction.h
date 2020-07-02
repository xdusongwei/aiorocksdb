
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
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(transaction){
                delete transaction;
                transaction = nullptr;
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        Status put(RColumnFamily &columnFamily, const std::string& key, const std::string& value){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->Put(columnFamily.getHandle(), key, value);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status deleteKey(RColumnFamily &columnFamily, const std::string& key){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->Delete(columnFamily.getHandle(), key);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status commit(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->Commit();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        void setSavePoint(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            transaction->SetSavePoint();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        Status rollback(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->Rollback();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        void clearSnapshot(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            transaction->ClearSnapshot();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        Status prepare(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->Prepare();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status rollbackToSavePoint(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->RollbackToSavePoint();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status popSavePoint(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = transaction->PopSavePoint();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        ComplexStatus multiGet(const ReadOptions &options, std::vector<RColumnFamily*>& column_family_list, std::vector<Slice> keys){
            std::vector<std::string> values;
            std::vector<ColumnFamilyHandle*> handles;
            for(RColumnFamily* i : column_family_list) {
                handles.push_back(i->getHandle());
            }
            ComplexStatus result;
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            result.statusList = transaction->MultiGet(options, handles, keys, &values);
            result.valueList = values;
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return result;
        }

        ComplexStatus getForUpdate(const ReadOptions &options, RColumnFamily &columnFamily, const std::string &key){
            ComplexStatus result;
            {
                std::string str;
                PinnableSlice pinnable_val(&str);
                #ifndef USE_GIL
                py::gil_scoped_release release;
                #endif
                Status s = transaction->GetForUpdate(options, columnFamily.getHandle(), key, &pinnable_val);
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

        void setSnapshot(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            transaction->SetSnapshot();
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

     private:
        Transaction*   transaction;
};
#endif
