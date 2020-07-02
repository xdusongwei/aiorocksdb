#include <rocksdb/db.h>


#ifdef RDB
class RBatch: public WriteBatch{
     public:
        RBatch(){
        }

        void put(const std::string& key, const std::string& value, RColumnFamily &columnFamily){
            WriteBatch::Put(columnFamily.getHandle(), key, value);
        }

        void deleteKey(const std::string& key, RColumnFamily &columnFamily){
            WriteBatch::Delete(columnFamily.getHandle(), key);
        }
};
#endif
