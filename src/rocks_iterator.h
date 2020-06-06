#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RocksIterator{
    public:
        RocksIterator(){
            iter = nullptr;
        }

        void setIterator(Iterator* iter){
            this->iter = iter;
        }

        void close(RDB* db){
            if(iter != nullptr){
                delete iter;
                iter = nullptr;
            }
        }

    private:
        Iterator* iter;
};
#endif