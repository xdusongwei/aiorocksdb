#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RocksColumnFamily{
    public:
        RocksColumnFamily(const std::string &name){
            this->name = name;
            this->cf = nullptr;
        }

        void setHandle(ColumnFamilyHandle *cf){
            this->cf = cf;
        }

        ColumnFamilyHandle* getHandle(){
            return cf;
        }

        std::string& getName(){
            return name;
        }

        void drop(RDB* db){
            if(cf != nullptr){
                db->DropColumnFamily(cf);
                cf = nullptr;
            }
        }

        void close(RDB* db){
            if(cf != nullptr){
                db->DestroyColumnFamilyHandle(cf);
                cf = nullptr;
            }
        }

    private:
        std::string name;
        ColumnFamilyHandle* cf;
};
#endif