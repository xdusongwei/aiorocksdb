#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RocksSnapshot{
    public:
        RocksSnapshot(){
            snapshot = nullptr;
        }

        void setSnapshot(const Snapshot* snapshot){
            this->snapshot = snapshot;
        }

        const Snapshot* getSnapshot(){
            return snapshot;
        }

        void release(RDB* db){
            if(snapshot != nullptr){
                db->ReleaseSnapshot(snapshot);
                snapshot = nullptr;
            }
        }

    private:
        const Snapshot* snapshot;
};
#endif