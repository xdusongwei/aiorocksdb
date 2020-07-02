#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RSnapshot{
    public:
        RSnapshot(){
            snapshot = nullptr;
        }

        void setSnapshot(const Snapshot* snapshot){
            this->snapshot = snapshot;
        }

        const Snapshot* getSnapshot(){
            return snapshot;
        }

        void setReadOptions(ReadOptions& options){
            options.snapshot = snapshot;
        }

        void clearReadOptions(ReadOptions& options){
            options.snapshot = nullptr;
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
