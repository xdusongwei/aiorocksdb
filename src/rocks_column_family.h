#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RColumnFamily{
    public:
        RColumnFamily(const std::string &name){
            cd = ColumnFamilyDescriptor(name, ColumnFamilyOptions());
            cf = nullptr;
        }

        RColumnFamily(const std::string &name, const ColumnFamilyOptions& cf_options){
            cd = ColumnFamilyDescriptor(name, cf_options);
            cf = nullptr;
        }

        RColumnFamily(const ColumnFamilyDescriptor &cfd){
            cd = cfd;
            cf = nullptr;
        }

        void setHandle(ColumnFamilyHandle *cf){
            this->cf = cf;
        }

        ColumnFamilyHandle* getHandle(){
            return cf;
        }

        std::string& getName(){
            return cd.name;
        }

        bool isLoaded(){
            return cf != nullptr;
        }

        Status drop(RDB* db){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = db->DropColumnFamily(cf);
            cf = nullptr;
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status close(RDB* db){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = db->DestroyColumnFamilyHandle(cf);
            cf = nullptr;
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        ColumnFamilyDescriptor getColumnFamilyDescriptor(){
            return cd;
        }

    private:
        ColumnFamilyHandle* cf;
        ColumnFamilyDescriptor cd;
};
#endif
