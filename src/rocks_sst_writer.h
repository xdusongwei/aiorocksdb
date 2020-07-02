#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>


using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RSstFileWriter: public SstFileWriter{
    public:
        RSstFileWriter(const EnvOptions& env_options, const Options& options):
            SstFileWriter(env_options, options) {}

        Status open(const std::string& file_path){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = Open(file_path);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }

        Status put(const std::string& user_key, const std::string& value){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = Put(Slice(user_key), Slice(value));
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }

        Status deleteKey(const std::string& user_key){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = Delete(Slice(user_key));
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }

        Status deleteRange(const std::string& begin_key, const std::string& end_key){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = DeleteRange(Slice(begin_key), Slice(end_key));
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }

        Status finish(){
            ExternalSstFileInfo file_info;
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = Finish(&file_info);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }
};
#endif