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
            py::gil_scoped_release release;
            Status status = Open(file_path);
            py::gil_scoped_acquire acquire;
            return status;
        }

        Status put(const std::string& user_key, const std::string& value){
            py::gil_scoped_release release;
            Status status = Put(Slice(user_key), Slice(value));
            py::gil_scoped_acquire acquire;
            return status;
        }

        Status deleteKey(const std::string& user_key){
            py::gil_scoped_release release;
            Status status = Delete(Slice(user_key));
            py::gil_scoped_acquire acquire;
            return status;
        }

        Status deleteRange(const std::string& begin_key, const std::string& end_key){
            py::gil_scoped_release release;
            Status status = DeleteRange(Slice(begin_key), Slice(end_key));
            py::gil_scoped_acquire acquire;
            return status;
        }

        Status finish(){
            ExternalSstFileInfo file_info;
            py::gil_scoped_release release;
            Status status = Finish(&file_info);
            py::gil_scoped_acquire acquire;
            return status;
        }
};
#endif