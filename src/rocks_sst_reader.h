#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>
#include <rocksdb/sst_file_reader.h>


using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RSstFileReader: public SstFileReader{
    public:
        Status open_sst(const std::string& file_path){
            py::gil_scoped_release release;
            Status status = SstFileReader::Open(file_path);
            py::gil_scoped_acquire acquire;
            return status;
        }

        Status verifyChecksum(const ReadOptions& read_options){
            py::gil_scoped_release release;
            Status status = VerifyChecksum(read_options);
            py::gil_scoped_acquire acquire;
            return status;
        }

        void createIterator(RIterator& iter, ReadOptions &options){
            py::gil_scoped_release release;
            Iterator* newIter = NewIterator(options);
            py::gil_scoped_acquire acquire;
            iter.setIterator(newIter);
        }
};
#endif