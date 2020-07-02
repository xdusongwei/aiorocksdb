#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/db.h>
#include <rocksdb/sst_file_reader.h>


using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RSstFileReader: public SstFileReader{
    public:
        Status open_sst(const std::string& file_path){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = SstFileReader::Open(file_path);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }

        Status verifyChecksum(const ReadOptions& read_options){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status status = VerifyChecksum(read_options);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return status;
        }

        void createIterator(RIterator& iter, ReadOptions &options){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Iterator* newIter = NewIterator(options);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            iter.setIterator(newIter);
        }
};
#endif