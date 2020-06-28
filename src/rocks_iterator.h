#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RIterator{
    public:
        RIterator(){

        }

        void seek(const std::string prefix){
            py::gil_scoped_release release;
            if(iter != nullptr){
                iter->Seek(prefix);
            }
            py::gil_scoped_acquire acquire;
        }

        void seekForPrev(const std::string prefix){
            py::gil_scoped_release release;
            if(iter != nullptr){
                iter->SeekForPrev(prefix);
            }
            py::gil_scoped_acquire acquire;
        }

        void seekToFirst(){
            py::gil_scoped_release release;
            if(iter != nullptr){
                iter->SeekToFirst();
            }
            py::gil_scoped_acquire acquire;
        }

        void seekToLast(){
            py::gil_scoped_release release;
            if(iter != nullptr){
                iter->SeekToLast();
            }
            py::gil_scoped_acquire acquire;
        }

        bool valid(){
            if(iter != nullptr){
                return iter->Valid();
            }
            return false;
        }

        void prev(){
            py::gil_scoped_release release;
            if(iter != nullptr){
                iter->Prev();
            }
            py::gil_scoped_acquire acquire;
        }

        void next(){
            py::gil_scoped_release release;
            if(iter != nullptr){
                iter->Next();
            }
            py::gil_scoped_acquire acquire;
        }

        Status status(){
            return iter->status();
        }

        py::object key(){
            return py::bytes(iter->key().ToString());
        }

        py::object value(){
            return py::bytes(iter->value().ToString());
        }

        void setIterator(Iterator* iter){
            this->iter = iter;
        }

        void close(){
            py::gil_scoped_release release;
            if(iter != nullptr){
                delete iter;
                iter = nullptr;
            }
            py::gil_scoped_acquire acquire;
        }

    private:
        Iterator* iter;
};
#endif
