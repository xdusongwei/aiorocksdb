#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RIterator{
    public:
        RIterator(){

        }

        void seek(const std::string prefix){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                iter->Seek(prefix);
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        void seekForPrev(const std::string prefix){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                iter->SeekForPrev(prefix);
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        void seekToFirst(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                iter->SeekToFirst();
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        void seekToLast(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                iter->SeekToLast();
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        bool valid(){
            if(iter != nullptr){
                return iter->Valid();
            }
            return false;
        }

        void prev(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                iter->Prev();
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        void next(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                iter->Next();
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
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
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(iter != nullptr){
                delete iter;
                iter = nullptr;
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

    private:
        Iterator* iter;
};
#endif
