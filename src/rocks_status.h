#include <rocksdb/db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
struct ComplexStatus{
    ComplexStatus(){
        value = py::cast<py::none>(Py_None);
    }

    Status status;
    std::vector<Status> statusList;
    std::vector<std::string> valueList;
    py::object value;
};

struct LoadOptionsStatus{
    LoadOptionsStatus(){

    }

    Status status;
    std::vector<RColumnFamily> columnFamilyList;
    Options options;
};
#endif
