#include <rocksdb/options.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/options_util.h>


LoadOptionsStatus LoadLatestOptions(const ConfigOptions& config_options, const std::string& dbpath){
    LoadOptionsStatus result;
    Options options;
    std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
    #ifndef USE_GIL
    py::gil_scoped_release release;
    #endif
    Status s = LoadLatestOptions(config_options, dbpath, &options, &loaded_cf_descs);
    #ifndef USE_GIL
    py::gil_scoped_acquire acquire;
    #endif
    result.status = s;
    if(s.ok()){
        for(ColumnFamilyDescriptor i : loaded_cf_descs) {
            result.columnFamilyList.push_back(RColumnFamily(i));
        }
        result.options = options;
    }
    return result;
}
