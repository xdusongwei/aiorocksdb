#include <rocksdb/options.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/backup_engine.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RBackupReadonly{
    public:
        RBackupReadonly(){
            backup = nullptr;
        }

        Status open(const BackupEngineOptions &options){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = BackupEngineReadOnly::Open(Env::Default(), options, &backup);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        void close(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(backup != nullptr){
                delete backup;
                backup = nullptr;
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        Status restoreDbFromBackup(const RestoreOptions& options, uint32_t backup_id, const std::string& db_dir, const std::string& wal_dir){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = backup->RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status verifyBackup(uint32_t backup_id){
            Status s = backup->VerifyBackup(backup_id);
            return s;
        }

        std::vector<BackupInfo> getBackupInfo(){
            std::vector<BackupInfo> vecResult;
            backup->GetBackupInfo(&vecResult);
            return vecResult;
        }

    private:
        BackupEngineReadOnly*  backup;
};

class RBackup{
    public:
        RBackup(){
            backup = nullptr;
        }

        Status open(const BackupEngineOptions &options){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = BackupEngine::Open(Env::Default(), options, &backup);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status purgeOldBackups(uint32_t num_backups_to_keep){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = backup->PurgeOldBackups(num_backups_to_keep);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status deleteBackup(uint32_t backup_id){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = backup->DeleteBackup(backup_id);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        void close(){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            if(backup != nullptr){
                delete backup;
                backup = nullptr;
            }
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
        }

        Status createBackup(RDB* db){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = backup->CreateNewBackup(db);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status restoreDbFromBackup(const RestoreOptions& options, uint32_t backup_id, const std::string& db_dir, const std::string& wal_dir){
            #ifndef USE_GIL
            py::gil_scoped_release release;
            #endif
            Status s = backup->RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
            #ifndef USE_GIL
            py::gil_scoped_acquire acquire;
            #endif
            return s;
        }

        Status verifyBackup(uint32_t backup_id){
            Status s = backup->VerifyBackup(backup_id);
            return s;
        }

        std::vector<BackupInfo> getBackupInfo(){
            std::vector<BackupInfo> vecResult;
            backup->GetBackupInfo(&vecResult);
            return vecResult;
        }

    private:
        BackupEngine*  backup;
};
#endif
