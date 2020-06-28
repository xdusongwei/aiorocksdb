#include <rocksdb/options.h>
#include <rocksdb/db.h>
#include <rocksdb/utilities/backupable_db.h>

using namespace ROCKSDB_NAMESPACE;

#ifdef RDB
class RBackupReadonly{
    public:
        RBackupReadonly(){
            backup = nullptr;
        }

        Status open(const BackupableDBOptions &options){
            py::gil_scoped_release release;
            Status s = BackupEngineReadOnly::Open(Env::Default(), options, &backup);
            py::gil_scoped_acquire acquire;
            return s;
        }

        void close(){
            py::gil_scoped_release release;
            if(backup != nullptr){
                delete backup;
                backup = nullptr;
            }
            py::gil_scoped_acquire acquire;
        }

        Status restoreDbFromBackup(const RestoreOptions& options, uint32_t backup_id, const std::string& db_dir, const std::string& wal_dir){
            py::gil_scoped_release release;
            Status s = backup->RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
            py::gil_scoped_acquire acquire;
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

        Status open(const BackupableDBOptions &options){
            py::gil_scoped_release release;
            Status s = BackupEngine::Open(Env::Default(), options, &backup);
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status purgeOldBackups(uint32_t num_backups_to_keep){
            py::gil_scoped_release release;
            Status s = backup->PurgeOldBackups(num_backups_to_keep);
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status deleteBackup(uint32_t backup_id){
            py::gil_scoped_release release;
            Status s = backup->DeleteBackup(backup_id);
            py::gil_scoped_acquire acquire;
            return s;
        }

        void close(){
            py::gil_scoped_release release;
            if(backup != nullptr){
                delete backup;
                backup = nullptr;
            }
            py::gil_scoped_acquire acquire;
        }

        Status createBackup(RDB* db){
            py::gil_scoped_release release;
            Status s = backup->CreateNewBackup(db);
            py::gil_scoped_acquire acquire;
            return s;
        }

        Status restoreDbFromBackup(const RestoreOptions& options, uint32_t backup_id, const std::string& db_dir, const std::string& wal_dir){
            py::gil_scoped_release release;
            Status s = backup->RestoreDBFromBackup(options, backup_id, db_dir, wal_dir);
            py::gil_scoped_acquire acquire;
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
