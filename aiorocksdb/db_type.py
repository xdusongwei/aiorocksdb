from typing import *
import abc


T = TypeVar('T')


class StatusT(Generic[T]):
    @abc.abstractmethod
    def code(self) -> int:
        ...

    @abc.abstractmethod
    def subcode(self) -> int:
        ...

    @abc.abstractmethod
    def ok(self) -> bool:
        ...

    @abc.abstractmethod
    def ToString(self) -> str:
        ...

    result: Optional[T] = None


class ComplexStatusT:
    status: StatusT
    value: Union[Optional[str], List[str]]
    status_list: List[StatusT]
    value_list: List[bytes]


class DbPathT:
    path: str
    target_size: int


class OptionsT:
    @abc.abstractmethod
    def IncreaseParallelism(self, total_threads: int = 16):
        ...

    @abc.abstractmethod
    def OptimizeLevelStyleCompaction(self, memtable_memory_budget: int = 512 * 1024 * 1024):
        ...

    max_log_file_size: int
    max_background_flushes: int
    max_subcompactions: int
    max_background_compactions: int
    base_background_compactions: int
    max_background_jobs: int
    delete_obsolete_files_period_micros: int
    wal_dir: str
    db_log_dir: str
    db_paths: List[DbPathT]
    use_fsync: bool
    max_total_wal_size: int
    max_file_opening_threads: int
    max_open_files: int
    paranoid_checks: bool
    error_if_exists: bool
    create_missing_column_families: bool
    create_if_missing: bool


class ReadOptionsT:
    tailing: bool


class RColumnFamilyT:
    @abc.abstractmethod
    def get_name(self) -> str:
        ...

    @abc.abstractmethod
    def is_loaded(self) -> bool:
        ...


class SnapshotT:
    @abc.abstractmethod
    def set_read_options(self, options: ReadOptionsT):
        ...


class LatestOptionsStatusT(StatusT[List[RColumnFamilyT]]):
    options: Optional[OptionsT] = None


__all__ = ['StatusT', 'ComplexStatusT', 'DbPathT', 'OptionsT', 'ReadOptionsT', 'RColumnFamilyT',
           'SnapshotT', 'LatestOptionsStatusT', ]
