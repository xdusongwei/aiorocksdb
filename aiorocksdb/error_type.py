from .db_type import *


class StatusError(Exception):
    def __init__(self, status):
        super(StatusError, self).__init__()
        self.status: StatusT = status

    def __str__(self):
        return f'<StatusError code:{self.status.code()} subCode:{self.status.subcode()} {self.status.ToString()}>'


__all__ = ['StatusError', ]
