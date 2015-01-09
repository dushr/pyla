class BasePylaException(Exception):
    pass

class BaseEntryException(BasePylaException):
    pass

class NotFound(BaseEntryException):
    pass
