class InvalidFieldException(Exception):
    pass

class InvalidDatabasePrefixException(Exception):
    pass

class UUIDFieldNotPresentException(Exception):
    pass

class NeedToReimplementThisMethodException(Exception):
    pass

class DatabaseLockedException(Exception):
    pass