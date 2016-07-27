class BaseLocksError(Exception):
    pass


class ArityError(BaseLocksError, ValueError):
    pass

class LockError(BaseLocksError, RuntimeError):
    pass

class AlreadyLocked(LockError):
    pass

class LockNotHeld(LockError):
    pass

class LockExpired(LockNotHeld):
    pass

class InvalidLockValue(LockError, ValueError):
    pass

class SerializationError(InvalidLockValue):
    pass
