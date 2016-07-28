class BaseLocksError(Exception):
    pass


class ArityError(BaseLocksError, ValueError):
    pass

class LockError(BaseLocksError, RuntimeError):
    pass

class LockAlreadyHeld(LockError):
    pass

class LockNotOwned(LockError):
    pass

class LockExpired(LockNotOwned):
    pass

class InvalidLockValue(LockError, ValueError):
    pass

class SerializationError(InvalidLockValue):
    pass
