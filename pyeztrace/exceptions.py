class SetupNotDoneError(Exception):
    """
    Exception raised when setup is not done.
    """
    pass

class SetupAlreadyDoneError(Exception):
    """
    Exception raised when setup is already done.
    """
    pass

class SetupError(Exception):
    """
    Exception raised for general setup errors.
    """
    pass

class TracingError(Exception):
    """
    Exception raised for errors in the tracing system.
    """
    pass